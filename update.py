#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
a1-siku-core 升级件 v1.0.0
作者: 维护者团队｜功能: 增量/回滚/配置保护（引导安装/自检/升级维护，跨平台纯标准库）
增量应用 / 回滚 / 配置保护
用法:
  python3 update.py --check              # 对比已安装版本 vs 本包版本
  python3 update.py --apply              # 增量应用（备份先行，config 保护）
  python3 update.py --rollback           # 回滚最近一次备份
  python3 update.py --list-backups       # 列出备份
跨平台：macOS / Linux / Windows（Python 3.8+，纯标准库）
"""
import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path

MODULE_ID = "a1-siku-core"
def _pkg_version():
    """版本单源化（P1-20260816）：从同目录 manifest.yaml 读包版本，禁止硬编码（硬编码→升级幂等误判）。"""
    _mf = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manifest.yaml")
    try:
        with open(_mf, encoding="utf-8") as _f:
            for _line in _f:
                if _line.strip().startswith("version:"):
                    return _line.split(":", 1)[1].strip()
    except OSError:
        pass
    raise SystemExit("❌ 版本单源化失败：无法读取同目录 manifest.yaml 的 version 字段，中止安装（避免幂等误判）")


VERSION = _pkg_version()
DEFAULT_TARGET = os.path.expanduser("~/siku-core")

# 增量应用时同步到目标的文件（config.example 作为模板按配置保护规则处理）
SYNC_FILES = ["install.py", "update.py", "README.md", "INSTALL.md", "manifest.yaml", "mcp_server.py", "samples", "src"]
# 配置保护：已填充真实值的 config 不被覆盖
CFG_NAME = "config.ini"
TPL_NAME = "config.example"


def log(msg, tag="INFO"):
    print("[%s] %s" % (tag, msg), flush=True)


def log_ok(msg):
    log(msg, "OK")


def log_err(msg):
    log(msg, "ERROR")


def read_version(target):
    mf = os.path.join(target, "config", ".manifest.json")
    if os.path.exists(mf):
        try:
            import json
            with open(mf, "r", encoding="utf-8") as f:
                return json.load(f).get("version")
        except (OSError, ValueError):
            pass
    vf = os.path.join(target, "version")
    if os.path.exists(vf):
        with open(vf, "r", encoding="utf-8") as f:
            return f.read().strip()
    return None


def config_has_real_values(cfg_path):
    if not os.path.exists(cfg_path):
        return False
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or line.startswith(";"):
                    continue
                val = line.split("=", 1)[1].strip() if "=" in line else ""
                if val and "<" not in val and "PLACEHOLDER" not in val:
                    return True
    except OSError:
        return False
    return False


def backup_dir(target):
    return os.path.join(target, "backups", "upd-%s" % time.strftime("%Y%m%d-%H%M%S"))


def do_backup(target):
    """整目录备份（跳过 backups 自身）"""
    if not os.path.isdir(target):
        return None
    bd = backup_dir(target)
    os.makedirs(bd, exist_ok=True)
    for name in os.listdir(target):
        src = os.path.join(target, name)
        if name == "backups" or os.path.islink(src):
            continue
        dst = os.path.join(bd, name)
        if os.path.isdir(src):
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)
    with open(os.path.join(bd, "backup.log"), "w", encoding="utf-8") as f:
        f.write("backup of %s at %s\n" % (target, time.strftime("%Y-%m-%d %H:%M:%S")))
    return bd


def list_backups(target):
    bdir = os.path.join(target, "backups")
    if not os.path.isdir(bdir):
        log("无备份目录")
        return
    for name in sorted(os.listdir(bdir)):
        if name.startswith("upd-"):
            log("  %s" % name)


def cmd_check(target):
    inst = read_version(target)
    if inst is None:
        log_err("未检测到已安装版本（%s 无 version 文件），请先运行 install.py" % target)
        return 1
    log("已安装版本: %s" % inst)
    log("本包版本:   %s" % VERSION)
    if inst == VERSION:
        log_ok("版本一致，无需更新")
    else:
        log("存在版本差异，可运行 --apply 增量应用")
    return 0


def cmd_apply(target, here):
    inst = read_version(target)
    if inst is None:
        log_err("目标未安装（无 version），请先运行 install.py")
        return 1
    log("增量应用：%s -> %s" % (inst, VERSION))
    bd = do_backup(target)
    if bd:
        log_ok("备份完成: %s" % bd)
    updated = 0
    for name in SYNC_FILES:
        src = os.path.join(str(here), name)
        dst = os.path.join(target, name)
        if not os.path.exists(src):
            continue
        if os.path.exists(dst) and os.path.samefile(src, dst):
            continue
        if os.path.isdir(src):
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)
        updated += 1
        log_ok("更新: %s" % name)
    # config 模板：仅当目标 config 无真实值时更新
    tpl = os.path.join(str(here), TPL_NAME)
    dst_cfg = os.path.join(target, CFG_NAME)
    if config_has_real_values(dst_cfg):
        log("配置保护：%s 含真实值，跳过覆盖" % CFG_NAME)
    elif os.path.exists(tpl):
        shutil.copy2(tpl, dst_cfg)
        updated += 1
        log_ok("更新: %s（模板）" % CFG_NAME)
    # 版本写回（P2-20260816）：--apply 后把版本写入目标 config/.manifest.json（read_version 依据）
    mf = os.path.join(target, "config", ".manifest.json")
    try:
        _man = {}
        if os.path.exists(mf):
            with open(mf, "r", encoding="utf-8") as _f:
                _man = json.load(_f)
        _man["module"] = MODULE_ID
        _man["version"] = VERSION
        _man["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        with open(mf, "w", encoding="utf-8") as _f:
            json.dump(_man, _f, ensure_ascii=False, indent=1)
        log_ok("版本写回: %s -> %s" % (mf, VERSION))
    except (OSError, ValueError) as _e:
        log_err("版本写回失败: %s" % _e)
        return 1
    log_ok("增量应用完成（%d 个文件），版本 -> %s" % (updated, VERSION))
    log("回滚命令：python3 update.py --rollback")
    return 0


def cmd_rollback(target):
    bdir = os.path.join(target, "backups")
    if not os.path.isdir(bdir):
        log_err("无备份目录，无法回滚")
        return 1
    snaps = sorted([d for d in os.listdir(bdir) if d.startswith("upd-")])
    if not snaps:
        log_err("无可用备份快照")
        return 1
    snap = os.path.join(bdir, snaps[-1])
    log("回滚到快照: %s" % snap)
    # 恢复快照内文件（保留 backups 目录本身）
    for name in os.listdir(snap):
        if name == "backup.log":
            continue
        src = os.path.join(snap, name)
        dst = os.path.join(target, name)
        if os.path.isdir(src):
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)
    log_ok("回滚完成")
    return 0


def main():
    ap = argparse.ArgumentParser(description="%s 升级件 v%s" % (MODULE_ID, VERSION))
    ap.add_argument("--check", action="store_true", help="版本对比")
    ap.add_argument("--apply", action="store_true", help="增量应用")
    ap.add_argument("--rollback", action="store_true", help="回滚最近备份")
    ap.add_argument("--list-backups", action="store_true", help="列出备份")
    ap.add_argument("--target", default=None, help="安装目录（默认 %s）" % DEFAULT_TARGET)
    args = ap.parse_args()

    target = os.path.abspath(os.path.expanduser(args.target or DEFAULT_TARGET))
    here = Path(__file__).resolve().parent

    if args.list_backups:
        list_backups(target)
        return 0
    if args.rollback:
        return cmd_rollback(target)
    if args.apply:
        return cmd_apply(target, here)
    return cmd_check(target)


if __name__ == "__main__":
    sys.exit(main())
