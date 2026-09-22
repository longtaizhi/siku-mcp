#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
install_skills.py — a1-siku-core 技能三件安装器（siku-query / siku-write / siku-ops）

作者: a1-siku-core 维护者｜功能: 把随包技能装进目标系统的 skills 目录（Hermes / OpenClaw 双支持）
用法:
  python3 install_skills.py                 # 自动探测目标 skills 目录并安装
  python3 install_skills.py --target <dir>  # 指定目标 skills 目录
  python3 install_skills.py --dry-run       # 演练（只打印动作，零写入）
  python3 install_skills.py --list          # 列出本包技能与安装状态
  python3 install_skills.py --uninstall     # 卸载（仅移除本包三个技能目录）
特性: 幂等（内容相同跳过）｜覆盖前自动备份（.bak-时间戳）｜纯标准库｜零依赖
"""
import argparse
import filecmp
import os
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SKILLS = ["siku-query", "siku-write", "siku-ops"]


def log(msg, tag="INFO"):
    print("[%s] %s" % (tag, msg), flush=True)


def detect_target():
    """目标 skills 目录探测：HERMES_HOME → OPENCLAW_HOME → 常见默认"""
    cands = []
    hh = os.environ.get("HERMES_HOME")
    oh = os.environ.get("OPENCLAW_HOME")
    if hh:
        cands.append(os.path.join(hh, "skills"))
    if oh:
        cands.append(os.path.join(oh, "skills"))
    cands.append(os.path.expanduser("~/.hermes/skills"))
    cands.append(os.path.expanduser("~/.openclaw/skills"))
    for c in cands:
        if os.path.isdir(c):
            return c, cands
    return None, cands


def read_frontmatter_name(skill_md):
    """读取 SKILL.md frontmatter 的 name 字段（校验用）"""
    try:
        with open(skill_md, encoding="utf-8") as f:
            lines = f.read().splitlines()
    except OSError:
        return ""
    if not lines or lines[0].strip() != "---":
        return ""
    for ln in lines[1:60]:
        if ln.strip() == "---":
            break
        if ln.startswith("name:"):
            return ln.split(":", 1)[1].strip().strip('"').strip("'")
    return ""


def dirs_equal(a, b):
    """比较两个技能目录内容是否一致（忽略 .bak-* / __pycache__）"""
    def snap(d):
        out = {}
        for root, dirs, files in os.walk(d):
            dirs[:] = [x for x in dirs if not x.startswith(".bak-") and x != "__pycache__"]
            for f in files:
                if f.startswith(".bak-"):
                    continue
                fp = os.path.join(root, f)
                out[os.path.relpath(fp, d)] = os.path.getsize(fp)
        return out
    sa, sb = snap(a), snap(b)
    if set(sa) != set(sb):
        return False
    for rel in sa:
        if not filecmp.cmp(os.path.join(a, rel), os.path.join(b, rel), shallow=False):
            return False
    return True


def do_install(target, dry_run):
    if not os.path.isdir(target):
        log("目标 skills 目录不存在: %s —— 请先创建（或 --target 指定）" % target, "ERROR")
        return 2
    installed = skipped = backed = 0
    for name in SKILLS:
        src = os.path.join(HERE, name)
        dst = os.path.join(target, name)
        if not os.path.isdir(src):
            log("源技能缺失: %s（请使用完整模块包）" % src, "ERROR")
            return 2
        # 前置校验：SKILL.md + frontmatter name 与目录名一致
        smd = os.path.join(src, "SKILL.md")
        fname = read_frontmatter_name(smd)
        if fname != name:
            log("技能 %s frontmatter name=%r 与目录名不一致 —— 中止（防装载排除）" % (name, fname), "ERROR")
            return 2
        if os.path.isdir(dst) and dirs_equal(src, dst):
            log("技能 %s 已是最新（内容一致）——跳过" % name, "OK")
            skipped += 1
            continue
        if dry_run:
            log("DRY-RUN 将安装技能: %s -> %s" % (name, dst))
            continue
        if os.path.isdir(dst):
            bak = "%s.bak-%s" % (dst, time.strftime("%Y%m%d-%H%M%S"))
            shutil.move(dst, bak)
            log("已有同名技能已备份: %s" % bak, "WARN")
            backed += 1
        sz = os.path.join(os.path.dirname(src), name)
        shutil.copytree(sz, dst)
        log("技能已安装: %s -> %s" % (name, dst), "OK")
        installed += 1
    if dry_run:
        log("演练完成（零写入）")
        return 0
    log("安装完成：新增/更新 %d，跳过 %d，备份 %d" % (installed, skipped, backed), "OK")
    log("验证：python3 %s --list" % os.path.abspath(__file__))
    return 0


def do_list(target):
    log("本包技能: %s" % ", ".join(SKILLS))
    log("目标目录: %s" % (target or "（未探测到——用 --target 指定）"))
    if target:
        for name in SKILLS:
            dst = os.path.join(target, name)
            state = "已安装" if os.path.isdir(dst) else "未安装"
            print("  - %-12s %s" % (name, state))
    return 0


def do_uninstall(target, dry_run):
    """卸载：仅移除本包三个技能目录（先自动备份）"""
    if not target or not os.path.isdir(target):
        log("目标 skills 目录不存在，无需卸载", "WARN")
        return 0
    for name in SKILLS:
        dst = os.path.join(target, name)
        if not os.path.isdir(dst):
            log("技能 %s 未安装——跳过" % name)
            continue
        if dry_run:
            log("DRY-RUN 将卸载技能: %s" % dst)
            continue
        bak = "%s.uninstalled-bak-%s" % (dst, time.strftime("%Y%m%d-%H%M%S"))
        shutil.move(dst, bak)
        log("技能已卸载（备份保留）: %s" % bak, "OK")
    if dry_run:
        log("演练完成（零写入）")
    return 0


def main():
    ap = argparse.ArgumentParser(description="a1-siku-core 技能三件安装器")
    ap.add_argument("--target", help="目标 skills 目录（默认自动探测）")
    ap.add_argument("--dry-run", action="store_true", help="演练，零写入")
    ap.add_argument("--list", action="store_true", help="列出技能与安装状态")
    ap.add_argument("--uninstall", action="store_true", help="卸载（仅移除本包技能）")
    args = ap.parse_args()

    target = args.target
    if not target:
        target, cands = detect_target()
        if not target and not args.list:
            log("未能探测到 skills 目录。候选: %s" % ", ".join(cands), "ERROR")
            log("请用 --target <你的 skills 目录> 安装（Hermes 通常为 ~/.hermes/skills）", "ERROR")
            return 2
    if args.list:
        return do_list(target)
    if args.uninstall:
        return do_uninstall(target, args.dry_run)
    return do_install(target, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
