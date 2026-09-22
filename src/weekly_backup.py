#!/usr/bin/env python3
"""
weekly_backup.py — 每周日全量备份（pipeline_runner cron调度）

打包内容：memory_store.db + chromadb + private/ + config/ + logs/ + memory/
输出位置：$SIKU_ROOT/weekly/siku-full-YYYY-MM-DD.tar.gz
保留策略：保留最近4周
"""
import os, shutil, tarfile, glob
from datetime import datetime, timedelta

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_OPENCLAW_HOME = os.environ.get("OPENCLAW_HOME", os.path.expanduser("~/.openclaw"))  # OpenClaw 主目录（环境变量可覆盖）

BASE = _SIKU_ROOT
WEEKLY_DIR = os.path.join(BASE, "weekly")
os.makedirs(WEEKLY_DIR, exist_ok=True)

today = datetime.now().strftime("%Y-%m-%d")
# 只在周日执行（weekday() == 6）
if datetime.now().weekday() != 6:
    print(f"[SKIP] 今天不是周日（{datetime.now().strftime('%A')}），跳过备份")
    exit(0)
tarball = os.path.join(WEEKLY_DIR, f"siku-full-{today}.tar.gz")

if os.path.exists(tarball):
    print(f"[SKIP] 今日备份已存在: {tarball}")
    exit(0)

# 需要打包的内容
paths = [
    os.path.join(BASE, "memory_store.db"),
    os.path.join(BASE, "memory_store.chromadb"),
    os.path.join(BASE, "private"),
    os.path.join(BASE, "config"),
    os.path.join(BASE, "logs"),
    os.path.join(BASE, ".concern_activity_cache.json"),
    os.path.join(BASE, ".concern_arbitration_queue.json"),
    os.path.join(_OPENCLAW_HOME, "workspace/memory"),
    os.path.join(_SIKU_ROOT, "permissions.yaml"),
]

with tarfile.open(tarball, "w:gz") as tar:
    for p in paths:
        if os.path.exists(p):
            arcname = os.path.basename(p) if not p.startswith(BASE) else os.path.relpath(p, BASE)
            tar.add(p, arcname=arcname)
            print(f"  + {p}")

# 清理旧备份（保留最近4周）
for old in sorted(glob.glob(os.path.join(WEEKLY_DIR, "siku-full-*.tar.gz")))[:-4]:
    os.remove(old)
    print(f"  - 清理旧备份: {os.path.basename(old)}")

print(f"\n✅ 全量备份完成: {tarball}")
