#!/usr/bin/env python3
# ── venv 守卫 ( 2026-08-17) ──
# 非 venv 解释器（系统 python 3.9 等）自动重执行为 venv python——fail-closed，不静默降级。
# 升级/venv 路径变化后系统 python 跑会缺 3.11 编译依赖；SIKU_VENV_PYTHON 可覆盖（运维/沙盒测试）。
import os as _os, sys as _sys

_SIKU_ROOT = _os.environ.get("SIKU_ROOT", _os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = _os.environ.get("HERMES_HOME", _os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）
if _os.environ.get("SIKU_VENV_GUARDED") != "1":
    _vp = _os.environ.get("SIKU_VENV_PYTHON") or _os.path.join(_HERMES_HOME, "hermes-agent/venv/bin/python")
    if not _os.path.exists(_vp):
        _sys.stderr.write("WARN venv-guard: %s 不存在 → 运行于系统解释器 %s（降级已留痕）\n" % (_vp, _sys.executable))
    elif _os.path.realpath(_sys.executable) != _os.path.realpath(_vp):
        _os.environ["SIKU_VENV_GUARDED"] = "1"
        _os.execv(_vp, [_vp] + _sys.argv)

"""L1摘录Hermes侧 — 每10分钟检查state.db对话活动，有活动则建议不写入"""
import os, sqlite3, logging, fcntl, time
from datetime import datetime

STATE_DB = os.path.join(_HERMES_HOME, "state.db")
LOG = os.path.join(_SIKU_ROOT, "logs/l1_hermes_extractor.log")
LOCK_FILE = os.path.join(_HERMES_HOME, "scripts/.l1_extractor.lock")
CHECK_FILE = os.path.join(_HERMES_HOME, "scripts/.l1_last_check")

logging.basicConfig(filename=LOG, level=logging.INFO, format="%(asctime)s %(message)s")

# 日志截断：超 1MB 裁尾部
if os.path.exists(LOG) and os.path.getsize(LOG) > 1_048_576:
    with open(LOG) as f:
        lines = f.readlines()
    with open(LOG, "w") as f:
        f.writelines(lines[-5000:])

def acquire_lock():
    """文件锁防并发，拿不到直接退出"""
    lock_dir = os.path.dirname(LOCK_FILE)
    os.makedirs(lock_dir, exist_ok=True)
    fd = os.open(LOCK_FILE, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        return None

def last_activity():
    if not os.path.exists(STATE_DB):
        return 0
    sql = "SELECT MAX(timestamp) FROM messages"
    try:
        conn = sqlite3.connect(STATE_DB)
        row = conn.execute(sql).fetchone()
        conn.close()
        return row[0] if row and row[0] else 0
    except Exception as e:
        logging.error(f"state.db 查询失败: {e}")
        return 0

def perm_audit(layer):
    """P0-2 权限写路径接入(2026-08-03): audit模式只记录不拦截, 观察1周后转enforce"""
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import check_permission as cp
    cp.check(os.environ.get("SIKU_AGENT", "用户"), "write", layer, mode="audit")

def main():
    fd = acquire_lock()
    if fd is None:
        print("ℹ️ L1摘录: 上一轮还在跑，跳过")
        logging.info("并发跳过")
        return

    try:
        perm_audit("L1")
        ts = last_activity()
        if not ts:
            print("ℹ️ L1摘录: state.db 不可访问，跳过")
            logging.info("state.db 不可访问，跳过")
            return

        last_ts = 0
        if os.path.exists(CHECK_FILE):
            with open(CHECK_FILE) as f:
                try:
                    last_ts = float(f.read().strip())
                except:
                    last_ts = 0

        window = 600
        if ts > last_ts + window:
            print(f"📋 L1建议: 检测到新对话活动 (时间: {datetime.fromtimestamp(ts).isoformat()})")
            logging.info(f"新活动: ts={ts}, last_ts={last_ts}")
        else:
            print("ℹ️ L1摘录: 无可摘录内容")
            logging.info(f"无活动: ts={ts}, last_ts={last_ts}")

        with open(CHECK_FILE, "w") as f:
            f.write(str(ts))
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

if __name__ == "__main__":
    main()
