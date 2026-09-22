#!/usr/bin/env python3
"""
四库统一管道调度器 — 一条cron搞定所有
用法: 
  python3 pipeline_runner.py reta        # 单管道（兼容旧用法）
  python3 pipeline_runner.py all         # 跑全部
  python3 pipeline_runner.py scheduler   # ⭐ 统一调度（推荐）

cron配置（仅需一条）：
  * * * * * /usr/bin/python3 <安装目录>/src/pipeline_runner.py scheduler >> .../logs/cron.log 2>&1
"""
import os, sys, time, json
import subprocess
from datetime import datetime, timedelta
from threading import Semaphore

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）

BASE = _SIKU_ROOT
SCRIPTS = os.path.join(BASE, "scripts")
LOCK_DIR = os.path.join(BASE, ".locks")
AUDIT_LOG = os.path.join(BASE, "audit", "pipeline", "cron_lock.log")
TRACKER_FILE = os.path.join(LOCK_DIR, "scheduler_tracker.json")
# (2026-08-17): 蒸馏补偿——sentinel T9 检测到蒸馏日缺跑/失败时写此标志，次日(周二)补跑
DISTILL_CATCHUP_FLAG = os.path.join(LOCK_DIR, "distill_catchup.flag")

os.makedirs(LOCK_DIR, exist_ok=True)
os.makedirs(os.path.dirname(AUDIT_LOG), exist_ok=True)

# ── 时间窗口 ──
TIME_WINDOW_START = 6    # 06:00 开始（用户要求22:00-06:00训练）
TIME_WINDOW_END = 22     # 22:00 结束
# ── 异步执行控制 ──
MAX_CONCURRENT = 2  # 最多2个管道任务同时跑（防 CPU/IO 峰值）
_semaphore = Semaphore(MAX_CONCURRENT)
_running = {}  # pid -> {"name": task_name, "proc": Popen, "lock": lock_path}


def run_async(script, args, name, lock_path):
    """异步启动子进程，不阻塞调度循环"""
    if not _semaphore.acquire(timeout=30):  # 死锁修复：30s 拿不到槽位就跳过本轮
        print(f"SKIP {name}: 并发槽位耗尽 (MAX_CONCURRENT={MAX_CONCURRENT})，跳过本轮")
        return None
    try:
        proc = subprocess.Popen(
            ["python3", script] + args,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except Exception as e:
        # 2026-08-06 修复(A1)：Popen 异常 → 删 lock + 释放信号量 + 记审计，防信号量泄漏导致并发槽位永久耗尽
        release_lock(lock_path)
        _semaphore.release()
        try:
            with open(AUDIT_LOG, "a") as f:
                f.write(f"[{datetime.now().isoformat()}] FAIL Popen {name}: {e}\n")
        except Exception:
            pass
        print(f"FAIL {name}: Popen 启动失败 ({e})")
        return None
    _running[proc.pid] = {"name": name, "proc": proc, "lock": lock_path}


# ── 调度表 ──
# type: interval/clock
#   interval: 每N分钟一次，在时间窗口内反复执行
#   clock:   按指定时间点执行
SCHEDULE = [
    {"name": "reta",       "type": "interval", "every_min": 5,  "window": (6, 22), "script": "reta_pipeline.py",         "args": [],              "desc": "晋升清扫"},
    {"name": "l1-harvest", "type": "interval", "every_min": 10, "window": (6, 22), "script": "l1_harvest_all.py",       "args": [],              "desc": "L1全Agent摘录(P0-1B)"},
    # P2-4 死配置已注释（times=[] 永不触发；真正调度为 distill-monday）
    # {"name": "distill",    "type": "clock",    "times": [],                          "script": "l4_smart.py",           "args": ["distill"],     "desc": "知识蒸馏"},
    {"name": "distill-monday","type":"clock",  "times": ["12:00"], "day": 0,         "script": "l4_smart.py",           "args": ["distill", "--llm", "--entities"], "desc": "知识蒸馏(周一,本地LLM提炼+实体抽取)"},
    # P2-4 死配置已注释（times=[] 永不触发；decay 由 RETA 管道承担）
    # {"name": "decay",      "type": "clock",    "times": [],                          "script": "l4_smart.py",           "args": ["decay"],       "desc": "记忆衰减"},
    # B卡修复(2026-08-08): reinforce/verify 已改为 crontab 直连脚本（l3_enhance.py / g3_inject_verify.py --sr 0.5），
    # 删除 SCHEDULE 条目防双入口重复调度（cron 06:15/06:00 + SCHEDULE 06:30/07:00 曾重复触发 l3_enhance.py 2 小时）
    # R3 修正(2026-08-11, ): graph 定时任务彻底移除 —— 图通道生产默认关闭(SIKU_GRAPH_CHANNEL=0)，
    #   l3_retrieval.py 检索不读 graph_edges；且 R2 图谱审计()结论: graph_edges 15.2M 边 95.27% 伪边(same_type+supports)。
    #   → 定时重建图谱=白干活，降频保留无意义。图谱仅实验启用图通道时手动建一次:
    #     python3 scripts/graph_builder.py --full --force confirm
    #   （守卫保留: 有边拒绝 exit 2 + --force 需二次确认，手动重建同样受保护）
    # {"name": "graph",      "type": "clock",    "times": ["03:00"], "day": 0,          "script": "graph_builder.py",      "args": ["--full", "--force", "confirm"], "desc": "知识图谱(实验启用时手动建一次)"},
    {"name": "sentinel",   "type": "interval", "every_min": 360, "window": (6, 22), "script": "sentinel.py",            "args": [],              "desc": "哨兵自检"},
    {"name": "sentinel-full", "type": "clock",    "times": ["06:15","18:00"],     "script": "sentinel.py",            "args": ["--full"],      "desc": "完整哨兵(含Profile格式)"},
            {"name": "weekly_backup", "type": "clock", "times": ["06:20"],                   "script": "weekly_backup.py",      "args": [],              "desc": "周日全量备份"},
    {"name": "chroma_sync", "type": "clock", "times": ["06:00"], "script": "chroma_sync.py", "args": [], "desc": "Chroma向量同步"},
    {"name": "dashboard",  "type": "clock",    "times": ["07:00"],                   "script": "report_dashboard.py",   "args": [],              "desc": "仪表盘"},
    {"name": "cheatsheet", "type": "clock",    "times": ["07:30"],                   "script": "update_cheatsheet.py",  "args": [],              "desc": "速查卡更新"},
    {"name": "snapshot",   "type": "clock",    "times": ["07:30"],                   "script": "snapshot.py",           "args": ["create"],      "desc": "每日快照"},
    {"name": "concern",    "type": "clock",    "times": ["06:05"],                   "script": "concern_lifecycle.py",  "args": ["activity"],   "desc": "Concern活性更新(每日)"},
    {"name": "arbitration", "type": "clock",    "times": ["07:00"],                   "script": "concern_lifecycle.py",  "args": ["audit"],      "desc": "冲突仲裁检查(每日)"},
    {"name": "gene-promote", "type": "interval", "every_min": 60, "window": (6, 22), "script": "gene_promote.py", "args": [], "desc": "基因晋升: memory-bank→四库"},
    {"name": "importance",  "type": "clock",    "times": ["19:00"],                   "script": "importance_scoring.py", "args": ["--dual-axis"],      "desc": "重要性评分(双轴加权,)"},
    {"name": "score-baseline", "type": "clock", "times": ["19:05"],                   "script": "evolution_baseline.py", "args": ["--source", "importance"], "desc": "评分基线收集(每日,importance后,)"},
    # [DEPRECATED-2026-08-27] 停用: g1_check 全库回写事故元凶( P1), 06:20 每日执行
    # {"name": "g1_check",   "type": "clock",    "times": ["06:20"],                   "script": "g1_check.py",             "args": [],              "desc": "G1质检检查(每日)"},
    {"name": "usage-feedback", "type": "clock", "times": ["06:45"],                   "script": "l3_usage_feedback.py",    "args": [],              "desc": "使用反馈回写(每日)"},
    # P2-G 审计降级：audit_log 定期 hash 存档 + integrity_check
    # script 绝对路径（跨侧：审计机制归属 hermes 侧 $HERMES_HOME/scripts）；06:10/06:12 避开 06:00/06:05/06:20/06:45
    {"name": "audit-archive", "type": "clock", "times": ["06:10"],                   "script": os.path.join(_HERMES_HOME, "scripts", "audit_integrity.py"), "args": ["archive"], "desc": "审计日志hash存档(每日,P2-G)"},
    {"name": "audit-integrity", "type": "clock", "times": ["06:12"],                   "script": os.path.join(_HERMES_HOME, "scripts", "audit_integrity.py"), "args": ["integrity"], "desc": "审计库完整性检查(每日,P2-G)"},
    # [DEPRECATED-2026-08-27] 停用: 旧质检脚本( P1)
    # {"name": "g2_label",   "type": "clock",    "times": ["06:40"],                   "script": "g2_label.py",             "args": ["--batch-genes", "--labeler", "g2_batch"], "desc": "G2批量标注(每日)"},
    # [DEPRECATED-2026-08-27] 停用: 旧质检脚本( P1)
    # {"name": "g3_writeback", "type": "clock",  "times": ["07:05"],                   "script": "g3_inject_verify.py",    "args": ["--writeback", "--reviewer", "g3_batch"], "desc": "G3批量复核回写(每日)"},
    # [DEPRECATED-2026-08-27] 停用: 旧质检脚本( P1)
    # {"name": "g2_annotate", "type": "clock",    "times": ["06:35", "12:00"],                   "script": "g2_annotate.py",         "args": [],              "desc": "G2标注(每日)"},
    {"name": "l4c_sync",    "type": "clock",    "times": ["12:30"], "day": 0,         "script": "l4c_sync.py",           "args": [],              "desc": "L4c同步(周一)"},
]

# 构建查询索引
NAME_MAP = {p["name"]: p for p in SCHEDULE}

# ── 运行锁 ──
def _pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True

def acquire_lock(name, timeout_min=10):
    lock_file = os.path.join(LOCK_DIR, f"{name}.lock")
    if os.path.exists(lock_file):
        # 2026-08-05 修复：lock 写 PID，持有者已死 → 孤儿锁立即回收（防 30 分钟 SKIP 窗口）
        try:
            with open(lock_file) as f:
                _c = f.read().strip()
            _pid = int(_c.split()[0]) if _c else 0
            if _pid > 0 and not _pid_alive(_pid):
                os.remove(lock_file)
        except Exception:
            pass
        if os.path.exists(lock_file):
            age = time.time() - os.path.getmtime(lock_file)
            if age < timeout_min * 60:
                print(f"SKIP {name}: 上一轮未完成 ({age:.0f}s)")
                return None
            os.remove(lock_file)
    with open(lock_file, "w") as f:
        f.write(f"{os.getpid()} {time.time()}")
    return lock_file

def release_lock(lock_file):
    if lock_file and os.path.exists(lock_file):
        os.remove(lock_file)

def in_window(start_h, end_h):
    now = datetime.now()
    return start_h <= now.hour < end_h

# ── 调度跟踪 ──
def load_tracker():
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE) as f:
            return json.load(f)
    return {}

def save_tracker(tracker):
    with open(TRACKER_FILE, "w") as f:
        json.dump(tracker, f, indent=2)
    # tracker 字段说明:
    #   tracker["reta"]    = 上次执行时间（ISO格式）
    #   tracker["reta_rc"] = reta 退出码（0=成功, 非0=失败），供 SOP-02 判定


def cleanup_finished():
    """清理已结束的进程，释放信号量 + 锁 + 记录 reta_rc"""
    for pid, info in list(_running.items()):
        try:
            pid_result, status = os.waitpid(pid, os.WNOHANG)
            if pid_result != 0:
                name = info["name"]
                lock_path = info["lock"]
                if lock_path and os.path.exists(lock_path):
                    os.remove(lock_path)
                del _running[pid]
                _semaphore.release()
                # 记录返回码到 tracker
                rc = os.WEXITSTATUS(status) if os.WIFEXITED(status) else -1
                try:
                    tracker = load_tracker()
                    tracker[name + "_rc"] = rc
                    if name == "reta":
                        tracker["reta_rc"] = rc
                    save_tracker(tracker)
                except Exception:
                    pass
                # (2026-08-17): 蒸馏补偿——补跑成功清标志
                if name == "distill-monday" and rc == 0 and os.path.exists(DISTILL_CATCHUP_FLAG):
                    try:
                        os.remove(DISTILL_CATCHUP_FLAG)
                    except Exception:
                        pass
                now = datetime.now()
                status_str = "OK" if rc == 0 else f"FAIL(exit={rc})"
                log = f"[{now.isoformat()}] {status_str} {name} (async completed)\n"
                with open(AUDIT_LOG, "a") as f:
                    f.write(log)
        except ChildProcessError:
            lock_path = info["lock"]
            if lock_path and os.path.exists(lock_path):
                os.remove(lock_path)
            del _running[pid]
            _semaphore.release()


def should_run(entry, tracker):
    """判断当前是否该执行这个管道"""
    now = datetime.now()
    name = entry["name"]
    last = tracker.get(name)

    # 新增: day 字段检查（0=Mon ... 6=Sun，与 Python datetime.weekday() 一致）
    if "day" in entry and entry["day"] is not None:
        days = entry["day"] if isinstance(entry["day"], list) else [entry["day"]]
        # (2026-08-17): 蒸馏补偿——补跑标志存在时 distill-monday 允许周二(1)补跑
        if entry.get("name") == "distill-monday" and os.path.exists(DISTILL_CATCHUP_FLAG):
            days = list(set(days + [1]))
        if now.weekday() not in days:
            return False

    if entry["type"] == "interval":
        # 间隔型：检查距上次运行是否超过间隔
        every = entry["every_min"]
        wstart, wend = entry.get("window", (6, 22))
        if not in_window(wstart, wend):
            return False
        if last is None:
            return True  # 从未跑过
        elapsed = (now - datetime.fromisoformat(last)).total_seconds() / 60
        return elapsed >= every

    elif entry["type"] == "clock":
        # 定点型：检查当前分钟是否匹配指定时间点
        current = now.strftime("%H:%M")
        if current not in entry["times"]:
            return False
        if last is None:
            return True
        last_dt = datetime.fromisoformat(last)
        # 同一分钟内不重复执行
        return (now - last_dt).total_seconds() >= 60

    return False

# ── 执行 ──
def run_pipeline(name):
    entry = NAME_MAP.get(name)
    if not entry:
        print(f"Unknown: {name}")
        return 1

    now = datetime.now()
    lock = acquire_lock(name, 30)
    if lock is None:
        return 0

    script = os.path.join(SCRIPTS, entry["script"])
    args = entry.get("args", [])
    cmd = f"python3 {script} " + " ".join(args)

    print(f"[{now.isoformat()}] START {name}: {entry['desc']}")
    print(f"  CMD: {cmd}")
    run_async(script, args, name, lock)

    # 更新跟踪
    tracker = load_tracker()
    tracker[name] = now.isoformat()
    save_tracker(tracker)

    log = f"[{now.isoformat()}] STARTED {name}: {entry['desc']}\n"
    with open(AUDIT_LOG, "a") as f:
        f.write(log)
    return 0

def scheduler():
    """统一调度：每分钟由cron触发，检查所有管道是否到时间"""
    triggered = 0
    for entry in SCHEDULE:
        cleanup_finished()  # 死锁修复：循环内先清理，防止循环卡死导致信号量/锁永不释放
        if should_run(entry, load_tracker()):
            run_pipeline(entry["name"])
            triggered += 1
    # 2026-08-05 修复：等待本轮子进程全部结束再退出（含删 lock/释放信号量），
    # 防止孤儿 lock 残留 30 分钟 SKIP 窗口 + scheduler 进程堆积；watchdog 600s 兜底
    deadline = time.time() + 570
    while _running and time.time() < deadline:
        cleanup_finished()
        time.sleep(5)
    cleanup_finished()
    return 0


def _watchdog_force_exit():
    """watchdog 强杀前处理：terminate 子进程 → 删 lock → 记 tracker → bounded wait → os._exit"""
    print("WATCHDOG: scheduler 运行超 10 分钟，强制退出")
    snap = list(_running.values())
    # 1) terminate 所有在跑子进程（防孤儿进程）
    for info in snap:
        try:
            info["proc"].terminate()
        except Exception:
            pass
    # 2) 删 lock（防孤儿锁 30 分钟 SKIP 窗口）
    for info in snap:
        release_lock(info.get("lock"))
    # 3) 记录 tracker（被强杀，rc=-9）
    try:
        tracker = load_tracker()
        for info in snap:
            tracker[info["name"] + "_rc"] = -9
        save_tracker(tracker)
    except Exception:
        pass
    # 4) bounded wait 4s：回收已 terminate 的子进程（信号量/锁一并释放）
    deadline = time.time() + 4
    while _running and time.time() < deadline:
        cleanup_finished()
        time.sleep(0.5)
    # 5) 兜底：仍未退出的 kill + 清理残留
    for pid, info in list(_running.items()):
        try:
            info["proc"].kill()
        except Exception:
            pass
        del _running[pid]
        _semaphore.release()
    try:
        with open(AUDIT_LOG, "a") as f:
            f.write(f"[{datetime.now().isoformat()}] WATCHDOG_KILL {len(snap)} children\n")
    except Exception:
        pass
    os._exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("USAGE:")
        print("  python3 pipeline_runner.py scheduler   # 统一调度（推荐，配cron * * * * *）")
        print("  python3 pipeline_runner.py all         # 全部跑一次")
        print(f"  python3 pipeline_runner.py <name>     # 单个管道: {', '.join(NAME_MAP.keys())}")
        sys.exit(1)

    mode = sys.argv[1]
    if mode == "scheduler":
        # 死锁兜底：10 分钟后强制退出，防止任何未知挂起导致实例常驻（launchd 会重新拉起）
        import threading
        _wt = threading.Timer(600, _watchdog_force_exit)
        _wt.daemon = True  # 2026-08-05 修复：daemon 线程不阻止进程退出，scheduler 无子任务时秒退，防进程堆积
        _wt.start()
        sys.exit(scheduler())
    elif mode == "all":
        for p in SCHEDULE:
            run_pipeline(p["name"])
    else:
        sys.exit(run_pipeline(mode))
