#!/usr/bin/env python3
"""L4c同步 — 蒸馏后触发，从L4b选精华同步到L4c智慧层"""
import os, yaml, glob, logging, re
from datetime import datetime

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

WISDOM = os.path.join(_SIKU_ROOT, "smart/wisdom")
CURATION = os.path.join(_SIKU_ROOT, "smart/curation")
LOG = os.path.join(_SIKU_ROOT, "logs/l4c_sync.log")

logging.basicConfig(filename=LOG, level=logging.INFO, format="%(asctime)s %(message)s")
# 日志截断
if os.path.exists(LOG) and os.path.getsize(LOG) > 1_048_576:
    with open(LOG) as f:
        lines = f.readlines()
    with open(LOG, "w") as f:
        f.writelines(lines[-5000:])

def get_meta(raw):
    m = re.match(r"^---\n(.*?)\n---", raw, re.DOTALL)
    if m:
        try:
            d = yaml.safe_load(m.group(1))
            if isinstance(d, dict): return d
        except: pass
    try:
        gen = yaml.safe_load_all(raw); first = next(gen)
        if isinstance(first, dict): return first
    except: pass
    try:
        d = yaml.safe_load(raw)
        if isinstance(d, dict): return d
    except: pass
    return None

def entry_score(data):
    # R4 修复 2026-08-29：confidence/importance 可能是空串或 str（yaml 脏数据），
    # 直接 >= 比较会 TypeError 导致条目被跳过（历史 19 条错误，见卡）
    def _num(v, default):
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return default
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    base = _num(data.get("importance"), 5)
    conf = _num(data.get("confidence"), 0.7)
    adj = 1 if conf >= 0.9 else (-1 if conf < 0.5 else 0)
    return max(1, min(10, int(base) + adj))

def main():
    os.makedirs(WISDOM, exist_ok=True)
    synced = errors = 0
    max_score = 0
    for subdir in ["principles", "insights"]:
        full_path = os.path.join(CURATION, subdir)
        if not os.path.isdir(full_path): continue
        for fpath in sorted(glob.glob(os.path.join(full_path, "*.yaml"))):
            try:
                with open(fpath) as f: raw = f.read()
                meta = get_meta(raw)
                if meta is None: errors += 1; continue
                s = entry_score(meta)
                if s >= 6:
                    base = meta.get("summary", meta.get("content", ""))
                    wisdom_doc = {
                        "id": meta.get("id"), "type": subdir, "source": "L4b_sync",
                        "summary": base[:500], "importance": s,
                        "confidence": meta.get("confidence", 0.7),
                        "synced_at": datetime.now().isoformat(),
                        "original_path": fpath,
                    }
                    eid = meta.get("id", os.path.basename(fpath).replace(".yaml", ""))
                    wpath = os.path.join(WISDOM, f"{eid}.yaml")
                    if not os.path.exists(wpath):
                        with open(wpath, "w") as wf:
                            yaml.dump(wisdom_doc, wf, allow_unicode=True, sort_keys=False)
                        synced += 1
                    if s > max_score: max_score = s
            except Exception as e:
                logging.error(f"{fpath}: {e}")
                errors += 1
    logging.info(f"L4c同步完成: {synced}条新同步 / {errors}条错误")
    print(f"✅ L4c同步完成: {synced}条新写入 wisdom，最高分 {max_score}")

if __name__ == "__main__":
    main()
