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

"""
P3 L4智能层 - 知识蒸馏 + 记忆活性 + 注入效果验证
四库v4.1 (去掉训练数据引擎)
"""
import json, os, sys, sqlite3, hashlib, re, time
import query_cache_invalidate  # S2 M3 清缓存挂钩（轻量纯 stdlib）
# (2026-08-16): 蒸馏入库相似度去重守卫（增量防检索池膨胀；阈值/开关可配）
# 缺失/导入失败 → SimDedupGuard=None → store() 回退原行为（INSERT OR IGNORE 精确去重不变）
try:
    from distill_sim_dedup import SimDedupGuard
except Exception as _dedup_import_err:
    SimDedupGuard = None
    # (2026-08-17): 守卫绕过留痕——导入失败 → audit WARN（gate_check 通道），不静默降级
    try:
        import datetime as _dt
        _awd = os.environ.get("SIKU_AUDIT_DIR") or os.path.join(_SIKU_ROOT, "audit", "write")
        os.makedirs(_awd, exist_ok=True)
        with open(os.path.join(_awd, "audit-%s.yaml" % _dt.datetime.now().strftime("%Y-%m-%d")), "a") as _af:
            _af.write("---\nts: %s\nop: dedup-guard\nop_type: warn\neid: import-fail\ntype: guard\nsummary_a: SimDedupGuard 导入失败 → 相似度去重降级: %s\nalert: yes\n---\n"
                      % (_dt.datetime.now().isoformat(), _dedup_import_err))
    except Exception:
        pass
from datetime import datetime, timezone, timedelta
from collections import defaultdict

BASE = _SIKU_ROOT
# A卡(2026-08-09 ): DB_PATH 支持环境变量覆盖（轨道测试用 /tmp 副本库）
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE, "memory_store.db"))

# ── 本地 LLM 蒸馏配置（distill --llm 分支，A卡 2026-08-09） ──
# (2026-08-17): 通道前置修复——8083 无服务（今日蒸馏 21 次 LLM 全 502 降级），
# 文本蒸馏改走 8081（mlx Qwen3.8-27B-8bit 纯文本）；视觉任务才用 8083。env 覆盖保留。
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "http://localhost:8081/v1/chat/completions")
LLM_MODEL = os.environ.get("LLM_MODEL", "mlx-community/Qwen3.8-27B-8bit")
LLM_TIMEOUT = int(os.environ.get("LLM_TIMEOUT", "60"))

def db():
    # (2026-08-17): busy_timeout=5000——并发写卡点不再 locked 崩溃（双进程同库场景实测）
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

def now():
    return datetime.now(timezone(timedelta(hours=8))).isoformat()

# P2-1 修复：蒸馏日志孤儿化——增加文件日志（tee：stdout + logs/l4_smart.log）
_LOG_PATH = os.path.join(BASE, "logs", "l4_smart.log")

def _log(msg):
    """同时输出到 stdout 与 l4_smart.log（带时间戳）"""
    line = "%s %s" % (now(), msg)
    print(msg)
    try:
        os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
        with open(_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        print("l4_smart log warn: %s" % e)

def _track_distill(rc, note=""):
    """蒸馏补偿( 2026-08-17): 完成标志写 scheduler_tracker（复用 pipeline_runner 通道）。
    sentinel T9 据此判断「蒸馏日未完成→告警/次日补跑」，防策展断供。"""
    try:
        tf = os.path.join(BASE, ".locks", "scheduler_tracker.json")
        os.makedirs(os.path.dirname(tf), exist_ok=True)
        tr = {}
        if os.path.exists(tf):
            with open(tf, encoding="utf-8") as f:
                tr = json.load(f)
        tr["distill"] = now()
        tr["distill_rc"] = rc
        if note:
            tr["distill_note"] = note
        with open(tf, "w", encoding="utf-8") as f:
            json.dump(tr, f, indent=2)
    except Exception:
        pass

# ── A卡(2026-08-09): distill 运行锁（.locks/distill.lock，PID+超时，语义对齐 pipeline_runner） ──
_DISTILL_LOCK = os.path.join(BASE, ".locks", "distill.lock")

def _pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True

def acquire_distill_lock(timeout_min=10):
    """拿 distill 锁；上轮未完成(age<10min)返回 None → 调用方 SKIP"""
    try:
        os.makedirs(os.path.dirname(_DISTILL_LOCK), exist_ok=True)
    except Exception:
        pass
    if os.path.exists(_DISTILL_LOCK):
        try:
            with open(_DISTILL_LOCK) as f:
                _c = f.read().strip()
            _pid = int(_c.split()[0]) if _c else 0
            if _pid > 0 and not _pid_alive(_pid):
                os.remove(_DISTILL_LOCK)  # 孤儿锁回收（持有者已死）
        except Exception:
            pass
        if os.path.exists(_DISTILL_LOCK):
            age = time.time() - os.path.getmtime(_DISTILL_LOCK)
            if age < timeout_min * 60:
                print(f"SKIP distill: 上一轮未完成 ({age:.0f}s)")
                return None
            os.remove(_DISTILL_LOCK)  # 超时陈旧锁
    with open(_DISTILL_LOCK, "w") as f:
        f.write(f"{os.getpid()} {time.time()}")
    return _DISTILL_LOCK

def release_distill_lock(lock_file):
    try:
        if lock_file and os.path.exists(lock_file):
            os.remove(lock_file)
    except Exception:
        pass

# ── L4a 模式识别（pattern recognition） ──

class PatternExtractor:
    """从L2/L3中提取重复模式"""

    @staticmethod
    def extract():
        conn = db()
        rows = conn.execute("""
            SELECT id, summary, content, source_agent, type, confidence
            FROM memory_store
            WHERE confidence > 0.6
              AND type != 'L4b'   -- A卡(2026-08-09): 排除L4b输出自身，防蒸馏产物回流簇内
                                   -- 改变成员集 → --llm 幂等id(md5(agent+成员id))失效重复入库
            ORDER BY confidence DESC
            LIMIT 500
        """).fetchall()
        conn.close()

        # 按source_agent聚类
        clusters = defaultdict(list)
        for r in rows:
            clusters[r["source_agent"] or "unknown"].append(dict(r))

        patterns = []
        for agent, entries in clusters.items():
            if len(entries) >= 3:
                # 同类条目≥3即视为模式；pattern_summary 取簇内代表性条目的语义内容
                # （2026-08-09 体检B卡修复：原"有N条相关记录"为计数统计非智慧条目）
                rep = max(entries, key=lambda e: (e["confidence"], len(e["summary"] or "")))
                pattern_summary = (rep["summary"] or "").strip() or (rep["content"] or "").strip()[:100] or f"{agent} 模式"
                patterns.append({
                    "source_agent": agent,
                    "count": len(entries),
                    "avg_confidence": round(sum(e["confidence"] for e in entries) / len(entries), 2),
                    "pattern_summary": pattern_summary,
                    "references": [e["id"] for e in entries[:5]],
                    # A卡(--llm): 簇内全成员 id + 摘要（供本地 LLM 跨条目提炼；B卡路径不读取）
                    "member_ids": [e["id"] for e in entries],
                    "member_summaries": [{
                        "id": e["id"], "source_agent": e.get("source_agent") or agent,
                        "summary": (e.get("summary") or "")[:200]
                    } for e in entries]
                })

        return patterns

# ── L4b 知识蒸馏（wisdom distillation） ──

class WisdomDistiller:
    """从模式中蒸馏出可复用知识"""

    @staticmethod
    def distill(patterns, use_llm=False, extract_entities=False):
        """模式 → L4b 蒸馏条目。

        use_llm=False → B卡行为（id=内容哈希，references 仅内嵌 content JSON，向后兼容）。
        use_llm=True  → 每簇调本地 LLM(8081) 跨条目提炼；幂等 id=md5(agent+排序成员id)
                        （LLM 输出非确定性，不能按 summary 哈希，否则重复入库）；
                        LLM 调用失败/非法 JSON → 降级复用 B卡 distill 分支产物（不复制逻辑）。
        extract_entities=True → (2026-08-17) 独立实体抽取步骤：
                        文本提炼成功后追加 _extract_entities（复用 _llm_call），
                        失败/空 → entities/relations 不设（NULL）→ 不阻塞文本入库；
                        id 由 agent+成员id 决定，与实体无关 → 幂等性不变。
        """
        wisdoms = []
        llm_ok = llm_fail = 0
        ent_ok = ent_fail = 0
        for p in patterns:
            wisdom = {
                # 2026-08-09 体检B卡修复：id 改按内容哈希（原按 count，count 每次变化即产生新 id，
                # 导致同质统计条目反复写入且 INSERT OR IGNORE 永久去重停滞）
                "id": hashlib.md5(f"l4b_{p['source_agent']}_{p['pattern_summary']}".encode()).hexdigest()[:16],
                "type": "L4b",
                "summary": p["pattern_summary"],
                "source_agent": p["source_agent"],
                "confidence": p["avg_confidence"],
                "importance": min(10, 5 + p["count"] // 5),
                "timestamp": now(),
                "references": json.dumps(p["references"])
            }
            if use_llm:
                member_ids = sorted(set(p.get("member_ids") or p["references"]))
                try:
                    refined = WisdomDistiller._llm_refine(p)
                    if refined:
                        # A卡幂等：md5(agent+排序成员id)，同簇重跑同 id → INSERT OR IGNORE 去重
                        wisdom["id"] = hashlib.md5(
                            f"l4b_llm_{p['source_agent']}_{','.join(member_ids)}".encode()
                        ).hexdigest()[:16]
                        wisdom["summary"] = refined["summary"]
                        wisdom["content"] = refined["insight"]
                        wisdom["db_references"] = json.dumps(member_ids, ensure_ascii=False)
                        llm_ok += 1
                        # : 独立实体抽取——失败降级不阻塞文本入库，id 不变
                        if extract_entities:
                            entities, relations = WisdomDistiller._extract_entities(p)
                            if entities or relations:
                                if entities:
                                    wisdom["entities"] = json.dumps(entities, ensure_ascii=False)
                                if relations:
                                    wisdom["relations"] = json.dumps(relations, ensure_ascii=False)
                                ent_ok += 1
                            else:
                                ent_fail += 1
                                _log(f"实体抽取失败/空 {p['source_agent']}: 降级——文本入库不受影响")
                    else:
                        llm_fail += 1  # 空输出/非法 JSON → 保留 B卡 wisdom（降级）
                except Exception as e:
                    llm_fail += 1
                    _log(f"LLM蒸馏失败 {p['source_agent']}: {e}")
            wisdoms.append(wisdom)
        if extract_entities:
            _log(f"实体抽取: 成功{ent_ok} 失败/空{ent_fail}")
        return wisdoms, llm_ok, llm_fail

    @staticmethod
    def _build_entity_prompt(p):
        """: 实体抽取 prompt——输入=簇内成员摘要，输出 entities/relations JSON"""
        members = p.get("member_summaries") or []
        if not members:
            return None
        lines = "\n".join(
            f"- id={m['id']} source={m.get('source_agent', '?')} summary={m['summary']}"
            for m in members
        )
        return (
            "你是四库实体抽取器。下面是同一来源簇内 %d 条记忆条目的摘要：\n%s\n\n"
            "任务：抽取这些条目共同提及的核心实体（人名/系统/项目/规则/概念等）"
            "及其相互关系（仅跨条目或重复出现的，单条独有细节不要）。\n"
            "只输出JSON："
            '{"entities": ["实体1", "实体2", ...], "relations": [{"source": "实体A", "target": "实体B", "type": "关系类型"}]}\n'
            "要求：entities ≤10 个且每个≤20字；relations ≤10 条且 type ≤12字；无关系可省略 relations。"
        ) % (len(members), lines)

    @staticmethod
    def _parse_entity_output(text):
        """: 容错解析实体输出——剥围栏/取首个{...}/json.loads；结构非法返回 (None, None)"""
        if not text:
            return None, None
        t = text.strip()
        t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t, flags=re.S).strip()
        m = re.search(r"\{.*\}", t, re.S)
        if m:
            t = m.group(0)
        try:
            d = json.loads(t)
        except Exception:
            return None, None
        if not isinstance(d, dict):
            return None, None
        entities, relations = None, None
        raw_ents = d.get("entities")
        if isinstance(raw_ents, list):
            ents = [str(e).strip()[:20] for e in raw_ents if str(e).strip()][:10]
            entities = ents if ents else None
        raw_rels = d.get("relations")
        if isinstance(raw_rels, list):
            rels = []
            for r in raw_rels[:10]:
                if isinstance(r, dict):
                    s = str(r.get("source") or "").strip()[:20]
                    tgt = str(r.get("target") or "").strip()[:20]
                    tp = str(r.get("type") or "").strip()[:12]
                    if s and tgt:
                        rels.append(f"{s}-[{tp or '相关'}]->{tgt}")
                elif str(r).strip():
                    rels.append(str(r).strip()[:60])
            relations = rels if rels else None
        return entities, relations

    @staticmethod
    def _extract_entities(p):
        """: 独立实体抽取步骤——复用 _llm_call；任何失败返回 (None, None) 不抛出"""
        try:
            prompt = WisdomDistiller._build_entity_prompt(p)
            if prompt is None:
                return None, None
            raw = WisdomDistiller._llm_call(prompt)
            return WisdomDistiller._parse_entity_output(raw)
        except Exception:
            return None, None

    @staticmethod
    def _build_llm_prompt(p):
        """跨条目提炼 prompt：输入=簇内成员 summaries（带 id/source_agent）"""
        members = p.get("member_summaries") or []
        if not members:
            return None
        lines = "\n".join(
            f"- id={m['id']} source={m.get('source_agent', '?')} summary={m['summary']}"
            for m in members
        )
        return (
            "你是四库知识蒸馏器。下面是同一来源簇内 %d 条记忆条目的摘要：\n%s\n\n"
            "任务：提炼这些条目之间的共同规律/模式，形成一条跨条目可复用知识。\n"
            "要求：1) 200字以内 2) 不复述任何单条原文 3) 只输出JSON："
            '{"summary": "一句话摘要≤60字", "insight": "规律/模式洞察≤200字"}'
        ) % (len(members), lines)

    @staticmethod
    def _llm_call(prompt):
        """调本地 LLM（OpenAI 兼容端点，60s 超时），返回原始文本

        (2026-08-17): ①thinking 关闭——8081 Qwen3 思考模式会先烧完
        max_tokens 再吐 content（实测 finish=length、content 缺失）→ 文本蒸馏显式禁用推理
        换取确定性 JSON 输出；②.get("content") 硬化——content 仍可能 null/缺失 → 返回
        None 走降级不崩溃（今日 8083 502 之外的第二类通道故障）。llama.cpp 等兼容端点
        忽略未知请求字段，不影响旧通道。
        """
        import urllib.request
        payload = json.dumps({
            "model": LLM_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "max_tokens": 512,
            "stream": False,
            "thinking": {"type": "disabled"}
        }).encode("utf-8")
        req = urllib.request.Request(
            LLM_ENDPOINT, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=LLM_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        try:
            return data["choices"][0]["message"].get("content")
        except Exception:
            return None

    @staticmethod
    def _parse_llm_output(text):
        """容错解析本地模型输出：剥代码围栏/取首个{...}/json.loads，失败返回 None"""
        if not text:
            return None
        t = text.strip()
        t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t, flags=re.S).strip()
        m = re.search(r"\{.*\}", t, re.S)
        if m:
            t = m.group(0)
        try:
            d = json.loads(t)
        except Exception:
            return None
        if not isinstance(d, dict):
            return None
        summary = str(d.get("summary") or "").strip()
        insight = str(d.get("insight") or "").strip()
        if not summary and not insight:
            return None
        return {"summary": summary[:60], "insight": insight[:300]}

    @staticmethod
    def _llm_refine(p):
        prompt = WisdomDistiller._build_llm_prompt(p)
        if prompt is None:
            return None
        raw = WisdomDistiller._llm_call(prompt)
        return WisdomDistiller._parse_llm_output(raw)

    @staticmethod
    def store(wisdoms):
        """写入L4b条目到DB

        P2-6 双轨说明(2026-08-03): 本方法只写 memory_store.db (type=L4b, INSERT OR IGNORE 去重, 当前23条)。
        磁盘 curation/ 下全量 yaml (~2040文件, 前轮审计口径约1960条有效) 由另一管道落盘:
        reta_pipeline.py 蒸馏全量写 curation/insights 等 yaml + memory-bank 基因写 genes/。
        双轨定位: DB=策展蒸馏条目(经本方法去重写入, 供检索/注入); 磁盘=全量蒸馏/基因 yaml 归档。
        两处各有用途, 数量差异为设计使然, 不强行对齐。"""

        conn = db()
        # A卡(2026-08-09): references 落点 = memory_store."references" 列（可空，向后兼容）。
        # 实测原表无该列 → ALTER 加列（幂等：先查后加）；references 是 SQLite 关键字，
        # 必须带引号 "references"。新增可空列不影响既有写入（显式列清单）与检索。
        # (2026-08-17): entities/relations 实体级规范化可空列——同模式幂等 ALTER
        _cols = [r[1] for r in conn.execute("PRAGMA table_info(memory_store)").fetchall()]
        if "references" not in _cols:
            conn.execute('ALTER TABLE memory_store ADD COLUMN "references" TEXT')
            conn.commit()
        for _ent_col in ("entities", "relations"):
            if _ent_col not in _cols:
                conn.execute('ALTER TABLE memory_store ADD COLUMN "%s" TEXT' % _ent_col)
                conn.commit()
        inserted = 0
        sim_skipped = 0
        # (2026-08-16): 入库前相似度去重（增量防检索池膨胀）
        # 用既有非废弃条目（summary+content）建索引；初始化失败 → guard=None 回退原行为
        guard = None
        if wisdoms and SimDedupGuard is not None:
            try:
                guard = SimDedupGuard()
                if not guard.enabled:
                    guard = None  # 开关禁用 → 跳过索引构建，完全回退原行为
                else:
                    guard.load_from_db(conn)
                    _log(f"相似度去重守卫: 已索引{guard.stats['indexed']}条既有内容 (阈值{guard.threshold}, enabled={guard.enabled})")
            except Exception as e:
                _log(f"相似度去重守卫初始化失败，回退原行为: {e}")
                guard = None
        for w in wisdoms:
            try:
                text = (w.get("content") or w.get("summary") or "")
                if guard is not None:
                    dup, sim, reason = guard.is_duplicate(text)
                    if dup:
                        sim_skipped += 1
                        _log(f"相似度去重跳过: sim={sim:.3f} ({reason}) {(w.get('summary') or '')[:40]}")
                        continue
                cur = conn.execute("""
                    INSERT OR IGNORE INTO memory_store
                    (id, version, timestamp, type, summary, content,
                     confidence, importance, source_agent, "references",
                     entities, relations)
                    VALUES (?, '1.0', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (w["id"], w["timestamp"], w["type"], w["summary"],
                      w.get("content") or json.dumps(w), w["confidence"], w["importance"], w["source_agent"],
                      w.get("db_references"),  # A卡: --llm 时=簇内成员id列表JSON；不带 --llm 为空(NULL)
                      w.get("entities"),  # : 实体抽取产物 JSON/None(NULL)，可空向后兼容
                      w.get("relations")))
                # 2026-08-09 体检B卡修复：INSERT OR IGNORE 被忽略时 rowcount=0，仅计真实新增
                if cur.rowcount > 0:
                    inserted += 1
                    if guard is not None:
                        guard.add(text)  # 注册入库内容，供同批后续条目增量比较
            except Exception:
                pass
        conn.commit()
        conn.close()
        query_cache_invalidate.invalidate_query_cache()  # S2 M3：L4b 蒸馏入库后清缓存
        if sim_skipped:
            _log(f"蒸馏入库相似度去重: 跳过{sim_skipped}条, 入库{inserted}条")
        return inserted

# ── 记忆活性跟踪 ──

class ActivityTracker:
    """跟踪记忆活性（最后注入时间、注入次数、修正次数）"""

    @staticmethod
    def update_activity(entry_id, action="injected"):
        conn = db()
        now_ts = now()
        conn.execute("""
            UPDATE memory_store
            SET updated_at = ?
            WHERE id = ?
        """, (now_ts, entry_id))
        conn.commit()
        query_cache_invalidate.invalidate_query_cache()  # S2 M3：活性 UPDATE 后清缓存

        # 活动日志
        log_file = os.path.join(BASE, "audit", "activity", f"activity-{datetime.now().strftime('%Y%m%d')}.jsonl")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, "a") as f:
            f.write(json.dumps({
                "timestamp": now_ts,
                "entry_id": entry_id,
                "action": action
            }, ensure_ascii=False) + "\n")
        conn.close()

    @staticmethod
    def get_stale(threshold_days=30):
        """获取过期的记忆（需要降级或归档）"""
        conn = db()
        cutoff = (datetime.now(timezone(timedelta(hours=8))) - timedelta(days=threshold_days)).isoformat()
        stale = conn.execute("""
            SELECT id, type, summary, confidence, importance
            FROM memory_store
            WHERE updated_at < ?
              AND type NOT IN ('L4b', 'permanent')
            ORDER BY importance ASC
            LIMIT 20
        """, (cutoff,)).fetchall()
        conn.close()
        return [dict(r) for r in stale]

    @staticmethod
    def decay_stale(entries):
        """衰减过时记忆的置信度"""
        conn = db()
        for e in entries:
            conn.execute("""
                UPDATE memory_store
                SET confidence = MAX(0.1, confidence - 0.1),
                    importance = MAX(1, importance - 1)
                WHERE id = ?
            """, (e["id"],))
        conn.commit()
        conn.close()
        query_cache_invalidate.invalidate_query_cache()  # S2 M3：衰减 UPDATE 后清缓存
        return len(entries)

# ── 注入效果验证 ──

class InjectVerifier:
    """验证pre_llm_call注入是否生效"""

    @staticmethod
    def sample_recent(limit=30):
        """获取最近注入的条目供复核抽样，首批≤30条"""
        conn = db()
        rows = conn.execute("""
            SELECT id, summary, type, confidence, source_agent,
                   updated_at as last_injected
            FROM memory_store
            ORDER BY updated_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    @staticmethod
    def verify_one(entry_id, expected_usage, actual_usage):
        """验证单条注入是否被正确使用"""
        passed = expected_usage.lower() in (actual_usage or "").lower()
        result = {
            "entry_id": entry_id,
            "expected": expected_usage[:100],
            "actual": (actual_usage or "")[:100],
            "passed": passed,
            "timestamp": now()
        }

        log_file = os.path.join(BASE, "audit", "inject", "verify.jsonl")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, "a") as f:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")

        return passed

# ── 主流程 ──



class CrossAgentDistiller:
    """不按source_agent分组，按语义聚类。
    同一语义簇出现在>=2个Agent中 -> 提升为L4b跨Agent知识"""
    
    MIN_CLUSTER_SIZE = 2
    
    @staticmethod
    def distill():
        conn = db()
        rows = conn.execute("""
            SELECT id, summary, content, source_agent, type, confidence
            FROM memory_store
            WHERE confidence > 0.6
              AND type NOT IN ('L4b', 'pattern')
        """).fetchall()
        conn.close()
        
        from collections import defaultdict
        by_agent = defaultdict(list)
        for r in rows:
            agent = r["source_agent"] or "unknown"
            by_agent[agent].append(r)
        
        agents = list(by_agent.keys())
        if len(agents) < 2:
            print("跨Agent蒸馏: Agent数不足(<2)，跳过")
            return 0
            
        distilled = 0
        for i in range(len(agents)):
            for j in range(i + 1, len(agents)):
                a_summaries = {e["summary"][:30] for e in by_agent[agents[i]]}
                b_summaries = {e["summary"][:30] for e in by_agent[agents[j]]}
                shared = a_summaries & b_summaries
                
                for s in shared:
                    a_match = next((e for e in by_agent[agents[i]] if e["summary"][:30] == s), None)
                    b_match = next((e for e in by_agent[agents[j]] if e["summary"][:30] == s), None)
                    if a_match and b_match:
                        avg_conf = (a_match["confidence"] + b_match["confidence"]) / 2
                        CrossAgentDistiller._save_cross_knowledge(
                            a_match["summary"], a_match["content"],
                            a_match["type"], avg_conf,
                            [agents[i], agents[j]]
                        )
                        distilled += 1
        
        print(f"跨Agent蒸馏: 提取{distilled}条跨Agent知识")
        return distilled
    
    @staticmethod
    def _save_cross_knowledge(summary, content, etype, confidence, agents):
        conn = db()
        ts = datetime.now(timezone.utc).isoformat()
        import uuid; eid = str(uuid.uuid4())
        conn.execute("""INSERT OR REPLACE INTO memory_store
            (id, version, timestamp, type, summary, content,
             confidence, trust_score, importance, half_life, source_agent,
             audit_log, merge_history, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (eid, '1.0', ts, 'L4b', summary, content,
             confidence, 0.9, 8, 'permanent', 'multi-agent',
             json.dumps([{'action':'cross_agent_distill','agents':agents,'distilled_at':ts}]),
             '[]', ts, ts))
        conn.commit()
        conn.close()
        query_cache_invalidate.invalidate_query_cache()  # S2 M3：跨Agent蒸馏写入后清缓存

def perm_audit(layer):
    """P0-2 权限写路径接入(2026-08-03): audit模式只记录不拦截, 观察1周后转enforce"""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import check_permission as cp
    cp.check(os.environ.get("SIKU_AGENT", "用户"), "write", layer, mode="audit")

def main():
    perm_audit("L4")
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "patterns":
            patterns = PatternExtractor.extract()
            print(json.dumps(patterns, indent=2, ensure_ascii=False))
            return
        elif cmd == "distill":
            # A卡: distill --llm 启用本地LLM蒸馏；不带参数=B卡行为
            # : distill --entities 启用独立实体抽取（隐式启用 --llm）；可单独 --llm 保持旧行为
            use_llm = ("--llm" in sys.argv[2:]) or ("--entities" in sys.argv[2:])
            extract_entities = "--entities" in sys.argv[2:]
            lock = acquire_distill_lock()
            if lock is None:
                return
            try:
                try:
                    patterns = PatternExtractor.extract()
                    wisdoms, llm_ok, llm_fail = WisdomDistiller.distill(patterns, use_llm=use_llm, extract_entities=extract_entities)
                    stored = WisdomDistiller.store(wisdoms)
                    if use_llm:
                        # grep 可查: LLM蒸馏 成功/失败数
                        _log(f"LLM蒸馏: 成功{llm_ok} 失败{llm_fail} → 蒸馏完成: {len(wisdoms)}条模式 → 存储{stored}条")
                    else:
                        _log(f"蒸馏完成: {len(wisdoms)}条模式 → 存储{stored}条")
                    _track_distill(0)  # : 完成标志
                except Exception as _de:
                    _log(f"蒸馏失败: {_de}")
                    _track_distill(1, str(_de))  # : 失败留痕 → sentinel 告警/次日补跑
                    raise
            finally:
                release_distill_lock(lock)
            cleanup_old_bak()
            return
        elif cmd == "stale":
            stale = ActivityTracker.get_stale(30)
            print(f"过时记忆: {len(stale)}条")
            return
        elif cmd == "decay":
            stale = ActivityTracker.get_stale(30)
            n = ActivityTracker.decay_stale(stale)
            print(f"衰减完成: {n}条")
            return
        elif cmd == "sample":
            sample = InjectVerifier.sample_recent(30)
            print(json.dumps(sample, indent=2, ensure_ascii=False))
            return
        elif cmd == "cross_distill":
            n = CrossAgentDistiller.distill()
            _log(f"跨Agent蒸馏完成: {n}条")
            return
        elif cmd == "verify":
            if len(sys.argv) < 4:
                print("用法: python3 l4_smart.py verify <entry_id> <expected_usage> [actual_usage]")
                return
            actual = sys.argv[4] if len(sys.argv) > 4 else sys.argv[3]
            passed = InjectVerifier.verify_one(sys.argv[2], sys.argv[3], actual)
            print(f"注入验证: {'✅ 通过' if passed else '❌ 未通过'}")
            return

    # 全流程
    print("=== P3 L4智能层 ===\n")

    # 1. 模式提取
    print("[1] 模式提取...")
    patterns = PatternExtractor.extract()
    print(f"  找到 {len(patterns)} 个模式")

    # 2. 知识蒸馏
    print("[2] 知识蒸馏...")
    wisdoms, _ok, _fail = WisdomDistiller.distill(patterns)  # A卡: 全流程默认 B卡行为
    stored = WisdomDistiller.store(wisdoms)
    print(f"  蒸馏 {len(wisdoms)} 条 → 存储 {stored} 条")

    # 3. 记忆活性
    print("[3] 记忆活性...")
    stale = ActivityTracker.get_stale(30)
    decayed = ActivityTracker.decay_stale(stale) if stale else 0
    print(f"  过时 {len(stale)} 条 → 衰减 {decayed} 条")

    # 4. 注入效果抽样（首批≤30条）
    print("[4] 注入效果抽样...")
    sample = InjectVerifier.sample_recent(30)
    print(f"  待验证: {len(sample)} 条")

    print("\n=== P3 完成 ===")
    cleanup_old_bak()


def cleanup_old_bak():
    """清理$SIKU_ROOT/smart/curation/下超过7天的.bak文件（包括.bak.xxx变体）"""
    curation_dir = os.path.join(BASE, "smart", "curation")
    if not os.path.isdir(curation_dir):
        print("[cleanup] curation目录不存在，跳过")
        return

    tz = timezone(timedelta(hours=8))
    cutoff = datetime.now(tz) - timedelta(days=7)

    deleted = 0
    freed_bytes = 0

    for fname in os.listdir(curation_dir):
        if not (fname.endswith(".bak") or ".bak." in fname):
            continue
        fpath = os.path.join(curation_dir, fname)
        if not os.path.isfile(fpath):
            continue

        mtime = datetime.fromtimestamp(os.path.getmtime(fpath), tz=tz)
        if mtime < cutoff:
            size = os.path.getsize(fpath)
            os.remove(fpath)
            deleted += 1
            freed_bytes += size

    if deleted > 0:
        freed_kb = freed_bytes / 1024
        print(f"[cleanup] 清理完成: 删除了{deleted}个.bak文件, 释放{freed_kb:.1f}KB")
    else:
        print("[cleanup] 无过期.bak文件需清理")


if __name__ == "__main__":
    main()


# ── 5-B: 跨Agent自动蒸馏 ──────────────────────────────────

