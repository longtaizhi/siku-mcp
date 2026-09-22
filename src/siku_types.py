#!/usr/bin/env python3
"""
四库类型枚举权威源

唯一权威源：四库所有管道（入库 reta_pipeline / 评分 importance_scoring /
检索 l3_retrieval / 路由 router / 策展 l4b/l4c）的类型枚举与校验统一引本模块，
禁各文件自行硬编码类型清单（防漂移，R13.18 Skill+Schema 架构）。

清单（维护者 2026-08-27 拍板，扩展批次 2026-08-27 再补 5 类）：
- 13 内容记忆类型：lesson/fact/decision/result/insight/principle/info/
  instruction/correction/record/idea/estimate/rule
- 13 系统资产类型：spec/skill/cron/workflow/rule/benchmark/asset/monitor
  + design/research/script/config/reference
  （注意：rule 同时出现在内容类型与资产类型——语义不同但共用字面量，
   资产 rule=门禁规则，内容 rule=行为规则；合并去重后合法类型共 25 种）
分层：L1 资产本体原位（单源化）+ L2 提炼 + L3 检索（memory_store.type）+ L4 策展版本化
"""

# ── 13 内容记忆类型（存量，回归零影响）──
CONTENT_TYPES = [
    "lesson", "fact", "decision", "result", "insight", "principle",
    "info", "instruction", "correction", "record", "idea", "estimate",
    "rule",
]

# ── 13 系统资产类型（8 旧 + 5 新，扩展批次 2026-08-27 拍板）──
ASSET_TYPES = [
    "spec",      # SOP（规范文档）
    "skill",     # 技能包
    "cron",      # 脚本/定时任务
    "workflow",  # 协作流程文档+看板规范
    "rule",      # 门禁规则（与内容类型 rule 共用字面量，语义=系统资产）
    "benchmark", # 评测基准
    "asset",     # 模型训练资产
    "monitor",   # 监控资产
    "design",    # 方案/设计文档（记忆芯片/项目方案/项目方案等）
    "research",  # 调研成果（外部调研/对标报告）
    "script",    # 工具脚本（非定时，siku_asset_ingest 除外）
    "config",    # 配置资产（config.yaml/permissions/role_matrix 等）
    "reference", # 知识参考（Hermes 文档/工具手册/README 类）
]

# 合法类型全集（去重保序：13 内容 + 13 资产，rule 交集 → 25 种字面量）
VALID_TYPES = list(dict.fromkeys(CONTENT_TYPES + ASSET_TYPES))

# rule 双重语义声明（内容/资产共用字面量）
RULE_DUAL_SEMANTIC = True

# 资产类型集合（判断"新类型走通用策展/评分路径"）
ASSET_SET = frozenset(ASSET_TYPES)


def validate_type(t, strict=True):
    """类型枚举校验：非法类型 → 拒（严格模式抛 ValueError）。

    - strict=True（管道默认）：非法类型抛 ValueError（拒收，不留脏数据）
    - strict=False（只读巡检/兼容）：非法类型返回 False 不抛
    返回: True=合法 / False=非法（strict=False 时）
    """
    t = (t or "").strip()
    if t in VALID_TYPES:
        return True
    if strict:
        raise ValueError(
            "非法四库类型: %r —— 合法类型共 %d 种（13 内容+13 资产）: %s"
            % (t, len(VALID_TYPES), ",".join(VALID_TYPES))
        )
    return False


def is_asset_type(t):
    """是否为 13 类系统资产类型（用于管道分支：资产走通用策展/评分路径）"""
    return (t or "").strip() in ASSET_SET


def is_content_type(t):
    """是否为 13 内容记忆类型"""
    return (t or "").strip() in CONTENT_TYPES


# ── 资产类型默认有效期（天）：资产本体单源化 → 永久（不设 expires_at）──
# 内容类型有效期仍由 reta_pipeline.DEFAULT_EXPIRES_DAYS 管理（不动）
ASSET_PERMANENT = True

# ── 跨库冲突三分类（维护者——唯一权威源，R13.18）──
# siku_conflict.py 判定结果枚举统一引本模块（禁各文件硬编码清单，防漂移）。
# 三分类语义：
#   independent —— 独立：主题语义无关，照常写入（互不影响）
#   extension   —— 扩展：同主题补充新信息，不矛盾，可共存/合并
#   conflict    —— 矛盾：同主题但结论/事实/语义相反——标记不覆盖（影子阶段只标记，G3 转正后拦截）
CONFLICT_TYPES = ["independent", "extension", "conflict"]

# 矛盾子类（conflict 细分——audit 留痕粒度，非独立分类）
CONFLICT_SUBTYPES = [
    "conflict_summary_conclusion",  # 同 summary 异结论（结论方向相反）
    "conflict_event_fact",          # 同事件异事实（数字/量词冲突）
    "conflict_rule_semantic",       # 同规则异语义（规则语义相反）
]

CONFLICT_SUBTYPE_SET = frozenset(CONFLICT_SUBTYPES)


def validate_conflict_type(t, strict=True):
    """冲突三分类校验：非法分类 → 拒（严格模式抛 ValueError）。

    - strict=True（管道默认）：非法抛 ValueError
    - strict=False（只读巡检/兼容）：非法返回 False 不抛
    返回: True=合法 / False=非法（strict=False 时）
    """
    t = (t or "").strip().lower()
    if t in CONFLICT_TYPES:
        return True
    if strict:
        raise ValueError(
            "非法冲突分类: %r —— 合法三分类: %s（矛盾子类: %s）"
            % (t, ",".join(CONFLICT_TYPES), ",".join(CONFLICT_SUBTYPES))
        )
    return False


def validate_conflict_subtype(t, strict=True):
    """矛盾子类校验（仅 conflict 时使用）"""
    t = (t or "").strip().lower()
    if not t:
        return True  # 空 = 非矛盾，合法
    if t in CONFLICT_SUBTYPE_SET:
        return True
    if strict:
        raise ValueError("非法矛盾子类: %r —— 合法: %s" % (t, ",".join(CONFLICT_SUBTYPES)))
    return False


# ── 语义关系枚举（P1 relation_extract 唯一权威源——仿 siku_conflict 模式，R13.18）──
#  维护者：关系/三元组/事件抽取的关系类型受控枚举统一引本模块，
# relation_extract.py / 后续消费端禁自行硬编码关系清单（防漂移）。
# 伪边防护：supports（同 concern 共现伪信号）/ same_type（同类组合爆炸）类伪边
# 绝不在枚举内——S0 盘点实锤 13,253,066 边中 95.27% 为伪边（已删
# 14,386,125 条先例），图通道检索白名单仅 keyword_overlap/semantic_similar/same_agent。
# 本枚举=抽取类语义关系（显式文本证据），与图通道统计共现边（graph_builder 5 规则）
# 分层不混；写入 graph_edges 前必须过 validate_edge_relation（伪边拒绝）。
# 语义关系（10 类，relation_extract 产出——显式文本触发，杜绝共现/同类伪信号）：
#   proposes     提出/建议（X 提出 Y / 建议做 Y）
#   decides      拍板/决定（拍板 四层落实方案）
#   implements   实现/落地（X 实现了 Y / 落地 Y）
#   develops     开发/负责（X 开发了 Y / 负责 Y）
#   depends_on   依赖（X 依赖 Y / 基于 Y）
#   uses         使用（X 使用 Y / 采用 Y）
#   part_of      组成/隶属（X 属于 Y / X 是 Y 的一部分）
#   conflicts    冲突/矛盾（X 与 Y 冲突——对齐 siku_conflict 矛盾语义）
#   related_to   语义相关兜底（LLM 判定相关但无精确关系）
#   event_occurred 事件发生（事件抽取产出——"X 发生了/于 T 发生"类事件）
RELATION_TYPES = [
    "proposes", "decides", "implements", "develops", "depends_on",
    "uses", "part_of", "conflicts", "related_to", "event_occurred",
]

# 伪边（拒绝写入 graph_edges——S0 已删 1438 万条先例，R2 图谱清理纪律）
PSEUDO_EDGE_RELATIONS = ["supports", "same_type"]

RELATION_TYPE_SET = frozenset(RELATION_TYPES)
PSEUDO_EDGE_SET = frozenset(PSEUDO_EDGE_RELATIONS)


def validate_relation_type(t, strict=True):
    """语义关系枚举校验：非法关系 → 拒（严格模式抛 ValueError）。

    - strict=True（管道默认）：非法抛 ValueError
    - strict=False（只读巡检/兼容）：非法返回 False 不抛
    返回: True=合法 / False=非法（strict=False 时）
    """
    t = (t or "").strip().lower()
    if t in RELATION_TYPE_SET:
        return True
    if strict:
        raise ValueError(
            "非法语义关系: %r —— 合法枚举共 %d 类: %s（伪边 supports/same_type 一律拒绝）"
            % (t, len(RELATION_TYPES), ",".join(RELATION_TYPES))
        )
    return False


def validate_edge_relation(t, strict=True):
    """graph_edges 写边前置校验：非枚举关系 + 伪边（supports/same_type）→ 拒。

    relation_extract 写边前必须调用本函数（伪边零新增铁律）。
    """
    t = (t or "").strip().lower()
    if t in PSEUDO_EDGE_SET:
        if strict:
            raise ValueError(
                "伪边拒绝写入 graph_edges: %r —— supports/same_type 类伪边 "
                "（S0 已删 14,386,125 条先例，R2 图谱清理纪律）" % t
            )
        return False
    return validate_relation_type(t, strict=strict)


if __name__ == "__main__":
    # 自检：21 项声明全部合法；假类型拒绝
    assert len(VALID_TYPES) == len(set(VALID_TYPES)), "类型清单重复"
    assert set(CONTENT_TYPES) == {
        "lesson", "fact", "decision", "result", "insight", "principle",
        "info", "instruction", "correction", "record", "idea", "estimate", "rule",
    }, "13 内容类型与拍板清单不一致"
    assert set(ASSET_TYPES) == {
        "spec", "skill", "cron", "workflow", "rule", "benchmark", "asset", "monitor",
        "design", "research", "script", "config", "reference",
    }, "13 资产类型与拍板清单不一致"
    for t in VALID_TYPES:
        assert validate_type(t), t
    try:
        validate_type("fake_type")
        raise AssertionError("假类型未被拒")
    except ValueError:
        pass
    assert not validate_type("fake_type", strict=False)
    assert is_asset_type("spec") and is_asset_type("monitor")
    assert is_asset_type("design") and is_asset_type("reference")
    assert not is_asset_type("fact")
    assert is_content_type("fact") and is_content_type("rule")
    # : 冲突三分类枚举自检（同源唯一权威源）
    assert set(CONFLICT_TYPES) == {"independent", "extension", "conflict"}, "冲突三分类与拍板清单不一致"
    assert set(CONFLICT_SUBTYPES) == {
        "conflict_summary_conclusion", "conflict_event_fact", "conflict_rule_semantic",
    }, "矛盾三子类与拍板清单不一致"
    for ct in CONFLICT_TYPES:
        assert validate_conflict_type(ct), ct
    for st in CONFLICT_SUBTYPES:
        assert validate_conflict_subtype(st), st
    try:
        validate_conflict_type("fake_conflict")
        raise AssertionError("假冲突分类未被拒")
    except ValueError:
        pass
    assert not validate_conflict_type("fake_conflict", strict=False)
    # : 语义关系枚举自检（同源唯一权威源 + 伪边防护）
    assert set(RELATION_TYPES) == {
        "proposes", "decides", "implements", "develops", "depends_on",
        "uses", "part_of", "conflicts", "related_to", "event_occurred",
    }, "语义关系枚举与拍板清单不一致"
    for rt in RELATION_TYPES:
        assert validate_relation_type(rt), rt
    try:
        validate_relation_type("fake_rel")
        raise AssertionError("假关系未被拒")
    except ValueError:
        pass
    assert not validate_relation_type("fake_rel", strict=False)
    # 伪边防护：supports/same_type 必须被 validate_edge_relation 拒绝
    for pe in PSEUDO_EDGE_RELATIONS:
        assert not validate_edge_relation(pe, strict=False), "伪边 %r 未被拒" % pe
        try:
            validate_edge_relation(pe)
            raise AssertionError("伪边 %r 严格模式未抛" % pe)
        except ValueError:
            pass
    assert validate_edge_relation("decides")
    print("siku_types 自检 PASS: %d 合法类型（13 内容+13 资产，rule 交集）; 假类型拒绝 OK; 冲突三分类+3 子类枚举 OK; %d 语义关系枚举 OK + 伪边(%s)拒绝 OK"
          % (len(VALID_TYPES), len(RELATION_TYPES), ",".join(PSEUDO_EDGE_RELATIONS)))
