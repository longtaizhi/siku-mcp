#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
siku_concept_meta.py — 四库概念/本体只读元数据查询（P3）

支撑 MCP 扩展两个工具（siku_mcp_server.py 懒加载调用——零影响主链路）：
  concept_query    — 概念查询：种子概念集（C5 权威源）+ 语义层 jsonld
                     概念映射（broader/narrower 一层——子类/同义）
  ontology_explore — 本体探索：枚举（siku_types.py 唯一权威源）+ shapes
                     （shapes.yaml）+ 概念集统计

纪律：纯只读（零写库零写文件）；枚举引用 siku_types.py 不复制（R13.18 防漂移）；
shapes 复用 shacl_validate.load_shapes() 不复制。任何 import 失败→返回明确错误，
不阻塞 MCP 主链路。
"""

import os
import json

_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）
_CONCEPTS_DIR = os.environ.get("SIKU_CONCEPTS_DIR", os.path.join(_HERMES_HOME, "memory-bank", "concepts"))  # 概念集目录

# 权威源路径（与 l3_retrieval.py 同源常量保持一致）
SEED_CONCEPTS_PATH = os.path.join(_CONCEPTS_DIR, "种子概念集.json")      # C5 概念集权威源
JSONLD_PATH = os.environ.get("SIKU_LAYER_JSONLD", os.path.join(_CONCEPTS_DIR, "domain-graph.jsonld"))    # P1 语义层概念关系权威源
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))

# ── 缓存（模块级；只读文件内容缓存，mtime 变更自动失效）───────────────
_SEED_CACHE = None     # (mtime, {prefLabel: dict, altIndex: {alt: prefLabel}})
_JSONLD_CACHE = None   # (mtime, {prefLabel: dict})

# ── 种子概念集加载 ───────────────────────────────────────────────────
def load_seed_concepts(force=False):
    """读取种子概念集.json（C5 权威源）→ {prefLabel: {id,prefLabel,altLabels,context,group}}。

    返回 {"ok": bool, "concepts": {...}, "count": int, "error": str?}"""
    global _SEED_CACHE
    try:
        mtime = os.path.getmtime(SEED_CONCEPTS_PATH)
        if _SEED_CACHE and _SEED_CACHE[0] == mtime and not force:
            return _SEED_CACHE[1]
        with open(SEED_CONCEPTS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        by_pref, alt_index = {}, {}
        for c in data.get("concepts", []):
            pref = c.get("prefLabel", "")
            if not pref:
                continue
            rec = {
                "id": c.get("id", ""),
                "prefLabel": pref,
                "altLabels": c.get("altLabels", []) or [],
                "context": c.get("context", "") or "",
                "group": c.get("group", "") or c.get("groupLabel", "") or "",
            }
            by_pref[pref] = rec
            for alt in rec["altLabels"]:
                alt_index.setdefault(str(alt), pref)
        result = {"ok": True, "concepts": by_pref, "altIndex": alt_index,
                  "count": len(by_pref), "path": SEED_CONCEPTS_PATH}
        _SEED_CACHE = (mtime, result)
        return result
    except Exception as e:
        return {"ok": False, "error": "种子概念集读取失败: %s" % e,
                "concepts": {}, "altIndex": {}, "count": 0, "path": SEED_CONCEPTS_PATH}

# ── 语义层 jsonld 加载 ───────────────────────────────────────────
def _j_pref(node):
    """jsonld 节点 prefLabel（支持 str 或 [str,...]）。"""
    p = node.get("prefLabel")
    if isinstance(p, list):
        return p[0] if p else ""
    return p or ""


def load_jsonld(force=False):
    """读取domain-graph.jsonld（P1 语义层概念关系权威源）→ {prefLabel: 档案}。

    档案含 altLabels/broader/narrower/definition（broader/narrower 解析为
    prefLabel 名列表）。返回 {"ok", "concepts", "count", "error"}"""
    global _JSONLD_CACHE
    try:
        mtime = os.path.getmtime(JSONLD_PATH)
        if _JSONLD_CACHE and _JSONLD_CACHE[0] == mtime and not force:
            return _JSONLD_CACHE[1]
        with open(JSONLD_PATH, encoding="utf-8") as f:
            data = json.load(f)
        graph = data.get("@graph", [])
        raw = {}
        for node in graph:
            pref = _j_pref(node)
            if not pref:
                continue
            raw[pref] = node
        by_pref = {}
        # @id → prefLabel 反查表（broader/narrower 指向 @id）
        id2pref = {node.get("@id"): pref for pref, node in raw.items()}

        def _names(ids):
            out = []
            for i in (ids or []):
                if isinstance(i, dict):      # jsonld 形态 [{"@id": ...}]
                    i = i.get("@id", "")
                n = i.split("/")[-1] if isinstance(i, str) else ""
                # @id 形如 .../concept_xxx → 映射回 prefLabel
                out.append(id2pref.get(i, raw.get(n, {}).get("prefLabel", n)))
            return out

        for pref, node in raw.items():
            by_pref[pref] = {
                "id": node.get("@id", ""),
                "prefLabel": pref,
                "altLabels": node.get("altLabel", []) or [],
                "definition": node.get("definition", "") or "",
                "broader": _names(node.get("broader")),
                "narrower": _names(node.get("narrower")),
                "inScheme": bool(node.get("inScheme")),
            }
        result = {"ok": True, "concepts": by_pref, "count": len(by_pref),
                  "path": JSONLD_PATH}
        _JSONLD_CACHE = (mtime, result)
        return result
    except Exception as e:
        return {"ok": False, "error": "语义层 jsonld 读取失败: %s" % e,
                "concepts": {}, "count": 0, "path": JSONLD_PATH}

# ── concept_query：概念查询 ──────────────────────────────────────────
def query_concept(concept, top_k=5):
    """概念查询：种子概念集 + jsonld 概念映射命中。

    命中策略（由精到宽）：prefLabel 精确 → altLabel 精确 → prefLabel 包含
    → altLabel 包含。返回概念档案（prefLabel/altLabels/context/定义）+ 一层
    本体关系（broader 父类/narrower 子类——子类/同义映射，供 concept 通道
    与推理引擎 R2/R3 规则接线）。纯只读。
    """
    seed = load_seed_concepts()
    jl = load_jsonld()
    if not seed.get("ok") and not jl.get("ok"):
        return {"ok": False,
                "error": seed.get("error", "") + "；" + jl.get("error", "")}

    q = str(concept).strip()
    ql = q.lower()
    hits = []  # (score, source, prefLabel)

    def _score(pref, alts):
        if pref == q:
            return 0
        if q in (alts or []):
            return 1
        if ql in pref.lower():
            return 2
        for a in (alts or []):
            if ql in str(a).lower():
                return 3
        return None

    for src, data in (("seed", seed), ("jsonld", jl)):
        if not data.get("ok"):
            continue
        for pref, rec in data.get("concepts", {}).items():
            alts = rec.get("altLabels", []) or []
            sc = _score(pref, alts)
            if sc is not None:
                hits.append((sc, src, pref))
    hits.sort(key=lambda h: (h[0], h[1]))
    hits = hits[:max(1, int(top_k))]

    results = []
    for sc, src, pref in hits:
        if src == "seed":
            rec = seed["concepts"][pref]
            results.append({
                "source": "seed",
                "id": rec["id"],
                "prefLabel": rec["prefLabel"],
                "altLabels": rec["altLabels"],
                "context": rec["context"],
                "group": rec["group"],
            })
        else:
            rec = jl["concepts"][pref]
            results.append({
                "source": "jsonld",
                "id": rec["id"],
                "prefLabel": rec["prefLabel"],
                "altLabels": rec["altLabels"],
                "definition": rec["definition"],
                "broader": rec["broader"],      # 父类（子类映射方向之一）
                "narrower": rec["narrower"],    # 子类（R2 展开方向）
            })
    return {"ok": True, "query": q, "hits": len(results),
            "results": results, "meta": {"top_k": int(top_k)}}

# ── ontology_explore：本体探索 ───────────────────────────────────────
def _load_types_module():
    """siku_types.py 枚举（唯一权威源——引用不复制）。lazy import 防顶层副作用。"""
    import importlib.util
    spec = importlib.util.spec_from_file_location("siku_types", os.path.join(SCRIPTS_DIR, "siku_types.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_shapes():
    """shapes.yaml 形状清单（复用 shacl_validate.load_shapes 不复制）。"""
    try:
        import shacl_validate
        shapes = shacl_validate.load_shapes()
        return shapes, None
    except Exception as e:
        return None, "shapes 加载失败: %s" % e


def explore_enums():
    """枚举探索：siku_types.py 全部枚举（25 类型/关系/冲突分类）。"""
    try:
        m = _load_types_module()
        return {"ok": True,
                "content_types": m.CONTENT_TYPES,
                "asset_types": m.ASSET_TYPES,
                "valid_types": m.VALID_TYPES,
                "relation_types": m.RELATION_TYPES,
                "pseudo_edge_relations": m.PSEUDO_EDGE_RELATIONS,
                "conflict_types": m.CONFLICT_TYPES,
                "conflict_subtypes": m.CONFLICT_SUBTYPES,
                "meta": {"authority": "siku_types.py（唯一权威源——引用不复制）"}}
    except Exception as e:
        return {"ok": False, "error": "枚举读取失败: %s" % e}


def explore_shapes():
    """shapes 探索：shapes.yaml 非枚举硬约束清单（error/warning 分级）。"""
    shapes, err = _load_shapes()
    if err:
        return {"ok": False, "error": err}
    return {"ok": True, "shapes": shapes, "n_shapes": len(shapes),
            "meta": {"authority": "shapes.yaml（枚举引用 siku_types.py）",
                     "path": os.path.join(SCRIPTS_DIR, "shapes.yaml")}}


def explore_concepts():
    """概念集探索：种子概念集 + jsonld 统计（总数/分组/顶层概览）。"""
    seed = load_seed_concepts()
    jl = load_jsonld()
    out = {"ok": True}
    if seed.get("ok"):
        groups = {}
        for rec in seed["concepts"].values():
            g = rec.get("group") or "未分组"
            groups[g] = groups.get(g, 0) + 1
        out["seed"] = {"count": seed["count"], "groups": groups,
                       "path": seed.get("path")}
    else:
        out["seed"] = {"error": seed.get("error")}
    if jl.get("ok"):
        with_rel = sum(1 for r in jl["concepts"].values()
                       if r["broader"] or r["narrower"])
        out["jsonld"] = {"count": jl["count"], "with_relations": with_rel,
                         "path": jl.get("path")}
    else:
        out["jsonld"] = {"error": jl.get("error")}
    return out


def explore(scope="all"):
    """ontology_explore 统一入口：scope ∈ enums/shapes/concepts/all。只读。"""
    scope = (scope or "all").strip().lower()
    out = {"ok": True, "scope": scope}
    if scope in ("enums", "all"):
        out["enums"] = explore_enums()
    if scope in ("shapes", "all"):
        out["shapes"] = explore_shapes()
    if scope in ("concepts", "all"):
        out["concepts"] = explore_concepts()
    if scope not in ("enums", "shapes", "concepts", "all"):
        return {"ok": False, "error": "非法 scope %r（可选: enums/shapes/concepts/all）" % scope}
    return out


if __name__ == "__main__":
    import sys
    q = sys.argv[1] if len(sys.argv) > 1 else "门禁审核"
    print(json.dumps(query_concept(q), ensure_ascii=False, indent=2)[:2000])
