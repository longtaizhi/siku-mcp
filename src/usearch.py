#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""usearch — 统一检索 CLI（三源聚合：四库 siku / 会话 session / 文件 files）

设计来源: 39-统一检索CLI设计-20260818.md ()
只读聚合: 仅三类只读子进程（l3_retrieval search / session_search wrapper / grep-find），
脚本自身零写库零写文件（日志仅 stderr，--json 输出仅 stdout）。

用法:
  usearch "QUERY" [-k N] [-s siku,session,files] [-d DIR] [-f GLOB]
                  [--merge] [--json] [--timeout N] [--no-color] [-v]
退出码: 0=至少一源成功; 2=全源失败或参数错误; 3=空查询或运行时异常
"""
import argparse
import concurrent.futures as cf
import functools
import json
import os
import subprocess
import sys
import time

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）

VERSION = "1.0"
RRF_K = 60                      # 与四库一致
_VENV_PY = os.environ.get("SIKU_VENV_PYTHON") or os.path.join(_HERMES_HOME, "hermes-agent/venv/bin/python")
_SIKU_PY = os.path.join(_SIKU_ROOT, "scripts/l3_retrieval.py")
_SS_WRAP = os.path.join(_HERMES_HOME, "scripts/usearch_ss_wrap.py")
DEFAULT_DIRS = [os.path.expanduser("~/Desktop"), _SIKU_ROOT, os.path.expanduser("~/Documents")]
GREP_EXCLUDES = ["--exclude=*.db", "--exclude=*.db-*", "--exclude=*.sqlite",
                 "--exclude=*.sqlite3", "--exclude=*.bak*", "--exclude=*.pyc"]
MAX_SUMMARY = 50                # 摘要截断字数
MAX_GREP_CANDIDATES = 10000     # 内容搜索候选上限（按 mtime 最新 N 个；文件名搜索不受限）


# ── venv 自举（同 l3_retrieval execv 模式；venv 缺失 fail-open）──────────
if sys.version_info < (3, 11) and os.path.exists(_VENV_PY):
    os.execv(_VENV_PY, [_VENV_PY] + sys.argv)


def run_sub(cmd, timeout):
    """子进程只读调用；返回 (rc, stdout, stderr)。超时抛 TimeoutExpired。"""
    proc = subprocess.run(cmd, capture_output=True, text=True,
                          timeout=timeout, errors="replace")
    return proc.returncode, proc.stdout, proc.stderr


def parse_json_stdout(stdout):
    """l3_retrieval stdout 首行可能带 audit 前缀（如 'OK: read audit logged ...'）——
    从第一个 '{' 行起截取完整 JSON 再 loads（l3-retrieval-baseline-methodology 实测坑）。"""
    lines = stdout.splitlines()
    for i, line in enumerate(lines):
        if line.lstrip().startswith("{"):
            return json.loads("\n".join(lines[i:]))
    raise ValueError("stdout 无 JSON")


# ── 三源实现 ──────────────────────────────────────────────────────────
def src_siku(query, limit, timeout, _verbose):
    cmd = [_VENV_PY, _SIKU_PY, "search", "-q", query, "-n", str(limit), "--pretty"]
    rc, out, err = run_sub(cmd, timeout)
    if rc != 0:
        raise RuntimeError(f"l3_retrieval 退出码 {rc}: {err.strip()[:200]}")
    data = parse_json_stdout(out)
    results = []
    for r in data.get("results", []):
        results.append({"id": r.get("id", ""), "type": r.get("type", ""),
                        "summary": r.get("summary", ""), "score": r.get("score", 0),
                        "source_agent": r.get("source_agent", "")})
    return results


def src_session(query, limit, timeout, _verbose):
    cmd = [_VENV_PY, _SS_WRAP, query, str(limit)]
    rc, out, err = run_sub(cmd, timeout)
    if rc != 0:
        raise RuntimeError(f"session wrapper 退出码 {rc}: {err.strip()[:200]}")
    data = json.loads(out)
    if not data.get("success"):
        raise RuntimeError(f"session_search 返回失败: {str(data)[:200]}")
    results = []
    for r in data.get("results", []):
        results.append({"session_id": r.get("session_id", ""),
                        "when": r.get("when", ""),
                        "title": r.get("title") or "",
                        "snippet": r.get("snippet", "")[:200]})
    return results


def _find_prunes():
    """剪枝重型/非文档子树（backup/logs/snapshots/.bak/.git/node_modules/__pycache__），
    避免默认目录（含 39G 四库全书）内容搜索必然超时。"""
    return ["(", "-path", "*/.git", "-o", "-path", "*/node_modules",
            "-o", "-path", "*/backup", "-o", "-path", "*/__pycache__",
            "-o", "-path", "*/logs", "-o", "-path", "*/snapshots",
            "-o", "-path", "*/.bak-*", ")", "-prune", "-o"]


def src_files(query, limit, timeout, dirs, file_glob, _verbose):
    dirs = [d for d in dirs if os.path.isdir(d)]
    if not dirs:
        return []
    prunes = _find_prunes()
    paths = set()
    # 内容匹配：find 枚举候选（剪枝）→ xargs grep -l -I -i（候选多时避免 ARG_MAX）
    fargs = ["find"] + dirs + prunes + ["-type", "f"]
    if file_glob:
        fargs += ["-name", file_glob]
    rc, out, err = run_sub(fargs + ["-print0"], timeout)
    if rc != 0:
        raise RuntimeError(f"find 退出码 {rc}: {err.strip()[:200]}")
    if out.strip():
        cands = out.split("\0")
        if len(cands) > MAX_GREP_CANDIDATES:
            # 默认目录含巨型模型/数据集——内容搜索只取 mtime 最新 N 个候选
            # （与输出排序语义一致：mtime 新→旧；超界用 -d 收窄目录或 --timeout 放宽）
            def _mt(p):
                try:
                    return os.stat(p).st_mtime
                except OSError:
                    return 0.0
            cands = sorted(cands, key=_mt, reverse=True)[:MAX_GREP_CANDIDATES]
        try:
            proc = subprocess.run(["xargs", "-0", "grep", "-l", "-I", "-i", "--", query],
                                  input="\0".join(cands).encode("utf-8", "replace"),
                                  capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise
        out2 = proc.stdout.decode("utf-8", "replace")
        err2 = proc.stderr.decode("utf-8", "replace")
        if proc.returncode in (0, 1, 123):  # 1/123=xargs 下 grep 无匹配（macOS xargs 无匹配时返回 1）或瞬态 ENOENT
            paths.update(p for p in out2.splitlines() if p.strip())
        else:
            raise RuntimeError(f"grep 退出码 {proc.returncode}: {err2.strip()[:200]}")
    # 文件名匹配：find -name "*QUERY*"（⚠️ 必须显式 -print：BSD find 表达式结尾是测试时
    # 隐式 -print 会打印被 -prune 的目录本身（实测 .bak-*/__pycache__/logs 目录混入结果））
    fargs2 = ["find"] + dirs + prunes + ["-type", "f", "-name", f"*{query}*", "-print"]
    rc, out3, err3 = run_sub(fargs2, timeout)
    if rc == 0:
        paths.update(p for p in out3.splitlines() if p.strip())
    elif rc != 1:
        raise RuntimeError(f"find 退出码 {rc}: {err3.strip()[:200]}")
    # 按 mtime 新→旧排序，取 top K
    hits = []
    for p in paths:
        try:
            hits.append((os.stat(p).st_mtime, p))
        except OSError:
            continue
    hits.sort(reverse=True)
    return [{"path": p, "matched": True} for _, p in hits[:limit]]


SOURCES = {"siku": src_siku, "session": src_session, "files": src_files}


def collect(query, limit, timeout, sources, dirs, file_glob, verbose):
    """并行运行各源，单源失败/超时仅记录，不影响其他源。"""
    results, errors = {}, {}

    def run_one(name):
        if name == "files":
            def fn(q, l, t, v):  # 闭包适配：files 源额外绑定目录/glob，对外签名同其它源
                return src_files(q, l, t, dirs, file_glob, v)
        else:
            fn = SOURCES[name]
        t0 = time.time()
        try:
            rs = fn(query, limit, timeout, verbose)
            return name, rs, time.time() - t0, None
        except subprocess.TimeoutExpired:
            return name, [], time.time() - t0, f"超时（>{timeout}s）"
        except Exception as e:  # noqa: BLE001
            return name, [], time.time() - t0, str(e)[:200]

    with cf.ThreadPoolExecutor(max_workers=len(sources)) as ex:
        for name, rs, el, err in ex.map(run_one, sources):
            results[name] = {"ok": err is None, "results": rs,
                             "elapsed_s": round(el, 2),
                             "error": err or ""}
            if err:
                errors[name] = err
    return results, errors


# ── RRF 合并 ──────────────────────────────────────────────────────────
def rrf_merge(results, limit):
    cands = []
    for name, info in results.items():
        if not info["ok"]:
            continue
        for rank, item in enumerate(info["results"], start=1):
            cands.append((name, rank, 1.0 / (RRF_K + rank + 1), item))
    cands.sort(key=lambda c: (-c[2], c[0]))
    merged = []
    for name, rank, score, item in cands[:limit]:
        row = {"source": name, "rank": rank, "rrf_score": round(score, 4)}
        row.update(item)
        merged.append(row)
    return merged


# ── 输出 ──────────────────────────────────────────────────────────────
# ── 分层跳层提示（搜索分层方案 v1.3 层B——核心兜底；正常路径零开销）───────
LAYER_QUICK = (
    "【分层速查】文件实体(文档名/路径/内容)→L1 search_files/find；历史经验→L2 "
    "l3_retrieval.py；历史会话→L3 session_search；外部信息(最新/公司/新闻)→L4 "
    "web_search；不确定/混合→L0 usearch 聚合。0 命中先换层/换查询词。"
)


def build_hints(results, layer_hint=""):
    """依各源命中情况生成跳层提示列表。

    规则（方案 v1.3 层B）：
      1. siku 0 命中 + files 有命中 → 提示文件层命中前 3 条（L1 换层）
      2. 启用源全部 0 命中 → 提示换查询词（文件名变体/语义词）+ web 出口
      3. 调用方 --layer-hint 显式传预期层 → 追加预期层提示
    仅 0 命中分支产出提示——正常路径零开销（性能红线）。
    """
    hints = []
    ok_names = [n for n, i in results.items() if i.get("ok")]
    siku = results.get("siku", {})
    files = results.get("files", {})
    if siku.get("ok") and not siku.get("results") \
            and files.get("ok") and files.get("results"):
        top = [r["path"] for r in files["results"][:3] if isinstance(r, dict)]
        lines = "\n".join("  %d) %s" % (i + 1, p) for i, p in enumerate(top))
        hints.append(
            "📚 四库语义检索 0 命中，但 📁 本地文件层命中 %d 条——若目标是文件实体"
            "（文档/路径），请直接取下方文件层结果，或换文件名变体查询词。文件层前 %d 条：\n%s"
            % (len(files["results"]), min(3, len(top)), lines))
    all_zero = bool(ok_names) and all(not results[n]["results"] for n in ok_names)
    if all_zero:
        hints.append(
            "🔁 全部启用源 0 命中——建议换查询词（文件名变体/同义语义词）；"
            "若实为上网需求（查最新/公司/新闻）→ 用 web_search。\n" + LAYER_QUICK)
    if layer_hint:
        hints.append(
            "🎯 调用方预期层：%s —— 请优先从该层命中取结果；未命中按上方分层换层/换词。"
            % layer_hint)
    return hints


def _trunc(s, n=MAX_SUMMARY):
    s = (s or "").replace("\n", " ").strip()
    return s if len(s) <= n else s[:n] + "…"


def fmt_grouped(results, errors, query, limit, elapsed, verbose, color, hints=None):
    C = (lambda c, s: s)
    if color:
        def C(c, s): return f"\033[{c}m{s}\033[0m"
    out = [f'🔍 统一检索 usearch v{_trunc(VERSION, 6)} | "{query}" | 三源 × {limit} | 只读聚合', ""]
    if hints:
        out.append(C("33", "── 分层提示（跳层兜底）") + "─" * 12)
        out.extend(hints)
        out.append("")
    order = [("siku", "📚 四库记忆"), ("session", "💬 历史会话"), ("files", "📁 本地文件")]
    for name, label in order:
        info = results.get(name)
        if info is None:
            continue
        if not info["ok"]:
            out.append(C("31", f"── {label} ── ⚠️ 源失败: {_trunc(info['error'], 60)}（不影响其他源）"))
            out.append("")
            continue
        rs = info["results"]
        out.append(C("36", f"── {label}（{len(rs)} 命中）") + "─" * 20)
        if not rs:
            out.append("（0 命中）")
        for i, r in enumerate(rs, 1):
            if name == "siku":
                line = f"[{i}] {_trunc(r['type'])} | score {r['score']:.4f} | {_trunc(r['source_agent'])} | {_trunc(r['summary'])}"
                out.append(line)
                if verbose:
                    out.append(f"    id: {r['id']}")
            elif name == "session":
                out.append(f"[{i}] {r['session_id']} | {_trunc(r['when'], 24)} | 摘要: {_trunc(r['snippet'], 40)}")
                if verbose:
                    out.append(f"    title: {_trunc(r['title'], 40)}")
            else:
                out.append(f"[{i}] {r['path']}")
        out.append("")
    ok_n = sum(1 for i in results.values() if i["ok"])
    src_times = " / ".join(f"{k} {v['elapsed_s']}s" for k, v in results.items())
    out.append(C("33", "── 状态 ") + "─" * 20)
    out.append(f"{ok_n}/{len(results)} 源成功 | 总耗时 {elapsed:.1f}s | 各源: {src_times}")
    if errors:
        out.append("失败源: " + "; ".join(f"{k}={v}" for k, v in errors.items()))
    out.append("（只读聚合，无任何写入）")
    return "\n".join(out)


def fmt_merged(merged, results, errors, query, elapsed, color, hints=None):
    C = (lambda c, s: s)
    if color:
        def C(c, s): return f"\033[{c}m{s}\033[0m"
    badges = {"siku": "📚", "session": "💬", "files": "📁"}
    out = [f'🔍 统一检索 usearch v{_trunc(VERSION, 6)} | "{query}" | RRF 合并 Top-{len(merged)} | 只读聚合', ""]
    if hints:
        out.append(C("33", "── 分层提示（跳层兜底）") + "─" * 12)
        out.extend(hints)
        out.append("")
    if not merged:
        out.append("（合并 0 命中）")
    for i, m in enumerate(merged, 1):
        b = badges.get(m["source"], "•")
        if m["source"] == "siku":
            out.append(f"[{i}] {b} {m['rrf_score']:.4f} | {_trunc(m['type'])} | {_trunc(m['summary'])}  (siku rank {m['rank']})")
        elif m["source"] == "session":
            out.append(f"[{i}] {b} {m['rrf_score']:.4f} | {m['session_id']} | {_trunc(m['snippet'], 40)}")
        else:
            out.append(f"[{i}] {b} {m['rrf_score']:.4f} | {m['path']}")
    out.append("")
    ok_n = sum(1 for i in results.values() if i["ok"])
    src_times = " / ".join(f"{k} {v['elapsed_s']}s" for k, v in results.items())
    out.append(C("33", "── 状态 ") + "─" * 20)
    out.append(f"{ok_n}/{len(results)} 源成功 | 总耗时 {elapsed:.1f}s | 各源: {src_times}")
    if errors:
        out.append("失败源: " + "; ".join(f"{k}={v}" for k, v in errors.items()))
    out.append("（只读聚合，无任何写入）")
    return "\n".join(out)


def to_json(query, mode, results, merged, elapsed, hints=None):
    sources = {}
    for name, info in results.items():
        src = {"ok": info["ok"], "count": len(info["results"]),
               "elapsed_s": info["elapsed_s"]}
        if info["ok"]:
            src["results"] = info["results"]
        else:
            src["error"] = info["error"]
        sources[name] = src
    payload = {
        "version": VERSION, "query": query, "mode": mode,
        "sources": sources, "merge": merged,
        "status": {"ok_sources": sum(1 for i in results.values() if i["ok"]),
                   "failed": [n for n, i in results.items() if not i["ok"]]},
        "readonly": True,
    }
    # 向后兼容增量（复核意见⑦）：既有结构零变更，仅新增顶层 hints 字段
    # （0 命中时才非空；正常路径 hints=[] 与旧输出语义一致）
    if hints is not None:
        payload["hints"] = hints
    return json.dumps(payload, ensure_ascii=False, indent=2)


def main():
    ap = argparse.ArgumentParser(prog="usearch", description="统一检索 CLI：四库/会话/文件 三源只读聚合")
    ap.add_argument("query", help="检索词（必填）")
    ap.add_argument("-k", "--limit", type=int, default=5, help="每源返回条数（默认 5，范围 1-20）")
    ap.add_argument("-s", "--sources", default="siku,session,files",
                    help="启用源，逗号分隔：siku,session,files（默认全开）")
    ap.add_argument("-d", "--dir", action="append", default=None,
                    help="文件源搜索目录（可多次；默认 ~/Desktop,$SIKU_ROOT,~/Documents；$SIKU_ROOT 按环境变量解析，缺省 ~/siku-core）")
    ap.add_argument("-f", "--file-glob", default=None, help="文件源内容搜索只匹配该 glob（如 *.md）")
    ap.add_argument("--merge", action="store_true", help="RRF 合并排序模式（跨源合并 Top-N）")
    ap.add_argument("--json", action="store_true", help="结构化 JSON 输出（机器可读）")
    ap.add_argument("--layer-hint", default="", metavar="LAYER",
                    help="调用方显式预期检索层（如 L1文件/L2记忆/L3会话/L4外部/混合）——"
                         "输出附加该层提示（搜索分层方案 v1.3 层B）")
    ap.add_argument("--timeout", type=int, default=15, help="单源超时秒数（默认 15，范围 3-120）")
    ap.add_argument("--no-color", action="store_true", help="禁用 ANSI 颜色")
    ap.add_argument("-v", "--verbose", action="store_true", help="显示每源耗时/状态明细")
    args = ap.parse_args()

    query = args.query.strip()
    if not query:
        print("usearch: 错误：QUERY 为空（防误触全量 grep）", file=sys.stderr)
        sys.exit(3)
    if not (1 <= args.limit <= 20):
        print("usearch: 错误：--limit 范围 1-20", file=sys.stderr)
        sys.exit(2)
    if not (3 <= args.timeout <= 120):
        print("usearch: 错误：--timeout 范围 3-120", file=sys.stderr)
        sys.exit(2)
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    bad = [s for s in sources if s not in SOURCES]
    if bad or not sources:
        print(f"usearch: 错误：未知源 {bad or args.sources}（可选 siku,session,files）", file=sys.stderr)
        sys.exit(2)
    dirs = [os.path.expanduser(d) for d in (args.dir or DEFAULT_DIRS)]
    color = (not args.no_color) and sys.stdout.isatty()

    t0 = time.time()
    results, errors = collect(query, args.limit, args.timeout, sources, dirs,
                              args.file_glob, args.verbose)
    elapsed = time.time() - t0
    merged = rrf_merge(results, args.limit) if args.merge else None
    hints = build_hints(results, layer_hint=args.layer_hint)

    if args.json:
        print(to_json(query, "merged" if args.merge else "grouped",
                      results, merged, elapsed, hints))
    elif args.merge:
        print(fmt_merged(merged, results, errors, query, elapsed, color, hints))
    else:
        print(fmt_grouped(results, errors, query, args.limit, elapsed,
                          args.verbose, color, hints))

    ok_n = sum(1 for i in results.values() if i["ok"])
    sys.exit(0 if ok_n > 0 else 2)


if __name__ == "__main__":
    main()
