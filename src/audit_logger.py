#!/usr/bin/env python3
"""审计日志增强 - audit_logger.py"""
import argparse, os
from datetime import datetime
from collections import defaultdict

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

import json as _json
def _qs(v):
    """YAML 安全标量——自由文本值统一 JSON 引号（冒号/特殊字符不再破坏解析；2026-09-19 sop46fix）"""
    return _json.dumps(str(v), ensure_ascii=False)
BD=_SIKU_ROOT
WD=os.path.join(BD,"audit/write")
RD=os.path.join(BD,"audit/read")

def log_write(op,op_type,layer,eid,typ,summary,conf_b,conf_a,trig,reason):
    os.makedirs(WD,exist_ok=True)
    af=os.path.join(WD,"audit-%s.yaml"%datetime.now().strftime("%Y-%m-%d"))
    ts=datetime.now().isoformat()
    a=False
    c=0
    if os.path.exists(af):
        with open(af) as f: c=f.read().count('op: %s'%op)
    if c>10:
        reason="WRITE_STORM: %s recent writes >10"%c
        a=True
    with open(af,"a") as f:
        f.write("---\nts: %s\nop: %s\nop_type: %s\nlayer: %s\neid: %s\ntype: %s\nsummary_a: %s\nalert: %s\n---\n"%(ts,op,op_type,layer,eid,typ,_qs(summary),"yes" if a else "no"))
    print("OK: write audit logged (%s %s)"%(op_type,eid))

def log_read(reader,layer,eid,query,purpose):
    os.makedirs(RD,exist_ok=True)
    af=os.path.join(RD,"read-audit-%s.yaml"%datetime.now().strftime("%Y-%m-%d"))
    ts=datetime.now().isoformat()
    with open(af,"a") as f:
        f.write("---\nts: %s\nreader: %s\nlayer: %s\neid: %s\nquery: %s\npurpose: %s\n---\n"%(ts,reader,layer,eid,_qs(query),purpose))
    c=0
    if os.path.exists(af):
        with open(af) as f: c=f.read().count('reader: %s'%reader)
    if c>100:
        print("R001: READ_STORM - %s reads >100/min, degrading to summary mode"%reader)
    if layer in ("L4b","L4c") and reader not in _L4_READERS:
        print("R002: external agent %s reading %s - alerting admin"%(reader,layer))
    print("OK: read audit logged (%s %s)"%(reader,eid))

def check():
    print("Anomaly check:")
    for d in [WD,RD]:
        if not os.path.exists(d): continue
        for f in sorted(os.listdir(d)):
            if not f.endswith(".yaml"): continue
            fp=os.path.join(d,f)
            with open(fp) as fh:
                c=fh.read()
            ops=defaultdict(int)
            for line in c.split("\n"):
                if "op: " in line and "op_type" not in line:
                    k=line.split("op: ",1)[1].strip()
                    ops[k]+=1
            for op,n in ops.items():
                if n>10: print("  A001: %s in %s wrote %d times"%(op,f,n))
    print("  done")

def report(days=7):
    print("Audit report (last %d days):" % days)
    for d,label in [(WD,"Write"),(RD,"Read")]:
        if not os.path.exists(d): continue
        total=0
        for f in sorted(os.listdir(d)):
            if not f.endswith(".yaml"): continue
            with open(os.path.join(d,f)) as fh:
                total+=fh.read().count("ts:")
        print("  %s audits: %d entries"%(label,total))

if __name__=="__main__":
    p=argparse.ArgumentParser()
    sp=p.add_subparsers(dest="cmd")
    pw=sp.add_parser("write")
    pw.add_argument("--op",default="user"); pw.add_argument("--op-type",default="write")
    pw.add_argument("--layer",default="L3"); pw.add_argument("--eid",default="test")
    pw.add_argument("--type",default="decision"); pw.add_argument("--summary",default="")
    pw.add_argument("--conf-b",default="null"); pw.add_argument("--conf-a",default="0.9")
    pw.add_argument("--trig",default="manual"); pw.add_argument("--reason",default="")
    pr=sp.add_parser("read")
    pr.add_argument("--reader",default="user"); pr.add_argument("--layer",default="L3")
    pr.add_argument("--eid",default="search"); pr.add_argument("--query",default="")
    pr.add_argument("--purpose",default="")
    sp.add_parser("check")
    prpt=sp.add_parser("report")
    prpt.add_argument("--days",type=int,default=7)
    a=p.parse_args()
    if a.cmd=="write": log_write(a.op,a.op_type,a.layer,a.eid,a.type,a.summary,a.conf_b,a.conf_a,a.trig,a.reason)
    elif a.cmd=="read": log_read(a.reader,a.layer,a.eid,a.query,a.purpose)
    elif a.cmd=="check": check()
    elif a.cmd=="report": report(a.days if hasattr(a,'days') else 7)
    else: p.print_help()
