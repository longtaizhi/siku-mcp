#!/usr/bin/env python3
"""权限检查工具 - check_permission.py"""
import argparse, os, yaml
from datetime import datetime

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

PF=os.path.join(_SIKU_ROOT, "permissions.yaml")
AD=os.path.join(_SIKU_ROOT, "audit/write")

# 身份别名映射（按需自定义；环境变量 SIKU_ALIAS_MAP_JSON 可覆盖）：英文调用名 -> permissions.yaml 名称
ALIAS_MAP = {}
try:
    import json as _json
    ALIAS_MAP = _json.loads(os.environ.get("SIKU_ALIAS_MAP_JSON", "{}") or "{}")
except Exception:
    ALIAS_MAP = {}

def normalize_agent(agent):
    if not agent:
        return agent
    a = str(agent).strip()
    if a in ALIAS_MAP:
        return ALIAS_MAP[a]
    low = a.lower()
    if low in ALIAS_MAP:
        return ALIAS_MAP[low]
    return a

def audit(agent,action,layer,message,alert=False):
    af=os.path.join(AD,"audit-%s.yaml"%datetime.now().strftime("%Y-%m-%d"))
    ts=datetime.now().isoformat()
    al="true" if alert else "false"
    with open(af,"a") as f:
        f.write("---\nts: %s\nop: %s\nop_type: perm_%s\nlayer: %s\nmsg: %s\nalert: %s\n---\n"%(ts,agent,action,layer,message,al))

def check(agent,action,layer,mode="enforce"):
    """mode: enforce=按权限拦截（默认）| audit=只记录不拦截（P0-2 先行1周）"""
    agent = normalize_agent(agent)  # P1-3 别名归一化
    with open(PF) as f:
        perm=yaml.safe_load(f)
    level="D"
    ldata=perm["levels"]["D"]
    for lname,ld in perm["levels"].items():
        if agent in ld["agents"]:
            level=lname; ldata=ld; break
    print("Agent: %s | Level: %s | Action: %s | Layer: %s"%(agent,level,action,layer))
    if action=="write":
        ok=ldata.get("write_access",False)
        if ok: print("OK: write allowed")
        else: print("BLOCKED: %s cannot write %s"%(agent,layer))
    else:
        ok=ldata.get("read_access",True)
        if ok: print("OK: read allowed")
        else: print("BLOCKED: %s cannot read %s"%(agent,layer))
    if mode == "audit":
        audit(agent,action,layer,"audit_observe:%s" % ("allowed" if ok else "would_block"), not ok)
        return True
    audit(agent,action,layer,"allowed" if ok else "blocked",not ok)
    return ok

def list_agents():
    with open(PF) as f:
        perm=yaml.safe_load(f)
    print("%-5s %-10s %-8s %-8s" % ("Lvl","Agent","Write","Read"))
    print("-"*35)
    for lname,ld in perm["levels"].items():
        for ag in ld["agents"]:
            w="OK" if ld.get("write_access") else "NO"
            r="OK" if ld.get("read_access") else "NO"
            print("%-5s %-10s %-8s %-8s"%(lname,ag,w,r))
    print("\nDefault: %s" % perm.get("default_level","D"))

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--agent"); p.add_argument("--action",choices=["read","write"])
    p.add_argument("--layer",default="L3"); p.add_argument("--list",action="store_true")
    p.add_argument("--mode",choices=["audit","enforce"],default="enforce")
    a=p.parse_args()
    if a.list: list_agents()
    elif a.agent and a.action: check(a.agent,a.action,a.layer,mode=a.mode)
    else: p.print_help()
