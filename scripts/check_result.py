import json
with open("output/phase4_candidates_20251231.json", encoding="utf-8") as f:
    d = json.load(f)
print(f"共 {len(d['candidates'])} 只候选")
print(f"{'代码':12s} {'名称':10s} {'score':>5} {'回撤':>7} {'熊市周':>6} {'VR':>6} {'close/MA120':>11} {'recency天':>9} {'pps':>4} {'quiet':>5}")
print("-"*90)
for c in d["candidates"]:
    print(f"{c['instrument']:12s} {c.get('stock_name',''):10s} "
          f"{c['score']:5d} "
          f"{c.get('drawdown_ratio',0):.1%} "
          f"{c.get('bear_weeks',0):6d} "
          f"{c.get('vr') or 0:6.2f} "
          f"{c.get('close_vs_ma120') or 0:11.3f} "
          f"{c.get('trigger_recency_days','?'):9} "
          f"{str(c.get('pre_peak_surge','?')):4s} "
          f"{str(c.get('quiet_pre_trigger','?')):5s}")
