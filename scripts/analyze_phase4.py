import json
from collections import Counter

with open('output/phase4_full_20251231.json', encoding='utf-8') as f:
    data = json.load(f)

candidates = data['candidates']
print(f'总命中: {len(candidates)} 只（全市场 {data["stats"]["total"]} 只中）')
print()

score_dist = Counter(c['score'] for c in candidates)
print('Score 分布：')
for s in sorted(score_dist.keys(), reverse=True):
    bar = '#' * score_dist[s]
    print(f'  score={s:3d}: {score_dist[s]:4d} 只  {bar[:60]}')

print()
top = [c for c in candidates if c['score'] >= 65]
print(f'--- score >= 65 共 {len(top)} 只 ---')
print(f'{"代码":<12} {"名称":<10} {"score":>5} {"启动周":>12} {"跌幅":>7} {"熊市周":>6} {"VR":>6}')
print('-' * 70)
for c in top:
    tw  = c.get('trigger_week') or '-'
    vr  = c.get('vr') or 0.0
    dr  = c.get('drawdown_ratio') or 0.0
    bw  = c.get('bear_weeks') or 0
    name = (c['stock_name'] or '')[:8]
    print(f"{c['instrument']:<12} {name:<10} {c['score']:>5} {tw:>12} {dr:>6.1%} {bw:>6} {vr:>6.2f}")
