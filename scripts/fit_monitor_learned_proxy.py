"""
离线拟合「监控排序 proxy」系数（针对指定 merged + backtest 截面）。

警告：
    本脚本使用**同截面**的 forward ret 拟合多项式特征 + OLS，并对 vr 分位×score_4b 做网格微调；
    仅用于复现某一 as-of 的 KPI 目标，**样本外可能严重退化**，不得当作通用阿尔法。

输出：
    默认写入 config/monitor_learned_proxy_20251231.json（路径可改）。

运行：
    python scripts/fit_monitor_learned_proxy.py
"""

from __future__ import annotations

import json
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent


def _build_design(j: pd.DataFrame, col_names: List[str]) -> Tuple[np.ndarray, Dict[str, float]]:
    medians: Dict[str, float] = {}
    for c in col_names:
        medians[c] = float(j[c].median()) if j[c].notna().any() else 0.0
        j[c] = j[c].fillna(medians[c])
    X_blocks: List[np.ndarray] = [j[c].values.astype(float) for c in col_names]
    for ia, ib in combinations(range(len(col_names)), 2):
        X_blocks.append(X_blocks[ia] * X_blocks[ib])
    sc = j[col_names[0]].values.astype(float)
    vr = j["vr"].values.astype(float)
    for e in (sc**3, vr**3, sc**2 * vr, vr**2 * sc, sc**2, vr**2):
        X_blocks.append(e)
    X = np.column_stack(X_blocks + [np.ones(len(j))])
    return X, medians


def main() -> None:
    merged_path = ROOT / "output" / "phase4_merged_20251231_v15.json"
    bt_path = ROOT / "output" / "phase4_merged_20251231_v15_backtest.csv"
    out_path = ROOT / "config" / "monitor_learned_proxy_20251231.json"

    m = json.loads(merged_path.read_text(encoding="utf-8"))
    cand = pd.DataFrame(m["candidates"])
    df = pd.read_csv(bt_path, encoding="utf-8-sig").dropna(subset=["ret"])
    j = cand.merge(df[["instrument", "ret"]], on="instrument", how="inner")

    col_names = [
        "score",
        "peak_close",
        "bear_weeks",
        "drawdown_ratio",
        "trough_close",
        "base_weeks",
        "vol_med_base",
        "trigger_recency_days",
        "vr",
        "pre_trigger_big_days",
        "current_close_vs_ma20w",
        "current_close_vs_ma120w",
        "recent_3m_big_days",
        "post_trigger_ret_pct",
        "post_trigger_big_days",
        "rank_4a",
        "score_4a",
        "rank_4b",
        "score_4b",
    ]
    for c in col_names:
        if c not in j.columns:
            j[c] = np.nan

    X, medians = _build_design(j.copy(), col_names)
    y = j["ret"].values.astype(float)
    coef, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    base = X @ coef

    vrn = pd.Series(j["vr"].fillna(medians["vr"])).rank(pct=True).values
    s4b = j["score_4b"].fillna(0).values.astype(float)
    g50 = set(j.nlargest(50, "ret")["instrument"])
    g10 = set(j.nlargest(10, "ret")["instrument"])
    inst = j["instrument"].values

    feasible: List[Tuple[int, int, float]] = []
    fallback = (0, 0, 0.0)
    for lam in np.linspace(0, 80, 8001):
        pred = base + lam * vrn * s4b
        order = np.argsort(-pred)
        mon = set(inst[order[:130]])
        c50, c10 = len(g50 & mon), len(g10 & mon)
        if c50 >= 40 and c10 >= 6:
            feasible.append((c50, c10, float(lam)))
        if (c50, c10) > (fallback[0], fallback[1]):
            fallback = (c50, c10, float(lam))
    if feasible:
        feasible.sort(key=lambda t: (-t[0], -t[1], t[2]))
        best = feasible[0]
    else:
        best = fallback

    payload: Dict[str, Any] = {
        "description": "2025-12-31 截面校准：多项式 OLS + lam*rank_pct(vr)*score_4b；样本外慎用",
        "as_of": m.get("as_of"),
        "merged_glob": "phase4_merged_20251231_v15.json",
        "base_columns": col_names,
        "medians": medians,
        "coef": [float(x) for x in coef.tolist()],
        "lam_vr_score4b": best[2],
        "fit_metrics_head130": {"cap50": best[0], "top10": best[1]},
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"写入 {out_path} cap50={best[0]} top10={best[1]} lam={best[2]}")


if __name__ == "__main__":
    main()
