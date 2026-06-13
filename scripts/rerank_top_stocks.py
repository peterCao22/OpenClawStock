"""按 W1.end_date 之后 N 个交易日的表现，对选股结果 JSON 重新排序。

== 背景与定位 ==
``results/top_50_stocks_YYYY-MM-DD.json`` 是某一选股日产出的候选股列表，每只票都带有
``windows_detail`` 多窗口信息，其中 ``W1`` 是“右底/突破”窗口，其 ``end_date`` 即该票的
选股锚点日。本脚本用 ``kline_qfq`` 日 K 线（前复权）统计每只票在 **W1.end_date 之后
N 个交易日（默认 5 日）** 的真实表现，并据此对列表重新排序：

  - ``fwd_cum_return``  : 锚点后第 N 个交易日收盘 / 锚点日收盘 - 1（未来 N 日累计涨幅）
  - ``fwd_limit_up_cnt``: 这 N 个交易日内的涨停日次数（按板块阈值判定）

排序键由 ``--sort-by`` 控制：
  - ``gain``  （默认）: 先按累计涨幅降序，涨停次数作为次级排序键
  - ``limit``        : 先按涨停次数降序，累计涨幅作为次级排序键
  - ``score``        : 综合分 = 累计涨幅(%) + 涨停次数 * --limit-weight，按综合分降序

== 涨停阈值口径 ==
与 ``phase4_limit_up_analysis.py`` 保持一致：主板 ≥9.5%，创业板/科创板（300/301/688/689）
≥19.5%。单日涨幅用相邻 bar 收盘价计算（前复权数据下与复权因子一致），避免依赖
数据库中可能为空的 change_ratio 字段。

== 运行方式 ==
    # 默认：处理 results/top_50_stocks_2026-06-03.json，窗口 5 日，按涨幅排序
    python scripts/rerank_top_stocks.py results/top_50_stocks_2026-06-03.json

    # 自定义窗口天数 / 排序方式 / 输出路径
    python scripts/rerank_top_stocks.py results/top_50_stocks_2026-06-03.json \
        --days 5 --sort-by limit --out results/top_50_reranked.json

注意:
    - 锚点“次日起算 N 个交易日”，不含锚点日当天；锚点日收盘仅作为涨幅与首日涨停的基准。
    - 用该票自己在 kline_qfq 中的 bar 顺序取交易日，自然跳过停牌；若锚点后可用 bar 不足
      N 根（如数据尚未更新到最新），则用现有 bar 计算并在结果里标记 ``fwd_days_used``。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.db_session import engine


# ─── 涨停阈值（按板块区分）──────────────────────────────────────────────────
def get_limit_thresh(instrument: str) -> float:
    """返回该票单日涨停判定阈值（百分比）。

    参数:
        instrument: 形如 ``600667.SH`` / ``300191.SZ`` 的标的代码。

    返回:
        创业板/科创板（300/301/688/689 开头）返回 19.5，其余主板返回 9.5。
        留出 0.4~0.5 个点余量以兼容四舍五入与前复权微小误差。
    """
    code = instrument.split(".")[0]
    if code.startswith(("300", "301", "688", "689")):
        return 19.5
    return 9.5


# ─── 取数 ────────────────────────────────────────────────────────────────────
def fetch_forward_bars(instrument: str, anchor_date: str, days: int) -> pd.DataFrame:
    """取锚点日（含）及其后若干交易日的日 K bar。

    取 ``anchor_date`` 当天那根 bar 作为基准，外加其后最多 ``days`` 根 bar，
    用于计算未来 N 日涨幅与逐日涨停。多取 1 根（基准）便于算首日涨幅。

    参数:
        instrument:  标的代码。
        anchor_date: 锚点日（W1.end_date），字符串 ``YYYY-MM-DD``。
        days:        锚点后需要的交易日数（窗口长度）。

    返回:
        含 ``date / open / high / low / close / volume`` 的 DataFrame，按日期升序，
        第 0 行为锚点 bar（date >= anchor_date 的第一根）。可能少于 days+1 行。
    """
    # limit = days + 1：第 0 行是基准 bar，其后 days 行才是统计窗口
    sql = text(
        """
        SELECT date, open, high, low, close, volume
        FROM kline_qfq
        WHERE instrument = :inst AND date >= :anchor
        ORDER BY date ASC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(
            sql, conn,
            params={"inst": instrument, "anchor": anchor_date, "lim": days + 1},
        )
    df["date"] = pd.to_datetime(df["date"])
    return df


# ─── 单票指标计算 ────────────────────────────────────────────────────────────
def compute_forward_metrics(instrument: str, anchor_date: str, days: int) -> dict:
    """计算单票在锚点后 N 个交易日的累计涨幅与涨停次数。

    口径:
        - 基准 = 锚点日收盘（fetch 回来的第 0 行）。
        - 窗口 = 其后最多 ``days`` 根 bar。
        - 累计涨幅 = 窗口最后一根收盘 / 基准收盘 - 1。
        - 逐日涨幅 = 当根收盘 / 上一根收盘 - 1，与板块阈值比较计涨停。

    参数:
        instrument:  标的代码。
        anchor_date: 锚点日字符串。
        days:        窗口交易日数。

    返回:
        dict，键含 fwd_cum_return / fwd_limit_up_cnt / fwd_days_used /
        anchor_close / fwd_end_date / fwd_daily_returns；
        数据不足（连基准都取不到）时返回各项为 None/0 并标记 fwd_days_used=0。
    """
    df = fetch_forward_bars(instrument, anchor_date, days)
    base = {
        "anchor_close": None,
        "fwd_end_date": None,
        "fwd_cum_return": None,
        "fwd_limit_up_cnt": 0,
        "fwd_days_used": 0,
        "fwd_daily_returns": [],
    }
    if df.empty:
        return base

    thresh = get_limit_thresh(instrument)
    closes = df["close"].astype(float).reset_index(drop=True)

    # 第 0 行为锚点基准；窗口为其后的 1..days 行
    anchor_close = float(closes.iloc[0])
    window = closes.iloc[1:]            # 不含锚点当天
    n_used = len(window)
    base["anchor_close"] = round(anchor_close, 4)
    base["fwd_days_used"] = int(n_used)

    if n_used == 0 or anchor_close == 0:
        return base

    # 逐日涨幅（相邻 bar 收盘价），用于涨停判定
    daily_ret = closes.pct_change().iloc[1: 1 + n_used] * 100.0
    limit_cnt = int((daily_ret >= thresh).sum())

    base["fwd_end_date"] = str(df["date"].iloc[n_used].date())
    base["fwd_cum_return"] = round((float(closes.iloc[n_used]) / anchor_close - 1.0) * 100.0, 2)
    base["fwd_limit_up_cnt"] = limit_cnt
    base["fwd_daily_returns"] = [round(float(x), 2) for x in daily_ret.tolist()]
    return base


# ─── 主流程 ──────────────────────────────────────────────────────────────────
def get_w1_end_date(stock: dict) -> Optional[str]:
    """从单只股票记录中取 W1 窗口的 end_date 作为锚点日。

    参数:
        stock: JSON 列表中的单只股票 dict，含 ``windows_detail``。

    返回:
        W1 的 ``end_date`` 字符串；找不到 W1 时退化为顶层 ``rebound_date`` 或返回 None。
    """
    for w in stock.get("windows_detail", []) or []:
        if w.get("window") == "W1":
            return w.get("end_date")
    # 兜底：极少数记录无 W1 时用 rebound_date，避免整票被丢弃
    return stock.get("rebound_date")


def rerank(stocks: List[dict], days: int, sort_by: str, limit_weight: float) -> List[dict]:
    """对股票列表逐只补充未来 N 日指标并排序。

    参数:
        stocks:       原始股票 dict 列表。
        days:         窗口交易日数。
        sort_by:      'gain' / 'limit' / 'score'，见模块 docstring。
        limit_weight: score 模式下涨停次数的权重。

    返回:
        排好序的新列表（原 dict 上附加 fwd_* 字段与 rerank_rank）。
    """
    enriched = []
    for s in stocks:
        anchor = get_w1_end_date(s)
        new = dict(s)
        if not anchor:
            new.update({
                "anchor_date": None, "fwd_cum_return": None,
                "fwd_limit_up_cnt": 0, "fwd_days_used": 0,
            })
        else:
            metrics = compute_forward_metrics(s["instrument"], anchor, days)
            new["anchor_date"] = anchor
            new.update(metrics)
        enriched.append(new)

    # 排序：None 视为最差（排末尾）。用元组做多键排序，降序取负。
    def sort_key(x: dict):
        ret = x.get("fwd_cum_return")
        cnt = x.get("fwd_limit_up_cnt") or 0
        ret_v = ret if ret is not None else -1e9
        if sort_by == "limit":
            return (-cnt, -ret_v)
        if sort_by == "score":
            score = ret_v + cnt * limit_weight
            x["fwd_score"] = round(score, 2) if ret is not None else None
            return (-score, -ret_v)
        # 默认 gain
        return (-ret_v, -cnt)

    enriched.sort(key=sort_key)
    for i, x in enumerate(enriched, 1):
        x["rerank_rank"] = i
    return enriched


def print_table(stocks: List[dict], sort_by: str) -> None:
    """在终端打印排序后的精简表格，便于快速查看。"""
    print(f"\n{'排名':>4} {'代码':<11} {'名称':<8} {'锚点日':<11} "
          f"{'累计涨幅%':>9} {'涨停数':>6} {'有效日':>5}")
    print("─" * 64)
    for x in stocks:
        ret = x.get("fwd_cum_return")
        ret_s = f"{ret:+.2f}" if ret is not None else "  N/A"
        print(f"{x.get('rerank_rank', 0):>4} {x['instrument']:<11} "
              f"{(x.get('name') or '')[:8]:<8} {str(x.get('anchor_date') or '-'):<11} "
              f"{ret_s:>9} {x.get('fwd_limit_up_cnt', 0):>6} "
              f"{x.get('fwd_days_used', 0):>5}")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input", help="输入选股结果 JSON 路径")
    parser.add_argument("--days", type=int, default=5, help="W1.end_date 之后统计的交易日数（默认 5）")
    parser.add_argument("--sort-by", choices=["gain", "limit", "score"], default="gain",
                        help="排序方式：gain=累计涨幅(默认) / limit=涨停次数 / score=综合分")
    parser.add_argument("--limit-weight", type=float, default=3.0,
                        help="score 模式下每个涨停的加分权重（默认 3.0）")
    parser.add_argument("--out", default=None,
                        help="输出 JSON 路径（默认在输入文件名后加 _reranked_<sortby>）")
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        sys.exit(f"[error] 输入文件不存在: {in_path}")

    with in_path.open("r", encoding="utf-8") as f:
        stocks = json.load(f)
    if not isinstance(stocks, list):
        sys.exit("[error] 输入 JSON 顶层应为股票列表")

    print(f"[info] 读取 {len(stocks)} 只票，窗口={args.days} 交易日，排序={args.sort_by}")
    ranked = rerank(stocks, args.days, args.sort_by, args.limit_weight)

    out_path = Path(args.out) if args.out else \
        in_path.with_name(f"{in_path.stem}_reranked_{args.sort_by}.json")
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(ranked, f, ensure_ascii=False, indent=2)

    print_table(ranked, args.sort_by)
    print(f"\n[done] 已写出: {out_path}")


if __name__ == "__main__":
    main()
