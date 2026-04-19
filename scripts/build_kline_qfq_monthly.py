"""派生月表 ``kline_qfq_monthly`` 的建表 + 全量/增量回填脚本

== 背景与定位 ==
``quant_picker.py`` 里"月线层"规则需要每只票的月 K 线（O/H/L/C/V）+ 月线 60 月均线
（``ma60_m``，俗称 5 年线）。原实现是每次选股临时把全市场 ~9 年日线（923 万行）拉到
内存做月线聚合 + rolling，单次扫全市场要 5–6 分钟，瓶颈集中在 SQL 拉日线。
本脚本把"月线 + ma60_m"物化成派生表 ``kline_qfq_monthly``：

  - 仅存"已完整月"（上月最后一交易日已落库 / 当月 1 号之前的所有月份）；
  - **不存"未走完的当月"**——这一部分在 ``quant_picker`` 选股时按 T 日截止从日线现场聚合，
    避免历史回测时月表里的"当月行"是基于现在的当月而不是 T 日的当月；
  - ``ma60_m`` = 该月（含）及其之前 59 个完整月 close 的均值，月数不足 60 时为 ``NULL``。

== 表结构 ==
``kline_qfq_monthly``:
    instrument        VARCHAR(20)  NOT NULL
    month_key         CHAR(7)      NOT NULL    -- 'YYYY-MM'
    last_trade_date   DATE         NOT NULL    -- 该月最后一个**实际**交易日
    open / high / low / close / volume  -- 月 K 五要素
    ma60_m            NUMERIC(12,4)            -- 60 月线，前 59 个月为 NULL
    updated_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
    PRIMARY KEY (instrument, month_key)
    INDEX (instrument, last_trade_date DESC)

== 运行方式 ==
    # 1. 建表（幂等，已存在不会重建）
    python scripts/build_kline_qfq_monthly.py --create-only

    # 2. 全量回填（推荐第一次跑：建表 + 全市场 ~5006 票）
    python scripts/build_kline_qfq_monthly.py --rebuild

    # 3. 增量重算单票或多票（被 update_kline_qfq.py 调用 / 人工修数）
    python scripts/build_kline_qfq_monthly.py --instruments 002082.SZ 002413.SZ

注意:
    - 月表是日线的"派生视图"：月表里的数据不应单独修改，所有变更都应回到日线表后
      由本脚本重算；
    - 全量回填首次执行约 3-5 分钟（5006 票），后续日常增量是毫秒级；
    - 当某只票的某月只有 1 个交易日（如刚上市 / 长期停牌后复牌）也照常入表，规则层
      自己判断是否参与计算。
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.db_session import engine
import os
from dotenv import load_dotenv

load_dotenv(ROOT_DIR / ".env")


# ─── 数据库连接 ──────────────────────────────────────────────────────────────
def get_conn():
    """psycopg2 连接（与 update_kline_qfq.py 同源环境变量）"""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
    )


# ─── DDL ─────────────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS kline_qfq_monthly (
    instrument        VARCHAR(20)  NOT NULL,
    month_key         CHAR(7)      NOT NULL,
    last_trade_date   DATE         NOT NULL,
    open              NUMERIC(12,4),
    high              NUMERIC(12,4),
    low               NUMERIC(12,4),
    close             NUMERIC(12,4),
    volume            NUMERIC(20,2),
    ma60_m            NUMERIC(12,4),
    updated_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instrument, month_key)
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_monthly_inst_lastdate
ON kline_qfq_monthly(instrument, last_trade_date DESC);
"""

UPSERT_SQL = """
INSERT INTO kline_qfq_monthly
    (instrument, month_key, last_trade_date, open, high, low, close, volume, ma60_m, updated_at)
VALUES %s
ON CONFLICT (instrument, month_key) DO UPDATE SET
    last_trade_date = EXCLUDED.last_trade_date,
    open   = EXCLUDED.open,
    high   = EXCLUDED.high,
    low    = EXCLUDED.low,
    close  = EXCLUDED.close,
    volume = EXCLUDED.volume,
    ma60_m = EXCLUDED.ma60_m,
    updated_at = CURRENT_TIMESTAMP;
"""


# ─── 核心：日线 → 月线 ──────────────────────────────────────────────────────
def aggregate_monthly_for_instrument(
    df_daily: pd.DataFrame,
    exclude_current_month: bool = True,
    today: Optional[date] = None,
) -> pd.DataFrame:
    """单票日线 → 月线 + ma60_m。

    与 ``quant_picker.aggregate_monthly_klines`` 口径一致：月开 = 月内首日 open，
    月收 = 月内末日 close，月高/月低 = 区间 max/min，月量 = 区间 sum；ma60_m = 月线
    close 60 月滚动均值（不足 60 月为 NaN）。

    参数:
        df_daily: 含 ``date / open / high / low / close / volume`` 的单票日线，
            ``date`` 为字符串或日期，可乱序。
        exclude_current_month: 是否剔除"当月"（默认 True）。月表只存已完整月，避免
            把"未走完的当月"固化进表（quant_picker 在 T 日会现场重算 T 当月）。
        today: 用于判断"当月"，默认取系统日期；测试时可注入。

    返回:
        DataFrame[instrument 暂未填, month_key, last_trade_date, open, high, low,
                  close, volume, ma60_m]，按时间升序。空数据返回空表。
    """
    if df_daily is None or df_daily.empty:
        return pd.DataFrame(columns=[
            'month_key', 'last_trade_date', 'open', 'high', 'low', 'close', 'volume', 'ma60_m'
        ])

    df = df_daily[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
    df['_dt'] = pd.to_datetime(df['date'])
    df = df.sort_values('_dt').reset_index(drop=True)
    df['month_key'] = df['_dt'].dt.strftime('%Y-%m')

    # 聚合
    agg = df.groupby('month_key', sort=True).agg(
        open=('open', 'first'),
        high=('high', 'max'),
        low=('low', 'min'),
        close=('close', 'last'),
        volume=('volume', 'sum'),
        last_trade_date=('_dt', 'last'),
    ).reset_index()
    agg['last_trade_date'] = agg['last_trade_date'].dt.date

    # 剔除"当月"——避免把未走完的月份固化（与 quant_picker 在 T 日现场聚合保持一致）
    if exclude_current_month:
        if today is None:
            today = date.today()
        cur_month_key = today.strftime('%Y-%m')
        agg = agg[agg['month_key'] != cur_month_key].reset_index(drop=True)

    if agg.empty:
        return agg

    # ma60_m: 60 月滚动均值（min_periods=60，前 59 个月为 NaN）
    agg['ma60_m'] = agg['close'].astype(float).rolling(window=60, min_periods=60).mean()

    return agg


# ─── 入库 ────────────────────────────────────────────────────────────────────
def upsert_monthly(conn, instrument: str, df_monthly: pd.DataFrame) -> int:
    """把单票的月线 DataFrame upsert 到 kline_qfq_monthly。返回 upsert 行数。

    NaN（如 ma60_m 不足 60 月）会写成 NULL，便于 SQL 端 IS NULL 判断。
    """
    if df_monthly is None or df_monthly.empty:
        return 0

    rows = []
    now_ts = datetime.now()
    for _, r in df_monthly.iterrows():
        ma60 = r['ma60_m']
        ma60_v = float(ma60) if pd.notna(ma60) else None
        rows.append((
            instrument,
            r['month_key'],
            r['last_trade_date'],
            float(r['open']) if pd.notna(r['open']) else None,
            float(r['high']) if pd.notna(r['high']) else None,
            float(r['low']) if pd.notna(r['low']) else None,
            float(r['close']) if pd.notna(r['close']) else None,
            float(r['volume']) if pd.notna(r['volume']) else None,
            ma60_v,
            now_ts,
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=500)
    return len(rows)


def delete_monthly_for_instrument(conn, instrument: str) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM kline_qfq_monthly WHERE instrument = %s", (instrument,))
        n = cur.rowcount
    return n


# ─── 取数 ────────────────────────────────────────────────────────────────────
def fetch_all_instruments(conn) -> List[str]:
    """从 stock_list 取所有 instrument（与 update_kline_qfq.py 口径一致：排除空名）"""
    df = pd.read_sql(
        "SELECT instrument FROM stock_list WHERE name IS NOT NULL AND name != '' ORDER BY instrument",
        conn,
    )
    return df['instrument'].tolist()


def fetch_daily_for_instrument(conn, instrument: str) -> pd.DataFrame:
    """单票全量日线（注意：用 SQL ORDER BY 才能保证 aggregate 顺序）"""
    return pd.read_sql(
        f"""
        SELECT date, open, high, low, close, volume
        FROM kline_qfq
        WHERE instrument = '{instrument}'
        ORDER BY date ASC
        """,
        conn,
    )


def fetch_daily_batch(conn, instruments: List[str]) -> pd.DataFrame:
    """批量取多票全量日线（用于全量回填，比逐个票发 SQL 省 RTT）"""
    if not instruments:
        return pd.DataFrame()
    placeholders = ','.join([f"'{i}'" for i in instruments])
    return pd.read_sql(
        f"""
        SELECT instrument, date, open, high, low, close, volume
        FROM kline_qfq
        WHERE instrument IN ({placeholders})
        ORDER BY instrument, date ASC
        """,
        conn,
    )


# ─── 顶层流程 ────────────────────────────────────────────────────────────────
def create_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(CREATE_INDEX_SQL)
    conn.commit()
    print('[ddl] kline_qfq_monthly 表与索引已创建（或已存在）')


def rebuild_all(conn, batch_size: int = 200) -> None:
    """全量重建：清空表后批量回填全市场。

    分批处理是为了控制内存；每批 200 票约 4-6 万行月线，psycopg2 入库 ~1 秒。
    """
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE kline_qfq_monthly")
    conn.commit()
    print('[rebuild] 表已 TRUNCATE')

    instruments = fetch_all_instruments(conn)
    total = len(instruments)
    print(f'[rebuild] 共 {total} 只票，分批 {batch_size}/批')

    t_start = time.time()
    upserted_total = 0
    today = date.today()

    for batch_start in range(0, total, batch_size):
        batch = instruments[batch_start:batch_start + batch_size]
        t_b = time.time()
        df_batch = fetch_daily_batch(conn, batch)
        if df_batch.empty:
            continue
        # 按 instrument 分组分别聚合 → upsert
        for inst, g in df_batch.groupby('instrument'):
            df_m = aggregate_monthly_for_instrument(g, exclude_current_month=True, today=today)
            n = upsert_monthly(conn, inst, df_m)
            upserted_total += n
        conn.commit()
        elapsed = time.time() - t_b
        done = min(batch_start + batch_size, total)
        rate = done / max(time.time() - t_start, 1e-6)
        eta = (total - done) / max(rate, 1e-6)
        print(f'[rebuild] {done}/{total}  本批耗时 {elapsed:.1f}s  '
              f'累计入库 {upserted_total} 行  剩余~{eta:.0f}s')

    print(f'[rebuild] 完成：{total} 票 / {upserted_total} 月线行 / 总耗时 {time.time()-t_start:.1f}s')


def update_for_instruments(conn, instruments: List[str]) -> None:
    """重算指定票（增量入口，被 update_kline_qfq.py 调用）。

    每只票：删旧 + 重算 + upsert（"重算"包含 ma60_m，因为 ma60_m 依赖窗口内 60 月连续数据）。
    增量场景下逐票几十毫秒；几百票全部更新通常 < 30 秒。
    """
    if not instruments:
        return
    today = date.today()
    n_inst = 0
    n_rows = 0
    t0 = time.time()
    for inst in instruments:
        df_d = fetch_daily_for_instrument(conn, inst)
        if df_d.empty:
            continue
        df_m = aggregate_monthly_for_instrument(df_d, exclude_current_month=True, today=today)
        # 对该票 upsert（用 PK 冲突合并；不用 delete+insert，避免锁等）
        n_rows += upsert_monthly(conn, inst, df_m)
        n_inst += 1
    conn.commit()
    print(f'[update] 重算 {n_inst} 票 / {n_rows} 月线行 / 耗时 {time.time()-t0:.1f}s')


# ─── CLI ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--create-only', action='store_true', help='仅建表，不回填')
    parser.add_argument('--rebuild', action='store_true', help='全市场重建（先 TRUNCATE 再回填）')
    parser.add_argument('--instruments', nargs='+', help='增量重算指定票（如 002082.SZ）')
    parser.add_argument('--batch-size', type=int, default=200, help='全量回填的分批大小')
    args = parser.parse_args()

    conn = get_conn()
    try:
        create_table(conn)
        if args.create_only:
            return
        if args.rebuild:
            rebuild_all(conn, batch_size=args.batch_size)
            return
        if args.instruments:
            update_for_instruments(conn, args.instruments)
            return
        print('[hint] 没有指定 --rebuild 或 --instruments，仅建表完成。')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
