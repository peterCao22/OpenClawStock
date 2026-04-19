"""一次性修复 kline_qfq 中 pre_close / change / change_ratio / amplitude 字段为 NaN 字面值的脏数据

背景:
    历史 ``update_kline_qfq.py`` 在写库时, 若某次拉取的 K 线数据第一行 API 没返回 ``pc``
    （前收盘价）字段, ``df['change'] = df['close'] - df['pre_close']`` 会得到 NaN, 然后
    PostgreSQL numeric 列把 NaN 当作合法字面值存进去（IS NULL 不命中, 但 pandas 读出仍为 NaN）。
    这种"段首"行常见于：
        - 2020-2021 数据补齐后接到原数据的第一天（典型 2022-01-04）；
        - 后续除权除息触发的 "全量重拉" 段首；
        - 任何分批 / offset 的批次起点。

修复口径:
    用 ``LAG(close) OVER (PARTITION BY instrument ORDER BY date)`` 取该票"前一交易日"的收盘价
    填回 ``pre_close``, 并据此重算 ``change / change_ratio / amplitude``。
    在 **前复权** 数据下, 任意非除权除息日的 ``pre_close`` 应严格等于前一日 ``close``,
    所以这是无损修复; 真正的除权除息日 API 已经填了正确 ``pc``, 不会被本 SQL 命中。

不动字段:
    ``close / high / low / open / volume / amount`` 等核心字段一概不改, 月线/形态/微观/趋势规则不受影响。

运行方式:
    python scripts/fix_pre_close_nan.py             # dry-run, 仅打印将要修改的行数与样本
    python scripts/fix_pre_close_nan.py --apply     # 真正执行 UPDATE

注意:
    - 幂等: 重复跑没有副作用（同一行第二次跑不会再被命中）；
    - 不依赖外部 API, 不动 close 等数值列, 风险极低；
    - 单条 UPDATE, 1.4w 行预计几秒内完成。
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.db_session import engine
import psycopg2

from dotenv import load_dotenv
import os
load_dotenv(ROOT_DIR / ".env")


def get_conn():
    """psycopg2 连接（与 update_kline_qfq.py 用同一组环境变量）"""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
    )


# 命中条件: pre_close / change / amplitude 任一为 NaN 字面值即为脏行
# （change_ratio 若为 NaN 通常是 pre_close 也 NaN，会被一起命中）
DIRTY_PREDICATE = (
    "(k.pre_close = 'NaN'::numeric "
    " OR k.change    = 'NaN'::numeric "
    " OR k.amplitude = 'NaN'::numeric)"
)

# 用 LAG 取前一交易日的 close。前一交易日不存在或前一根的 close 也异常时,
# 不参与本次修复（极少数票第一根就 NaN 的情况留给 update_kline_qfq 重拉）
PAIRED_CTE = """
WITH paired AS (
    SELECT instrument, date, close, high, low,
           LAG(close) OVER (PARTITION BY instrument ORDER BY date) AS prev_close
    FROM kline_qfq
)
"""


def query_dirty_summary(conn) -> dict:
    """统计脏行总数 + 按年分布 + 可修复 / 不可修复 行数"""
    with conn.cursor() as cur:
        # 总数
        cur.execute(f"SELECT COUNT(*) FROM kline_qfq k WHERE {DIRTY_PREDICATE}")
        total_dirty = cur.fetchone()[0]

        # 可修复行数（前一日 close 存在且非 NaN 且 > 0）
        cur.execute(f"""
            {PAIRED_CTE}
            SELECT COUNT(*) FROM kline_qfq k JOIN paired p
              ON k.instrument = p.instrument AND k.date = p.date
            WHERE {DIRTY_PREDICATE}
              AND p.prev_close IS NOT NULL
              AND p.prev_close <> 'NaN'::numeric
              AND p.prev_close > 0
        """)
        fixable = cur.fetchone()[0]

    df_year = pd.read_sql(
        f"""
        SELECT EXTRACT(YEAR FROM date::date)::int AS y, COUNT(*) AS n
        FROM kline_qfq k WHERE {DIRTY_PREDICATE}
        GROUP BY 1 ORDER BY 1
        """,
        conn,
    )
    return {
        'total_dirty': total_dirty,
        'fixable': fixable,
        'unfixable': total_dirty - fixable,
        'by_year': df_year,
    }


def query_sample(conn, n: int = 8) -> pd.DataFrame:
    """抽几条 dry-run 样本：可修复行 + 修复后预期值"""
    return pd.read_sql(
        f"""
        {PAIRED_CTE}
        SELECT
            k.instrument, k.date,
            k.close,
            k.pre_close      AS pre_close_now,
            p.prev_close     AS pre_close_after,
            k.change         AS change_now,
            (k.close - p.prev_close) AS change_after,
            k.amplitude      AS amplitude_now,
            CASE WHEN p.prev_close > 0
                 THEN (k.high - k.low) / p.prev_close * 100
                 ELSE NULL END AS amplitude_after
        FROM kline_qfq k JOIN paired p
          ON k.instrument = p.instrument AND k.date = p.date
        WHERE {DIRTY_PREDICATE}
          AND p.prev_close IS NOT NULL
          AND p.prev_close <> 'NaN'::numeric
          AND p.prev_close > 0
        ORDER BY k.date DESC LIMIT {n}
        """,
        conn,
    )


UPDATE_SQL = f"""
{PAIRED_CTE}
UPDATE kline_qfq AS k
SET pre_close    = p.prev_close,
    change       = (k.close - p.prev_close),
    change_ratio = ((k.close - p.prev_close) / p.prev_close * 100),
    amplitude    = ((k.high - k.low) / p.prev_close * 100),
    updated_at   = CURRENT_TIMESTAMP
FROM paired p
WHERE k.instrument = p.instrument AND k.date = p.date
  AND {DIRTY_PREDICATE}
  AND p.prev_close IS NOT NULL
  AND p.prev_close <> 'NaN'::numeric
  AND p.prev_close > 0
"""


def apply_fix(conn) -> int:
    """真正执行 UPDATE, 返回修复行数"""
    with conn.cursor() as cur:
        t0 = time.time()
        cur.execute(UPDATE_SQL)
        n = cur.rowcount
        conn.commit()
        print(f"[apply] 修复 {n} 行，耗时 {time.time() - t0:.1f}s")
    return n


def main(apply: bool):
    conn = get_conn()
    try:
        # 调研阶段：先打印汇总
        s = query_dirty_summary(conn)
        print('=== 脏行统计 ===')
        print(f"  总脏行数 (dirty): {s['total_dirty']}")
        print(f"  可修复 (fixable): {s['fixable']}")
        print(f"  不可修复 (无前一日数据): {s['unfixable']}")
        print('  按年分布:')
        print(s['by_year'].to_string(index=False))

        print('\n=== 修复样本（before / after, 取最新 8 条）===')
        sample = query_sample(conn, 8)
        with pd.option_context('display.max_columns', None, 'display.width', 200):
            print(sample.to_string(index=False))

        if not apply:
            print('\n[dry-run] 未执行任何 UPDATE。如需真正修复，加 --apply。')
            return

        print('\n=== 执行 UPDATE ===')
        n = apply_fix(conn)

        # 修复后再统计一次
        s2 = query_dirty_summary(conn)
        print('\n=== 修复后脏行统计 ===')
        print(f"  剩余脏行: {s2['total_dirty']} (其中不可修复 {s2['unfixable']})")
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--apply', action='store_true', help='真正执行 UPDATE，默认 dry-run')
    args = parser.parse_args()
    main(args.apply)
