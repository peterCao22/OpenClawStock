"""
增量更新脚本：从 摩码云服 (Moma API) 拉取前复权日K数据，写入 kline_qfq 表

API 说明：
  K线接口：GET http://api.momaapi.com/hsstock/history/{code}/{type}/{adjust}/{token}
  基础信息：GET http://api.momaapi.com/hsstock/instrument/{code}/{token} (用于获取流通股本计算换手率)

前复权一致性处理：
  - 若 API 返回的 preClose (pc) ≠ kline_qfq 中该股前一日的 close，则认为发生了除权除息
  - 此时重新下载全量历史数据覆盖

运行方式：
    python scripts/update_kline_qfq.py
    python scripts/update_kline_qfq.py --instruments 600759.SH
"""

from __future__ import annotations

import argparse
import sys
import time
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# 加载 .env 环境变量
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

import httpx
import pandas as pd
import psycopg2
import psycopg2.extras
from loguru import logger
from dotenv import load_dotenv
from scripts.db_session import engine
load_dotenv(ROOT_DIR / ".env")

# 数据库配置
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
MOMA_TOKEN = os.getenv("MOMA_API_KEY")

# ─── 常量 ──────────────────────────────────────────────────────────────────────

# 摩码云服 API 基础 URL
MOMA_HISTORY_URL = "http://api.momaapi.com/hsstock/history/{}/{}/{}/{}"
MOMA_BASIC_URL = "http://api.momaapi.com/hsstock/instrument/{}/{}"

# API 请求间隔（秒），防止触发频率限制
# 摩码限制：300次/分钟 => 5次/秒 => 0.2s 间隔
REQUEST_SLEEP = 0.2 

# 单次重试上限
MAX_RETRIES = 3
RETRY_SLEEP = 2.0

# 价格容差
PRICE_TOLERANCE = 0.01


# ─── 数据库 ────────────────────────────────────────────────────────────────────

def get_conn():
    """获取 psycopg2 连接。"""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
    )


# ─── 辅助函数 ──────────────────────────────────────────────────────────────────

def get_moma_symbol(instrument: str) -> str:
    """
    确保代码格式为 Moma 需要的 'XXXXXX.SH' 或 'XXXXXX.SZ'
    数据库中已经是这个格式，直接返回即可，或者做简单校验
    """
    return instrument

def fetch_circulating_share(
    instrument: str,
    token: str,
    client: httpx.Client
) -> Optional[float]:
    """
    获取股票的流通股本 (fv)，用于计算换手率
    返回单位：股
    """
    url = MOMA_BASIC_URL.format(instrument, token)
    
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.get(url, timeout=10)
            if resp.status_code != 200:
                logger.warning(f"[{instrument}] 获取基础信息失败: {resp.status_code}")
                return None
                
            data = resp.json()
            
            # Moma 返回的可能是列表 [{}, {}] 也可能是单个字典 {}
            info = None
            if isinstance(data, list) and len(data) > 0:
                info = data[0]
            elif isinstance(data, dict):
                info = data
            
            if info:
                # fv: 流通股本 (单位通常是 股)
                fv = info.get('fv')
                if fv:
                    return float(fv)
            return None
        except Exception as e:
            time.sleep(RETRY_SLEEP)
            continue
    return None

# ─── API 调用 ──────────────────────────────────────────────────────────────────

def fetch_kline_moma(
    instrument: str,
    start_date: str,
    end_date: str,
    token: str,
    client: httpx.Client,
    circulating_share: Optional[float] = None
) -> Optional[pd.DataFrame]:
    """
    从 Moma 拉取前复权日K数据
    URL: /history/股票代码/d/f/token?st=...&et=...
    """
    # 构造 URL: 分时级别=d (日线), 除权方式=f (前复权)
    base_url = MOMA_HISTORY_URL.format(instrument, 'd', 'f', token)
    
    # 格式化日期为 YYYYMMDD
    st_str = start_date.replace("-", "")
    et_str = end_date.replace("-", "")
    
    params = {
        "st": st_str,
        "et": et_str,
        "lt": 5000 # 限制条数，防止过多
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = client.get(base_url, params=params, timeout=20)
            if resp.status_code == 429:
                logger.warning(f"[{instrument}] 触发限流，等待...")
                time.sleep(5)
                continue
            
            if resp.status_code != 200:
                logger.error(f"[{instrument}] HTTP {resp.status_code}")
                return None
                
            data = resp.json()
            # Moma 返回的是列表 [{}, {}]
            if not isinstance(data, list) or not data:
                # 可能是空数据或错误信息
                return None
                
            break
        except Exception as e:
            logger.warning(f"[{instrument}] 请求异常: {e}, 重试...")
            time.sleep(RETRY_SLEEP)
            continue
    else:
        return None

    # 转 DataFrame
    df = pd.DataFrame(data)
    
    # 映射字段
    # Moma: t(时间), o, h, l, c, v(量), a(额), pc(前收), sf(停牌)
    df = df.rename(columns={
        "t": "date",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "a": "amount",
        "pc": "pre_close"
    })
    
    # 处理日期格式: "2025-04-21 00:00:00" -> "2025-04-21"
    df['date'] = df['date'].apply(lambda x: x.split(' ')[0] if isinstance(x, str) else x)
    
    # 单位处理
    # Moma 的 v 通常是 "手" (Hands)，数据库通常存 "股"
    # 需要 * 100
    df['volume'] = df['volume'] * 100
    
    # 计算衍生字段
    df['change'] = df['close'] - df['pre_close']
    df['change_ratio'] = (df['close'] - df['pre_close']) / df['pre_close'] * 100
    df['amplitude'] = (df['high'] - df['low']) / df['pre_close'] * 100
    
    # 计算换手率 (Turnover)
    # turn = (volume / circulating_share) * 100
    if circulating_share and circulating_share > 0:
        df['turn'] = (df['volume'] / circulating_share) * 100
        df['turn'] = df['turn'].round(4)
    else:
        df['turn'] = None
        
    # 填充空缺字段
    for col in ["upper_limit", "lower_limit", "is_limit_up", "deal_number", 
                "ma5", "ma10", "ma20", "ma60"]:
        df[col] = None
        
    return df

# ─── 除权除息检测 ───────────────────────────────────────────────────────────────

def detect_ex_dividend(
    instrument: str,
    db_last_date: str,
    db_last_close: float,
    token: str,
    client: httpx.Client,
) -> bool:
    """
    检测是否发生除权除息
    """
    # 拉取 db_last_date 当天的数据
    # 注意：Moma 的 history 接口包含 'st' 和 'et'
    # 我们查同一天
    df_check = fetch_kline_moma(instrument, db_last_date, db_last_date, token, client)
    time.sleep(REQUEST_SLEEP)

    if df_check is None or df_check.empty:
        return False

    # Moma 返回的是前复权数据
    # 如果 API 返回的 close 与 数据库存的 close 不一致，说明历史数据被调整了
    api_close = float(df_check["close"].iloc[0])
    diff = abs(db_last_close - api_close)
    
    if diff > PRICE_TOLERANCE:
        logger.info(
            f"  [{instrument}] 检测到除权除息：DB close={db_last_close:.4f}，"
            f"API close={api_close:.4f}，差值={diff:.4f}"
        )
        return True
    return False


# ─── 数据库写入 (保持不变) ──────────────────────────────────────────────────────

INSERT_COLS = [
    "date", "instrument", "name",
    "open", "high", "low", "close", "pre_close",
    "volume", "deal_number", "amount",
    "change_ratio", "change", "turn",
    "upper_limit", "lower_limit", "is_limit_up", "amplitude",
    "ma5", "ma10", "ma20", "ma60",
]

UPSERT_SQL = f"""
    INSERT INTO kline_qfq ({', '.join(INSERT_COLS)})
    VALUES ({', '.join([f'%({c})s' for c in INSERT_COLS])})
    ON CONFLICT (date, instrument)
    DO UPDATE SET
        open         = EXCLUDED.open,
        high         = EXCLUDED.high,
        low          = EXCLUDED.low,
        close        = EXCLUDED.close,
        pre_close    = EXCLUDED.pre_close,
        volume       = EXCLUDED.volume,
        amount       = EXCLUDED.amount,
        change_ratio = EXCLUDED.change_ratio,
        change       = EXCLUDED.change,
        turn         = EXCLUDED.turn,
        amplitude    = EXCLUDED.amplitude,
        updated_at   = CURRENT_TIMESTAMP
"""

def upsert_stock(conn, instrument, df, name, dry_run):
    df = df.copy()
    df["instrument"] = instrument
    df["name"] = name
    df = df.where(pd.notna(df), other=None)
    records = df[INSERT_COLS].to_dict("records")
    
    if dry_run:
        logger.debug(f"  [{instrument}] dry-run：{len(records)} 行")
        return len(records)

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, records, page_size=1000)
    conn.commit()
    return len(records)

def delete_instrument_history(conn, instrument: str) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM kline_qfq WHERE instrument = %s", (instrument,))
        deleted = cur.rowcount
    conn.commit()
    return deleted

# ─── 查询辅助 ──────────────────────────────────────────────────────────────────

def get_instruments(connectable, filter_instruments=None, limit=None, offset=0):
    try:
        df = pd.read_sql(
            "SELECT instrument FROM stock_list WHERE name IS NOT NULL AND name != '' ORDER BY instrument",
            connectable,
        )
        instruments = df["instrument"].tolist()
    except Exception:
        df = pd.read_sql("SELECT DISTINCT instrument FROM kline_qfq ORDER BY instrument", connectable)
        instruments = df["instrument"].tolist()

    if filter_instruments:
        instruments = [i for i in instruments if i in set(filter_instruments)]
    else:
        if offset: instruments = instruments[offset:]
        if limit: instruments = instruments[:limit]
    
    return instruments

def get_stock_names(connectable):
    try:
        df = pd.read_sql("SELECT instrument, name FROM stock_list", connectable)
        return dict(zip(df["instrument"], df["name"]))
    except:
        return {}

def get_update_range(conn, instrument, global_start, global_end):
    end_date = global_end if global_end else (date.today() - timedelta(days=0)).strftime("%Y-%m-%d") # 默认到今天
    
    if global_start:
        return global_start, end_date

    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date) FROM kline_qfq WHERE instrument = %s", (instrument,))
        row = cur.fetchone()

    if row and row[0]:
        next_day = (row[0] + timedelta(days=1)).strftime("%Y-%m-%d")
        return next_day, end_date
    else:
        return "2020-01-01", end_date

# ─── 派生字段回填 (MA) ──────────────────────────────────────────────────────────

def recalc_derived_fields(conn, instruments: Optional[List[str]] = None) -> None:
    """
    只回填 MA，因为 pre_close, change, amplitude, turn 已经在 Python 端算好了
    """
    if instruments:
        placeholders = ", ".join(["%s"] * len(instruments))
        scope_where  = f"WHERE instrument IN ({placeholders})"
        scope_and_q  = f"AND q.instrument IN ({placeholders})"
        scope_args   = list(instruments)
    else:
        scope_where = scope_and_q = ""
        scope_args  = []

    t0 = time.time()
    with conn.cursor() as cur:
        # MA5 / MA10 / MA20 / MA60
        cur.execute(f"""
            UPDATE kline_qfq AS q
            SET
                ma5  = ROUND(src.ma5::numeric,  4),
                ma10 = ROUND(src.ma10::numeric, 4),
                ma20 = ROUND(src.ma20::numeric, 4),
                ma60 = ROUND(src.ma60::numeric, 4)
            FROM (
                SELECT instrument, date,
                    AVG(close) OVER (PARTITION BY instrument ORDER BY date ROWS BETWEEN  4 PRECEDING AND CURRENT ROW) AS ma5,
                    AVG(close) OVER (PARTITION BY instrument ORDER BY date ROWS BETWEEN  9 PRECEDING AND CURRENT ROW) AS ma10,
                    AVG(close) OVER (PARTITION BY instrument ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                    AVG(close) OVER (PARTITION BY instrument ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60
                FROM kline_qfq
                {scope_where}
            ) AS src
            WHERE q.instrument = src.instrument
              AND q.date        = src.date
              AND q.ma5        IS NULL
            {scope_and_q}
        """, scope_args * 2)
        n_ma = cur.rowcount

    conn.commit()
    logger.info(f"MA回填完成（{time.time() - t0:.1f}s）：{n_ma} 行")


# ─── 派生月表增量更新 ──────────────────────────────────────────────────────────

def refresh_monthly_for_instruments(instruments: List[str]) -> None:
    """触发 ``kline_qfq_monthly`` 月表的增量重算（仅本次更新过的票）。

    设计要点:
        - 月表是日线的派生视图（``scripts/build_kline_qfq_monthly.py``），仅存"已完整月"
          的 O/H/L/C/V + ``ma60_m``；
        - 因为 ``ma60_m`` 是 60 月滚动均值，"补尾巴" 只需要重算该票的所有月份并 upsert，
          逐票几十毫秒；几百票通常 < 30 秒；
        - 该步骤失败不应阻塞日线更新主流程（数据已落库，月表可下次手动 ``--rebuild``
          或单票 ``--instruments`` 修复）。
    """
    if not instruments:
        return
    try:
        # 局部导入避免循环引用、且避免在不需要时拉起依赖
        from scripts.build_kline_qfq_monthly import (
            get_conn as _get_conn_m,
            create_table as _create_table_m,
            update_for_instruments as _update_inst_m,
        )
        m_conn = _get_conn_m()
        try:
            _create_table_m(m_conn)  # 幂等：不存在才建表
            _update_inst_m(m_conn, instruments)
        finally:
            m_conn.close()
        logger.info(f"月表增量更新完成: {len(instruments)} 票")
    except Exception as e:
        # 派生表更新失败不影响日线更新本身；记日志后由人工 rebuild 兜底
        logger.error(f"月表增量更新失败（不影响日线主表）: {e}")


# ─── 主流程 ────────────────────────────────────────────────────────────────────

def main(start_date=None, end_date=None, instruments_filter=None, limit=None, offset=0, dry_run=False, fix_null_turn=False):
    token = MOMA_TOKEN
    if not token:
        logger.error("未设置 MOMA_API_KEY，无法运行")
        return

    conn = get_conn()
    try:
        if fix_null_turn:
            logger.info("模式启动：自动修复 NULL 换手率记录")
            try:
                # 查找所有存在 turn 为 NULL 的股票
                df_null = pd.read_sql("SELECT DISTINCT instrument FROM kline_qfq WHERE turn IS NULL ORDER BY instrument", engine)
                instruments = df_null["instrument"].tolist()
            except Exception as e:
                logger.error(f"查询需修复股票失败: {e}")
                return
            
            if instruments_filter:
                instruments = [i for i in instruments if i in set(instruments_filter)]
            
            if offset: instruments = instruments[offset:]
            if limit: instruments = instruments[:limit]
        else:
            instruments = get_instruments(engine, instruments_filter, limit=limit, offset=offset)
            
        name_map = get_stock_names(engine)
        total = len(instruments)
        logger.info(f"共 {total} 只股票待处理")

        # 获取全表日期范围用于判断
        df_range = pd.read_sql("SELECT MAX(date) AS max_d FROM kline_qfq", engine)
        qfq_max_date = str(df_range["max_d"].iloc[0]) if pd.notna(df_range["max_d"].iloc[0]) else None

        success = exdiv = skipped = failed = 0
        updated_instruments = set()
        t0 = time.time()

        with httpx.Client(timeout=20) as client:
            for i, instrument in enumerate(instruments):
                name = name_map.get(instrument)
                
                if fix_null_turn:
                    # 针对该股票，找到最早的 NULL 日期作为开始日期
                    with conn.cursor() as cur:
                        cur.execute("SELECT MIN(date) FROM kline_qfq WHERE instrument = %s AND turn IS NULL", (instrument,))
                        row = cur.fetchone()
                    
                    if row and row[0]:
                        start = row[0].strftime("%Y-%m-%d")
                        # 结束日期默认为今天，确保覆盖所有后续可能缺失的数据
                        end = end_date if end_date else date.today().strftime("%Y-%m-%d")
                    else:
                        skipped += 1
                        continue
                else:
                    start, end = get_update_range(conn, instrument, start_date, end_date)
                
                if start > end:
                    skipped += 1
                    continue

                # 1. 获取流通股本 (用于计算换手率)
                circulating_share = fetch_circulating_share(instrument, token, client)
                
                # 2. 拉取 K 线
                df = fetch_kline_moma(instrument, start, end, token, client, circulating_share)
                time.sleep(REQUEST_SLEEP)

                if df is None or df.empty:
                    failed += 1
                    continue

                # 3. 除权除息检测 (仅当向后追加时)
                is_forward_append = qfq_max_date and (start > qfq_max_date)
                if is_forward_append:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT date, close FROM kline_qfq WHERE instrument = %s ORDER BY date DESC LIMIT 1",
                            (instrument,)
                        )
                        last_row = cur.fetchone()
                    
                    if last_row and detect_ex_dividend(instrument, str(last_row[0]), float(last_row[1]), token, client):
                        # 全量重拉
                        hist_start = "2020-01-01"
                        df_full = fetch_kline_moma(instrument, hist_start, end, token, client, circulating_share)
                        time.sleep(REQUEST_SLEEP)
                        
                        if df_full is not None and not df_full.empty:
                            n_del = delete_instrument_history(conn, instrument)
                            upsert_stock(conn, instrument, df_full, name, dry_run)
                            updated_instruments.add(instrument)
                            logger.info(f"  [{instrument}] 除权除息重拉: {n_del} -> {len(df_full)}")
                            exdiv += 1
                            continue
                        else:
                            failed += 1
                            continue

                # 4. 正常写入
                upsert_stock(conn, instrument, df, name, dry_run)
                success += 1
                updated_instruments.add(instrument)

                if (i + 1) % 50 == 0:
                    logger.info(f"进度 {i+1}/{total} | 成功 {success} 除权 {exdiv} 失败 {failed}")

        logger.success(f"完成: 成功 {success}, 除权 {exdiv}, 跳过 {skipped}, 失败 {failed}")

        if not dry_run and updated_instruments:
            recalc_derived_fields(conn, list(updated_instruments))
            # 派生月表（kline_qfq_monthly）增量更新：仅对本次更新过的票重算月线 + ma60_m
            # 月表是 quant_picker 月线层规则的取数源，必须与日线保持一致
            refresh_monthly_for_instruments(list(updated_instruments))

    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, help="YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="YYYY-MM-DD")
    parser.add_argument("--instruments", nargs="+", help="股票代码列表")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--fix-null-turn", action="store_true", help="自动修复换手率(turn)为NULL的记录")
    args = parser.parse_args()
    
    main(args.start, args.end, args.instruments, args.limit, args.offset, args.dry_run, args.fix_null_turn)
