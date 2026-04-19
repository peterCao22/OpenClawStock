"""
使用 exchange_calendars 库生成本地交易所日历中的「交易日」，并 upsert 到 PostgreSQL 的 trading_calendar 表。

数据来源：https://github.com/gerrymanoim/exchange_calendars
- 安装：pip install exchange_calendars
- A 股现货交易日与上交所 XSHG 日历一致（库内中国股票现货仅提供 XSHG；沪深休市安排一致）。

注意：
- 日历由社区维护，若交易所临时调休，可能与官方最新通知存在偏差；生产环境关键日建议与上交所/深交所公告核对。
- 本脚本只写入「交易日」行（is_trading_day=true），不生成非交易日的占位行，与现有 trading_calendar 用法一致。

运行示例：
    py -3 scripts/sync_exchange_calendar.py
    py -3 scripts/sync_exchange_calendar.py --beg 2019-01-01 --end 2023-01-01 --calendar XSHG
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from typing import List

import exchange_calendars as xcals
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))
load_dotenv(ROOT_DIR / ".env")

from scripts.db_session import SessionLocal  # noqa: E402

# 默认与此前 TickDB 补数需求一致
DEFAULT_BEG = date(2019, 1, 1)
DEFAULT_END = date(2023, 1, 1)
DEFAULT_CALENDAR = "XSHG"


def sessions_to_yyyymmdd_list(sessions: pd.DatetimeIndex) -> List[str]:
    """
    将 exchange_calendars 返回的会话索引转为 YYYYMMDD 字符串列表（按时间排序）。

    参数:
        sessions: calendars.sessions_in_range 的返回值

    返回:
        升序排列的日期字符串列表
    """
    out: List[str] = []
    for ts in sessions:
        # 会话标签为交易所本地日历日，取 date 即可与 PostgreSQL date 对齐
        d = pd.Timestamp(ts).date()
        out.append(d.strftime("%Y%m%d"))
    return out


def upsert_trading_calendar_rows(trade_yyyymmdd: List[str]) -> int:
    """
    将交易日写入 trading_calendar；与 trade_date 冲突时更新 is_trading_day 与 day_of_week。

    参数:
        trade_yyyymmdd: YYYYMMDD 字符串列表

    返回:
        成功执行的 upsert 行数
    """
    sql = text(
        """
        INSERT INTO trading_calendar (trade_date, is_trading_day, day_of_week)
        VALUES (:trade_date, TRUE, :day_of_week)
        ON CONFLICT (trade_date)
        DO UPDATE SET
            is_trading_day = EXCLUDED.is_trading_day,
            day_of_week = EXCLUDED.day_of_week
        """
    )
    db = SessionLocal()
    n = 0
    try:
        for s in trade_yyyymmdd:
            d = datetime.strptime(s, "%Y%m%d").date()
            dow = d.weekday()  # 与库中现有数据一致：周一=0
            db.execute(sql, {"trade_date": d, "day_of_week": dow})
            n += 1
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return n


def run_sync(range_start: date, range_end: date, calendar_name: str) -> int:
    """
    从指定交易所日历取出闭区间内的所有交易日并写入数据库。

    参数:
        range_start: 区间起点（含）
        range_end: 区间终点（含）；仅影响可选会话上界，非交易日不会生成行
        calendar_name: exchange_calendars 的日历代码，如 XSHG

    返回:
        upsert 行数

    异常:
        KeyError: 日历代码不存在时由 get_calendar 抛出（包装为友好提示）
    """
    try:
        cal = xcals.get_calendar(calendar_name)
    except Exception as e:
        raise SystemExit(
            f"无法加载日历 {calendar_name!r}，请检查 exchange_calendars 版本及代码是否正确。"
            f" 可用名称示例: {sorted(xcals.get_calendar_names(include_aliases=False))[:8]} ...\n原因: {e}"
        ) from e

    # 使用日期字符串即可；库按会话标签解析，闭区间包含端点上的会话日
    sessions = cal.sessions_in_range(
        pd.Timestamp(range_start),
        pd.Timestamp(range_end),
    )
    ymd_list = sessions_to_yyyymmdd_list(sessions)
    print(
        f"日历 {calendar_name}: {range_start} ~ {range_end} 共 {len(ymd_list)} 个交易日 "
        f"（首 {ymd_list[0] if ymd_list else '-'} 末 {ymd_list[-1] if ymd_list else '-'}）"
    )
    if not ymd_list:
        return 0
    return upsert_trading_calendar_rows(ymd_list)


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    p = argparse.ArgumentParser(
        description="用 exchange_calendars 同步交易所交易日到 trading_calendar"
    )
    p.add_argument(
        "--beg",
        type=str,
        default=None,
        help="起始日期 YYYY-MM-DD，默认 2019-01-01",
    )
    p.add_argument(
        "--end",
        type=str,
        default=None,
        help="结束日期 YYYY-MM-DD（含），默认 2023-01-01",
    )
    p.add_argument(
        "--calendar",
        type=str,
        default=DEFAULT_CALENDAR,
        help=f"exchange_calendars 日历代码，A 股默认 {DEFAULT_CALENDAR}",
    )
    return p.parse_args()


def main() -> None:
    """脚本入口。"""
    args = parse_args()
    beg = datetime.strptime(args.beg, "%Y-%m-%d").date() if args.beg else DEFAULT_BEG
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else DEFAULT_END
    if beg > end:
        raise SystemExit("错误: --beg 不能晚于 --end")

    total = run_sync(beg, end, args.calendar.strip().upper())
    print(f"完成，共 upsert {total} 行。")


if __name__ == "__main__":
    main()
