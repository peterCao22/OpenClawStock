"""
搜狐证券（q.stock.sohu.com）专用数据同步脚本。

本脚本统一负责一切「从搜狐拉取行情并落库」的工作，目标有两类：

1) 指数日线 -> ``index_bar1d``
   - 数据源：搜狐 hisHq 接口（``zs_`` 前缀，如 ``zs_399300`` 沪深300）。
   - 该逻辑由 ``sync_moma_data.py`` 迁移而来，原处不再维护。

2) 个股日线（前复权） -> ``kline_qfq``
   - 数据源：搜狐 hisHq 接口（``cn_`` 前缀，沪深统一前缀，如 ``cn_600600`` / ``cn_000002``）。
   - 关键点：搜狐 hisHq 返回的是「不复权」原始价。本脚本通过搜狐「分红送配记录」页
     （``/cn/{code}/fhsp.shtml``）解析出历次除权除息事件，按「比例前复权法」自算复权因子，
     把不复权价严格还原成前复权价后写入 ``kline_qfq``，保证与历史 Moma 前复权口径一致。
   - 已用 600600 实测：重算前复权 2023-01-03 收盘 = 97.3101，与库内 Moma 值 97.31038 误差 < 0.0003。

量纲对齐（与 kline_qfq 现有数据一致）：
   - 成交量：搜狐返回「手」，库内存「股」，故 ``volume = 搜狐手 * 100``。
   - 成交额：搜狐返回「万元」，库内存「元」，故 ``amount = 搜狐万 * 10000``。
   - 换手率：搜狐返回百分数（如 1.10 表示 1.10%），直接采用。
   - pre_close / change / change_ratio / amplitude：均基于「前复权」序列重算
     （前一交易日前复权收盘），与库内口径一致。

复权一致性与除权处理：
   - 分红送配事件缓存在 ``stock_dividend_event`` / ``stock_dividend_meta`` 表；日常增量优先读库，
     仅在「从未拉取 / --force-fhsp / 分红季(5~10月)且本季尚未刷新」时才请求搜狐 fhsp 页。
   - 增量更新时，若发现「上次入库日 ~ 结束日」之间存在新的除权除息事件，则触发该股
     全量重拉（重新锚定到最新交易日），与 ``update_kline_qfq.py`` 的 detect_ex_dividend 思路一致。
   - 写入前对「与库内重叠日」做收盘价自检，偏差超阈值仅告警，避免送转/配股解析出错污染数据。

数据库写入层复用 ``scripts/update_kline_qfq.py``（upsert / MA 回填 / 月表刷新），避免重复实现。

运行示例：
    # 同步指数（默认三大指数，增量）
    python scripts/sync_sohu_data.py --index
    # 强制全量重拉某指数
    python scripts/sync_sohu_data.py --index --index-instruments 000300.SH --index-full

    # 同步个股（增量），写入前复权到 kline_qfq
    python scripts/sync_sohu_data.py --instruments 600600.SH 000002.SZ
    # 全市场日常增量（分红信息读库，本季未刷新才打 fhsp）
    python scripts/sync_sohu_data.py --all
    # 强制刷新分红缓存并同步
    python scripts/sync_sohu_data.py --all --force-fhsp
    # 个股全量重建（从 --start 起）
    python scripts/sync_sohu_data.py --instruments 600600.SH --stock-full --start 2020-01-01
    # 只算复权、不落库（自检/对账）
    python scripts/sync_sohu_data.py --instruments 600600.SH --dry-run
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd
from sqlalchemy import text

# 让脚本既能 `python scripts/sync_sohu_data.py` 直接跑，也能被作为模块导入
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.db_session import SessionLocal, engine  # noqa: E402
# 复用 kline_qfq 的写入层，保证与 Moma 增量脚本完全一致的口径
from scripts.update_kline_qfq import (  # noqa: E402
    INSERT_COLS,
    delete_instrument_history,
    get_conn,
    get_instruments,
    get_stock_names,
    recalc_derived_fields,
    refresh_monthly_for_instruments,
    upsert_stock,
)

# ─── 常量 ──────────────────────────────────────────────────────────────────────

# 搜狐 hisHq 单次查询约 100 条上限，回补长历史需按窗口分段。窗口取 120 自然日
# （约 80 个交易日 < 100），既安全又能减少请求数。
SOHU_CHUNK_DAYS = 120

# 每次 HTTP 请求间隔（秒），避免触发搜狐风控
SOHU_SLEEP = 0.6
SOHU_MAX_RETRY = 3

# 个股全量重建默认起始日（与 update_kline_qfq 的历史起点保持一致）
DEFAULT_STOCK_START = "2020-01-01"
# 指数同步默认起始日
DEFAULT_INDEX_START = "2023-01-01"

# 前复权自检：重算值与库内值的相对偏差阈值（超过则告警）
QFQ_SELFCHECK_TOL = 0.01  # 1%

# A 股除权除息高峰月份（年报 5~8 月、中报 9~10 月）。仅在这些月份内、且该股本季
# 尚未从搜狐刷新过 fhsp 时，才会发起网络请求；其余时候读 stock_dividend_event 本地缓存。
DIVIDEND_SEASON_MONTHS = {5, 6, 7, 8, 9, 10}

# 库内 instrument -> (搜狐 hisHq 指数代码, 指数中文名)
SOHU_INDEX_MAP: Dict[str, Tuple[str, str]] = {
    "000001.SH": ("zs_000001", "上证指数"),
    "000300.SH": ("zs_399300", "沪深300"),
    "399001.SZ": ("zs_399001", "深证成指"),
}


# ─── 搜狐 HTTP 基础 ────────────────────────────────────────────────────────────

def _sohu_get_text(url: str, params: Optional[dict] = None) -> str:
    """发起 GET 请求并按 GBK 解码返回文本。

    搜狐接口返回内容含中文（GBK 编码），数值部分为 ASCII，整体按 GBK 解码再解析最稳妥。

    参数:
        url:    请求地址。
        params: 查询参数字典。

    返回:
        解码后的响应文本；多次重试仍失败时抛出最后一次异常。
    """
    last_exc: Optional[Exception] = None
    for _ in range(SOHU_MAX_RETRY):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.content.decode("gbk", errors="replace")
        except Exception as exc:  # noqa: BLE001 网络类异常统一重试
            last_exc = exc
            time.sleep(2)
    raise RuntimeError(f"搜狐请求失败: {url} params={params} err={last_exc}")


def _sohu_hisHq_once(code: str, st: str, et: str) -> List[list]:
    """调用一次 hisHq 接口，返回 hq 行列表（单段，受 ~100 条上限约束）。

    参数:
        code: 搜狐代码，个股 ``cn_600600``、指数 ``zs_399300``。
        st / et: 起止日，格式 ``YYYYMMDD``。

    返回:
        ``hq`` 行列表，每行形如
        ``[date, open, close, change, change%, low, high, volume(手), amount(万), turn%]``；
        无数据或状态异常返回空列表。
    """
    url = "https://q.stock.sohu.com/hisHq"
    params = {"code": code, "start": st, "end": et,
              "stat": "1", "order": "A", "period": "d", "rt": "json"}
    txt = _sohu_get_text(url, params)
    data = json.loads(txt)
    if not data or not isinstance(data, list):
        return []
    block = data[0]
    if block.get("status") != 0:
        return []
    return block.get("hq") or []


def fetch_hisHq(code: str, start: datetime.date, end: datetime.date) -> List[list]:
    """分段拉取 hisHq 全区间日线，自动绕过单次 ~100 条上限。

    按 ``SOHU_CHUNK_DAYS`` 自然日窗口滚动请求，按日期去重合并、升序返回。

    参数:
        code:  搜狐代码（``cn_`` / ``zs_`` 前缀）。
        start: 起始日（含）。
        end:   结束日（含）。

    返回:
        合并后的 hq 行列表（按日期升序、去重）。
    """
    merged: Dict[str, list] = {}
    cur = start
    while cur <= end:
        seg_end = min(cur + datetime.timedelta(days=SOHU_CHUNK_DAYS - 1), end)
        rows = _sohu_hisHq_once(code, cur.strftime("%Y%m%d"), seg_end.strftime("%Y%m%d"))
        for r in rows:
            if r and r[0]:
                merged[r[0]] = r  # 以日期为键去重（分段边界可能重叠）
        time.sleep(SOHU_SLEEP)
        cur = seg_end + datetime.timedelta(days=1)
    return [merged[k] for k in sorted(merged.keys())]


def _to_f(x) -> Optional[float]:
    """把搜狐字段安全转 float（去千分位逗号/百分号），失败返回 None。"""
    try:
        return float(str(x).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None


# ─── 指数同步（由 sync_moma_data 迁移） ─────────────────────────────────────────

def _get_index_bar_max_date(db, instrument: str) -> Optional[datetime.date]:
    """查询 index_bar1d 中某指数已入库的最大日期，无数据返回 None。"""
    row = db.execute(
        text("SELECT MAX(date) FROM index_bar1d WHERE instrument = :inst"),
        {"inst": instrument},
    ).fetchone()
    return row[0] if row and row[0] else None


def _sohu_row_to_index_row(row: list, instrument: str, name: str,
                           created_at: datetime.date) -> Optional[dict]:
    """搜狐 hq 单行 -> index_bar1d 行字典。

    行字段顺序：``[date, open, close, change, change%, low, high, volume(手), amount(万), turn]``。
    指数无复权问题，直接采用原值；``pre_close`` 由 ``close - change`` 反推。
    """
    try:
        trade_date = datetime.datetime.strptime(row[0], "%Y-%m-%d").date()
    except (ValueError, IndexError, TypeError):
        return None
    close = _to_f(row[2])
    change = _to_f(row[3])
    pre_close = round(close - change, 4) if (close is not None and change is not None) else None
    rati = _to_f(row[4])
    rati = rati / 100.0 if rati is not None else None
    volume = None
    try:
        volume = int(float(row[7]))   # 指数成交量为整数手，与库内量纲一致
    except (TypeError, ValueError, IndexError):
        pass
    return {
        "date": trade_date,
        "instrument": instrument,
        "name": name,
        "pre_close": pre_close,
        "open": _to_f(row[1]),
        "high": _to_f(row[6]),
        "low": _to_f(row[5]),
        "close": close,
        "volume": volume,
        "amount": _to_f(row[8]) if len(row) > 8 else None,   # 成交额（万元）
        "change": change,
        "change_rati": rati,
        "created_at": created_at,
    }


def sync_index_bar1d(
    instruments: Optional[List[str]] = None,
    start_date_str: str = DEFAULT_INDEX_START,
    end_date_str: Optional[str] = None,
    full: bool = False,
) -> None:
    """从搜狐 hisHq 同步指数日线到 ``index_bar1d``。

    默认同步 上证 000001.SH / 沪深300 000300.SH / 深证成指 399001.SZ。库中已有数据时
    从 ``MAX(date)+1`` 增量拉取；采用「区间 DELETE 再 INSERT」幂等写法，可重复执行。

    参数:
        instruments:    instrument 列表（如 ``["000300.SH"]``）；None 时同步全部三个。
        start_date_str: 首次/全量回填起始日 ``YYYY-MM-DD``。
        end_date_str:   结束日，默认今天。
        full:           为 True 时忽略库内 MAX(date)，从 ``start_date_str`` 全量重拉覆盖。
    """
    if not end_date_str:
        end_date_str = datetime.date.today().strftime("%Y-%m-%d")
    sync_end = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    default_start = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
    targets = instruments or list(SOHU_INDEX_MAP.keys())
    created_at = datetime.date.today()

    insert_sql = text("""
        INSERT INTO index_bar1d
            (date, instrument, name, pre_close, open, high, low, close,
             volume, amount, change, change_rati, created_at)
        VALUES
            (:date, :instrument, :name, :pre_close, :open, :high, :low, :close,
             :volume, :amount, :change, :change_rati, :created_at)
    """)
    delete_sql = text("""
        DELETE FROM index_bar1d
        WHERE instrument = :inst AND date >= :d0 AND date <= :d1
    """)

    db = SessionLocal()
    try:
        for instrument in targets:
            if instrument not in SOHU_INDEX_MAP:
                print(f"[index] 未知指数 {instrument}，跳过（可在 SOHU_INDEX_MAP 中补充）。")
                continue
            sohu_code, name = SOHU_INDEX_MAP[instrument]
            max_date = _get_index_bar_max_date(db, instrument)
            if not full and max_date and max_date >= sync_end:
                print(f"[index] {instrument} ({name}) 已至 {max_date}，无需更新。")
                continue
            if full:
                sync_start = default_start
            else:
                sync_start = (max(default_start, max_date + datetime.timedelta(days=1))
                              if max_date else default_start)
            if sync_start > sync_end:
                print(f"[index] {instrument} ({name}) 起始 {sync_start} 晚于结束 {sync_end}，跳过。")
                continue

            print(f"[index] 拉取 {instrument} ({name}) {sync_start} ~ {sync_end} ...")
            raw = fetch_hisHq(sohu_code, sync_start, sync_end)
            rows = [r for r in (_sohu_row_to_index_row(x, instrument, name, created_at)
                                for x in raw) if r]
            if not rows:
                print(f"[index] {instrument} 无数据/解析 0 行，跳过。")
                continue

            d0 = min(r["date"] for r in rows)
            d1 = max(r["date"] for r in rows)
            db.execute(delete_sql, {"inst": instrument, "d0": d0, "d1": d1})
            for row in rows:
                db.execute(insert_sql, row)
            db.commit()
            print(f"[index] {instrument} ({name}) 写入 {len(rows)} 行（区间 {d0} ~ {d1}）。")
        print("[index] 指数日线同步完成。")
    finally:
        db.close()


# ─── 分红送配本地缓存（stock_dividend_event / stock_dividend_meta） ─────────────

CREATE_DIVIDEND_META_SQL = """
CREATE TABLE IF NOT EXISTS stock_dividend_meta (
    instrument   VARCHAR(20) PRIMARY KEY,
    fetched_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""
CREATE_DIVIDEND_EVENT_SQL = """
CREATE TABLE IF NOT EXISTS stock_dividend_event (
    instrument     VARCHAR(20) NOT NULL,
    ex_date        DATE NOT NULL,
    scheme_text    TEXT,
    cash_per_10    NUMERIC(12,4) DEFAULT 0,
    bonus_per_10   NUMERIC(12,4) DEFAULT 0,
    rights_per_10  NUMERIC(12,4) DEFAULT 0,
    rights_px      NUMERIC(12,4) DEFAULT 0,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instrument, ex_date)
);
"""
CREATE_DIVIDEND_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_dividend_event_inst
    ON stock_dividend_event(instrument);
"""


def ensure_dividend_tables(conn) -> None:
    """幂等创建分红缓存表（事件表 + 元数据表）。"""
    with conn.cursor() as cur:
        cur.execute(CREATE_DIVIDEND_META_SQL)
        cur.execute(CREATE_DIVIDEND_EVENT_SQL)
        cur.execute(CREATE_DIVIDEND_INDEX_SQL)
    conn.commit()


def current_dividend_season_start(ref: datetime.date) -> Optional[datetime.date]:
    """若 ref 处于分红季(5~10月)，返回当年 5-01；否则返回 None（非分红季无需刷新 fhsp）。"""
    if ref.month in DIVIDEND_SEASON_MONTHS:
        return datetime.date(ref.year, 5, 1)
    return None


def _get_dividend_fetched_at(conn, instrument: str) -> Optional[datetime.datetime]:
    """读取该股分红元数据上次从搜狐拉取时间；无记录返回 None。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT fetched_at FROM stock_dividend_meta WHERE instrument = %s",
            (instrument,),
        )
        row = cur.fetchone()
    if row and row[0]:
        return row[0]
    return None


def _count_dividend_events(conn, instrument: str) -> int:
    """统计该股本地缓存的除权事件条数。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM stock_dividend_event WHERE instrument = %s",
            (instrument,),
        )
        row = cur.fetchone()
    return int(row[0]) if row else 0


def should_refresh_fhsp(
    conn,
    instrument: str,
    force_fhsp: bool,
    ref_date: datetime.date,
) -> bool:
    """判断是否需从搜狐重新拉取 fhsp 分红页。

    仅在以下情况返回 True：
        - ``force_fhsp`` 强制刷新；
        - 本地从未缓存过（无 meta 或事件数为 0）；
        - 当前处于分红季，且 ``fetched_at`` 早于本季起始日（5-01），即本季尚未刷新过。

    非分红季且本地已有缓存 -> False（纯读库，零 fhsp 请求）。
    """
    if force_fhsp:
        return True
    fetched_at = _get_dividend_fetched_at(conn, instrument)
    if fetched_at is None or _count_dividend_events(conn, instrument) == 0:
        return True
    season_start = current_dividend_season_start(ref_date)
    if season_start is None:
        return False
    # fetched_at 可能是 datetime；与本季 5-01 比较
    fa = fetched_at.date() if isinstance(fetched_at, datetime.datetime) else fetched_at
    return fa < season_start


def load_dividends_from_db(conn, instrument: str) -> List["DivEvent"]:
    """从 ``stock_dividend_event`` 读取除权事件并转为 ``DivEvent`` 列表（升序）。"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ex_date, cash_per_10, bonus_per_10, rights_per_10, rights_px, scheme_text
            FROM stock_dividend_event
            WHERE instrument = %s
            ORDER BY ex_date
            """,
            (instrument,),
        )
        rows = cur.fetchall()
    events: List[DivEvent] = []
    for ex_date, c10, b10, r10, rpx, _scheme in rows:
        events.append(DivEvent(
            ex_date=ex_date,
            cash=float(c10 or 0) / 10.0,
            bonus=float(b10 or 0) / 10.0,
            rights=float(r10 or 0) / 10.0,
            rights_px=float(rpx or 0),
        ))
    return events


def save_dividends_to_db(conn, instrument: str, events: List["DivEvent"],
                         scheme_by_date: Dict[datetime.date, str]) -> None:
    """将除权事件 upsert 到 ``stock_dividend_event``，并更新 ``stock_dividend_meta.fetched_at``。

    采用 DELETE 该股全部旧事件再 INSERT 新列表，保证与搜狐页全量一致（事件可能被修正/补录）。
    """
    now = datetime.datetime.now()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM stock_dividend_event WHERE instrument = %s", (instrument,))
        for ev in events:
            c10 = round(ev.cash * 10, 4)
            b10 = round(ev.bonus * 10, 4)
            r10 = round(ev.rights * 10, 4)
            cur.execute(
                """
                INSERT INTO stock_dividend_event
                    (instrument, ex_date, scheme_text, cash_per_10, bonus_per_10,
                     rights_per_10, rights_px, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    instrument, ev.ex_date,
                    scheme_by_date.get(ev.ex_date, ""),
                    c10, b10, r10, ev.rights_px, now,
                ),
            )
        cur.execute(
            """
            INSERT INTO stock_dividend_meta (instrument, fetched_at)
            VALUES (%s, %s)
            ON CONFLICT (instrument) DO UPDATE SET fetched_at = EXCLUDED.fetched_at
            """,
            (instrument, now),
        )
    conn.commit()


def get_dividends_for_instrument(
    conn,
    instrument: str,
    force_fhsp: bool,
    ref_date: datetime.date,
    verbose: bool = False,
) -> List["DivEvent"]:
    """获取某股除权事件：优先读库，按需刷新搜狐 fhsp 后写回库。

    参数:
        conn:         psycopg2 连接（用于读写分红缓存表）。
        instrument:   库内代码，如 ``600600.SH``。
        force_fhsp:   是否强制从搜狐重拉。
        ref_date:     参考日（通常 sync_end），用于判断本季是否已刷新。
        verbose:      是否打印缓存命中/刷新日志。

    返回:
        ``DivEvent`` 升序列表。
    """
    ensure_dividend_tables(conn)
    if should_refresh_fhsp(conn, instrument, force_fhsp, ref_date):
        code6 = instrument.split(".")[0]
        events, schemes = _fetch_dividends_from_sohu(code6)
        time.sleep(SOHU_SLEEP)
        save_dividends_to_db(conn, instrument, events, schemes)
        if verbose:
            print(f"[div] {instrument} 已从搜狐刷新 {len(events)} 条分红事件")
        return events
    events = load_dividends_from_db(conn, instrument)
    if verbose and events:
        print(f"[div] {instrument} 读库缓存 {len(events)} 条分红事件")
    return events


# ─── 分红送配解析 + 前复权因子 ─────────────────────────────────────────────────

class DivEvent:
    """单个除权除息事件（已按「每10股」口径归一到每股）。

    属性:
        ex_date:    除权除息日。
        cash:       每股现金红利（元）= 每10股派息 / 10。
        bonus:      每股送转股数 = (每10股送股 + 转增) / 10。
        rights:     每股配股数 = 每10股配股 / 10。
        rights_px:  配股价（元/股），无配股时为 0。
    """

    __slots__ = ("ex_date", "cash", "bonus", "rights", "rights_px")

    def __init__(self, ex_date: datetime.date, cash: float = 0.0, bonus: float = 0.0,
                 rights: float = 0.0, rights_px: float = 0.0):
        self.ex_date = ex_date
        self.cash = cash
        self.bonus = bonus
        self.rights = rights
        self.rights_px = rights_px

    def coef(self, prev_close: float) -> float:
        """给定除权前一交易日「不复权收盘价」，返回该事件的复权系数。

        比例前复权法的除权参考价公式：
            参考价 = (前收盘 - 每股现金红利 + 配股价 * 每股配股数) / (1 + 每股送转 + 每股配股)
        复权系数 = 参考价 / 前收盘。无效输入时返回 1.0（不调整）。
        """
        if not prev_close or prev_close <= 0:
            return 1.0
        ref = (prev_close - self.cash + self.rights_px * self.rights) / (1.0 + self.bonus + self.rights)
        return ref / prev_close


def _parse_scheme_text(s: str) -> Tuple[float, float, float, float]:
    """解析分配方案中文文案，返回 (每10股送转股, 每10股派息元, 每10股配股, 配股价)。

    支持文案样例：
        "每10股派息1.66元"、"每10股送3股转增2股派5元(税后4.5元)"、"每10股配3股 配股价6.5元"。
    现金红利取税前数值；多笔同日记录在上层按日期累加。
    """
    s = s.replace("（", "(").replace("）", ")")
    song = sum(float(x) for x in re.findall(r"送\s*([\d.]+)\s*股", s))
    song += sum(float(x) for x in re.findall(r"转增?\s*([\d.]+)\s*股", s))
    # 派息：优先匹配「派(息)X元」，忽略括号内税后数值（税后另写在括号里）
    cash = 0.0
    m = re.search(r"派(?:息)?\s*([\d.]+)\s*元", s)
    if m:
        cash = float(m.group(1))
    peigu = 0.0
    mp = re.search(r"配\s*([\d.]+)\s*股", s)
    if mp:
        peigu = float(mp.group(1))
    peigu_px = 0.0
    mpx = re.search(r"配股价\s*([\d.]+)\s*元", s)
    if mpx:
        peigu_px = float(mpx.group(1))
    return song, cash, peigu, peigu_px


def _fetch_dividends_from_sohu(code6: str) -> Tuple[List[DivEvent], Dict[datetime.date, str]]:
    """从搜狐抓取并解析分红送配页，返回除权事件列表与 {ex_date: 方案文案}。

    页面：``https://q.stock.sohu.com/cn/{code6}/fhsp.shtml``（静态 HTML，GBK 编码）。
    只取「分红记录」表中含「除权除息日」的行，天然排除顶部「分配预案」。

    参数:
        code6: 6 位股票代码（不含交易所后缀），如 "600600"。

    返回:
        (``DivEvent`` 升序列表, 方案文案字典)。抓取失败返回 ([], {})。
    """
    url = f"https://q.stock.sohu.com/cn/{code6}/fhsp.shtml"
    try:
        html = _sohu_get_text(url)
    except Exception as exc:  # noqa: BLE001
        print(f"[div] {code6} 分红页抓取失败: {exc}")
        return [], {}

    pattern = re.compile(
        r"除权除息日</td>\s*<td[^>]*>\s*(\d{4}-\d{2}-\d{2})\s*</td>\s*<td[^>]*>(.*?)</td>",
        re.S,
    )
    agg: Dict[datetime.date, DivEvent] = {}
    schemes: Dict[datetime.date, str] = {}
    for m in pattern.finditer(html):
        try:
            ex_date = datetime.datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            continue
        scheme = re.sub(r"<[^>]+>", "", m.group(2)).strip()
        song, cash, peigu, peigu_px = _parse_scheme_text(scheme)
        if song == 0 and cash == 0 and peigu == 0:
            continue
        ev = agg.get(ex_date)
        if ev is None:
            agg[ex_date] = DivEvent(ex_date, cash / 10.0, song / 10.0, peigu / 10.0, peigu_px)
            schemes[ex_date] = scheme
        else:
            ev.cash += cash / 10.0
            ev.bonus += song / 10.0
            ev.rights += peigu / 10.0
            if peigu_px:
                ev.rights_px = peigu_px
            schemes[ex_date] = schemes.get(ex_date, "") + ";" + scheme
    events = [agg[k] for k in sorted(agg.keys())]
    return events, schemes


def build_qfq_factor_map(events: List[DivEvent],
                         close_by_date: Dict[datetime.date, float]) -> Dict[datetime.date, float]:
    """计算「前复权因子」对每个除权事件的贡献，返回 {事件日: 累计系数} 的辅助结构。

    前复权（锚定到最新交易日）满足：
        factor(date) = ∏ { coef(e) : e.ex_date > date }
    本函数为每个事件 e 计算其 coef（需要 e 除权日「前一交易日」的不复权收盘），
    返回 [(ex_date, coef)] 供 ``qfq_factor_at`` 按日期求后缀乘积。

    参数:
        events:        升序事件列表。
        close_by_date: {交易日: 不复权收盘}，用于取除权日前一交易日收盘。

    返回:
        {ex_date: coef} 字典（仅含 coef≠1 的有效事件）。
    """
    dates_sorted = sorted(close_by_date.keys())
    coef_map: Dict[datetime.date, float] = {}
    for ev in events:
        # 找除权日「前一个有行情的交易日」收盘
        prev_close = None
        # 二分/线性找最近一个 < ex_date 的交易日
        lo, hi = 0, len(dates_sorted) - 1
        idx = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            if dates_sorted[mid] < ev.ex_date:
                idx = mid
                lo = mid + 1
            else:
                hi = mid - 1
        if idx >= 0:
            prev_close = close_by_date[dates_sorted[idx]]
        if prev_close:
            c = ev.coef(prev_close)
            if abs(c - 1.0) > 1e-9:
                coef_map[ev.ex_date] = c
    return coef_map


def qfq_factor_at(date: datetime.date, coef_map: Dict[datetime.date, float]) -> float:
    """返回某交易日的前复权因子 = 所有「除权日 > date」事件系数之积。"""
    f = 1.0
    for ex_date, c in coef_map.items():
        if ex_date > date:
            f *= c
    return f


# ─── 个股前复权落库 ────────────────────────────────────────────────────────────

def to_sohu_stock_code(instrument: str) -> str:
    """库内 instrument（``600600.SH`` / ``000002.SZ``）-> 搜狐个股代码 ``cn_600600``。"""
    return "cn_" + instrument.split(".")[0]


def _build_qfq_df(instrument: str, name: Optional[str], raw_rows: List[list],
                  coef_map: Dict[datetime.date, float],
                  keep_from: datetime.date) -> pd.DataFrame:
    """把搜狐不复权行 + 复权因子 -> 符合 kline_qfq 写入口径的 DataFrame。

    参数:
        instrument: 库内代码。
        name:       股票名（来自 stock_list）。
        raw_rows:   搜狐 hq 行（升序，含 keep_from 之前的少量回溯行以提供首行 pre_close）。
        coef_map:   除权事件系数表（用于逐日求前复权因子）。
        keep_from:  实际入库的起始日（回溯行不写库，仅用于算 pre_close）。

    返回:
        含 INSERT_COLS 所需列的 DataFrame（仅保留 date >= keep_from 的行）。
    """
    recs = []
    for r in raw_rows:
        try:
            d = datetime.datetime.strptime(r[0], "%Y-%m-%d").date()
        except (ValueError, IndexError, TypeError):
            continue
        o, c, low, high = _to_f(r[1]), _to_f(r[2]), _to_f(r[5]), _to_f(r[6])
        vol_hand = _to_f(r[7])
        amt_wan = _to_f(r[8]) if len(r) > 8 else None
        turn = _to_f(r[9]) if len(r) > 9 else None
        f = qfq_factor_at(d, coef_map)
        recs.append({
            "date": d,
            # 前复权 = 不复权 * 因子（量价中只有 OHLC 需复权；量/额/换手率为实际成交，不复权）
            "open": round(o * f, 4) if o is not None else None,
            "high": round(high * f, 4) if high is not None else None,
            "low": round(low * f, 4) if low is not None else None,
            "close": round(c * f, 4) if c is not None else None,
            "volume": int(round(vol_hand * 100)) if vol_hand is not None else None,  # 手->股
            "amount": round(amt_wan * 10000, 2) if amt_wan is not None else None,    # 万->元
            "turn": round(turn, 4) if turn is not None else None,
        })
    df = pd.DataFrame(recs).sort_values("date").reset_index(drop=True)
    if df.empty:
        return df

    # pre_close = 前一交易日前复权收盘；首行因有回溯行而能取到
    df["pre_close"] = df["close"].shift(1)
    df["change"] = df["close"] - df["pre_close"]
    df["change_ratio"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100
    df["amplitude"] = (df["high"] - df["low"]) / df["pre_close"] * 100

    # 留空列（与 update_kline_qfq 一致，MA 由 SQL 回填、涨跌停/笔数暂不提供）
    df["name"] = name
    df["deal_number"] = None
    df["upper_limit"] = None
    df["lower_limit"] = None
    df["is_limit_up"] = None
    for col in ("ma5", "ma10", "ma20", "ma60"):
        df[col] = None

    # 数值列四舍五入
    for col in ("change", "change_ratio", "amplitude"):
        df[col] = df[col].round(4)

    df = df[df["date"] >= keep_from].reset_index(drop=True)
    return df


def _self_check_qfq(instrument: str, df: pd.DataFrame) -> None:
    """对 df 与库内 kline_qfq 重叠日的收盘价做抽样自检，偏差超阈值则告警。"""
    if df.empty:
        return
    d0, d1 = df["date"].min(), df["date"].max()
    db_df = pd.read_sql(
        text("SELECT date, close FROM kline_qfq WHERE instrument=:i AND date>=:d0 AND date<=:d1"),
        engine, params={"i": instrument, "d0": d0, "d1": d1},
    )
    if db_df.empty:
        return
    db_map = {pd.Timestamp(r.date).date(): float(r.close) for r in db_df.itertuples()}
    bad = 0
    checked = 0
    for r in df.itertuples():
        old = db_map.get(r.date)
        if old and r.close:
            checked += 1
            if abs(r.close - old) / old > QFQ_SELFCHECK_TOL:
                bad += 1
                if bad <= 3:
                    print(f"[selfcheck] {instrument} {r.date} 重算前复权={r.close} 库内={old} 偏差>{QFQ_SELFCHECK_TOL:.0%}")
    if checked:
        flag = "⚠️ 偏差较多，请检查送转/配股解析" if bad > max(2, checked * 0.1) else "OK"
        print(f"[selfcheck] {instrument} 重叠 {checked} 日，异常 {bad} 日 -> {flag}")


def _get_kline_max_date(conn, instrument: str) -> Optional[datetime.date]:
    """查询 kline_qfq 中某股已入库的最大日期，无数据返回 None。"""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date) FROM kline_qfq WHERE instrument = %s", (instrument,))
        row = cur.fetchone()
    return row[0] if row and row[0] else None


def sync_stock_kline(
    instruments: List[str],
    start_date_str: str = DEFAULT_STOCK_START,
    end_date_str: Optional[str] = None,
    full: bool = False,
    dry_run: bool = False,
    force_fhsp: bool = False,
) -> None:
    """从搜狐同步个股前复权日线到 ``kline_qfq``。

    流程（逐股）：
        1. 取库内 MAX(date) 决定增量起点（full=True 或无历史则从 start 全量）。
        2. 视需要解析分红送配（见下），若「上次入库日~结束日」存在新除权事件 -> 升级为全量重建。
        3. 拉取不复权日线（含少量回溯行以提供首行 pre_close），按因子还原前复权。
        4. 自检重叠日收盘价，dry_run 仅打印；否则 upsert + MA 回填 + 月表刷新。

    fhsp 查询策略（分红缓存）：
        - 除权事件持久化在 ``stock_dividend_event``；元数据 ``stock_dividend_meta.fetched_at`` 记录上次搜狐拉取时间。
        - 日常增量默认读库；仅当「从未缓存 / --force-fhsp / 分红季且本季尚未刷新」时才请求 fhsp 并写回库。
        - 非分红季且本地已有缓存 -> 零 fhsp 网络请求。

    参数:
        instruments:    库内代码列表，如 ["600600.SH", "000002.SZ"]。
        start_date_str: 全量/首次回填起始日 ``YYYY-MM-DD``。
        end_date_str:   结束日，默认今天。
        full:           强制全量重建。
        dry_run:        只算不写库。
        force_fhsp:     强制从搜狐重拉分红页并更新缓存。
    """
    if not end_date_str:
        end_date_str = datetime.date.today().strftime("%Y-%m-%d")
    sync_end = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    base_start = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
    name_map = get_stock_names(engine)

    total = len(instruments)
    verbose = total <= 50  # 少量时逐股打印；全市场时只打进度与汇总

    conn = get_conn()
    ensure_dividend_tables(conn)
    updated: List[str] = []
    n_write = n_skip = n_rebuild = n_fail = n_div_refresh = 0
    t0 = time.time()
    try:
        for i, instrument in enumerate(instruments):
            try:
                name = name_map.get(instrument)
                sohu_code = to_sohu_stock_code(instrument)
                max_date = _get_kline_max_date(conn, instrument)

                do_full = full or (max_date is None)
                # 增量且已到最新 -> 直接跳过（无需读分红/行情）
                if not do_full and max_date is not None and max_date >= sync_end:
                    n_skip += 1
                    if verbose:
                        print(f"[stock] {instrument} ({name}) 已至 {max_date}，无需更新。")
                    continue

                # 分红事件：读库或按需刷新搜狐 fhsp
                need_refresh_before = should_refresh_fhsp(conn, instrument, force_fhsp, sync_end)
                events = get_dividends_for_instrument(
                    conn, instrument, force_fhsp, sync_end, verbose=verbose,
                )
                if need_refresh_before:
                    n_div_refresh += 1

                if not do_full and max_date is not None and events:
                    if any(max_date < ev.ex_date <= sync_end for ev in events):
                        if verbose:
                            print(f"[stock] {instrument} 检测到 {max_date} 之后有除权事件 -> 全量重建")
                        do_full = True

                keep_from = base_start if do_full else (max_date + datetime.timedelta(days=1))
                if keep_from > sync_end:
                    n_skip += 1
                    if verbose:
                        print(f"[stock] {instrument} ({name}) 已至 {max_date}，无需更新。")
                    continue

                # 多取 ~15 自然日回溯行，确保首行 pre_close 可由前一交易日推出
                fetch_start = keep_from - datetime.timedelta(days=15)
                if verbose:
                    print(f"[stock] 拉取 {instrument} ({name}) {keep_from} ~ {sync_end} "
                          f"(full={do_full}, 分红事件 {len(events)} 条) ...")
                raw_rows = fetch_hisHq(sohu_code, fetch_start, sync_end)
                if not raw_rows:
                    n_fail += 1
                    if verbose:
                        print(f"[stock] {instrument} 无行情数据，跳过。")
                    continue

                close_by_date = {}
                for r in raw_rows:
                    try:
                        close_by_date[datetime.datetime.strptime(r[0], "%Y-%m-%d").date()] = _to_f(r[2])
                    except (ValueError, IndexError, TypeError):
                        continue
                coef_map = build_qfq_factor_map(events, close_by_date)

                df = _build_qfq_df(instrument, name, raw_rows, coef_map, keep_from)
                if df.empty:
                    n_skip += 1
                    continue

                if verbose:
                    _self_check_qfq(instrument, df)

                if dry_run:
                    if verbose:
                        print(f"[stock] {instrument} dry-run：{len(df)} 行"
                              f"（{df['date'].min()} ~ {df['date'].max()}），末行 close={df['close'].iloc[-1]}")
                    continue

                if do_full:
                    delete_instrument_history(conn, instrument)
                    n_rebuild += 1
                upsert_stock(conn, instrument, df, name, dry_run=False)
                updated.append(instrument)
                n_write += 1
                if verbose:
                    print(f"[stock] {instrument} ({name}) 写入 {len(df)} 行。")
            except Exception as exc:  # noqa: BLE001 单股失败不阻塞整体
                n_fail += 1
                print(f"[stock] {instrument} 处理失败: {exc}")

            if not verbose and (i + 1) % 100 == 0:
                print(f"[stock] 进度 {i+1}/{total} | 写入 {n_write} 重建 {n_rebuild} "
                      f"跳过 {n_skip} 失败 {n_fail} | 用时 {time.time()-t0:.0f}s")

        if not dry_run and updated:
            recalc_derived_fields(conn, updated)
            refresh_monthly_for_instruments(updated)
        print(f"[stock] 完成：写入 {n_write}（重建 {n_rebuild}），跳过 {n_skip}，失败 {n_fail}，"
              f"分红刷新 {n_div_refresh}，共 {total} 只，用时 {time.time()-t0:.0f}s"
              + ("" if dry_run else "，MA/月表已刷新。"))
    finally:
        conn.close()


# ─── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="搜狐证券数据同步（指数 + 个股前复权）")
    # 指数
    parser.add_argument("--index", action="store_true", help="同步指数日线到 index_bar1d")
    parser.add_argument("--index-instruments", nargs="+", default=None,
                        help="限定指数（如 000001.SH 399001.SZ），默认全部三个")
    parser.add_argument("--index-full", action="store_true",
                        help="指数全量重拉（忽略库内 MAX(date)，用于修数）")
    # 个股
    parser.add_argument("--instruments", nargs="+", default=None,
                        help="同步个股前复权到 kline_qfq（库内代码，如 600600.SH 000002.SZ）")
    parser.add_argument("--all", action="store_true",
                        help="同步 stock_list 全市场个股（与 --instruments 二选一）")
    parser.add_argument("--limit", type=int, default=None, help="配合 --all：限制处理数量（分批）")
    parser.add_argument("--offset", type=int, default=0, help="配合 --all：跳过前 N 只（分批）")
    parser.add_argument("--stock-full", action="store_true", help="个股全量重建（从 --start）")
    parser.add_argument("--force-fhsp", action="store_true",
                        help="强制从搜狐重拉分红页并更新 stock_dividend_event 缓存")
    parser.add_argument("--dry-run", action="store_true", help="个股只算不写库（对账/自检）")
    # 公共
    parser.add_argument("--start", type=str, default=None, metavar="YYYY-MM-DD",
                        help="起始日；个股默认 2020-01-01，指数默认 2023-01-01")
    parser.add_argument("--end", type=str, default=None, metavar="YYYY-MM-DD", help="结束日，默认今天")
    args = parser.parse_args()

    if not args.index and not args.instruments and not args.all:
        parser.error("请至少指定 --index、--instruments 或 --all 之一")

    if args.index:
        sync_index_bar1d(
            instruments=args.index_instruments,
            start_date_str=args.start or DEFAULT_INDEX_START,
            end_date_str=args.end,
            full=args.index_full,
        )

    # 个股目标：--all 取 stock_list 全市场（支持 --limit/--offset 分批）；否则用 --instruments
    stock_targets: Optional[List[str]] = None
    if args.all:
        stock_targets = get_instruments(engine, limit=args.limit, offset=args.offset)
        print(f"[stock] --all 模式：stock_list 命中 {len(stock_targets)} 只"
              f"（offset={args.offset}, limit={args.limit}）")
    elif args.instruments:
        stock_targets = args.instruments

    if stock_targets:
        sync_stock_kline(
            instruments=stock_targets,
            start_date_str=args.start or DEFAULT_STOCK_START,
            end_date_str=args.end,
            full=args.stock_full,
            dry_run=args.dry_run,
            force_fhsp=args.force_fhsp,
        )


if __name__ == "__main__":
    main()
