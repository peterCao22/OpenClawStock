import sys
import os
import argparse
import traceback
from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
import datetime

# 将项目根目录加入 sys.path 以便导入 scripts 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.db_session import engine, SessionLocal, Base
from scripts.models import CategoryTree, StockBasic, FinancialIndex, LimitUpPool, StockCategoryMapping, LimitDownPool, TechnicalMacd, TechnicalMa, TechnicalBoll, TechnicalKdj, StockList
from scripts.moma_api_client import MomaApiClient

# 默认同步的指数：instrument 须大写后缀 .SH（与 index_bar1d 存量口径一致）
DEFAULT_INDEX_INSTRUMENTS = [
    ("000001.SH", "上证指数"),
    ("000300.SH", "沪深300"),
]

# 需修复 id 自增/主键的 K 线及业务表（表名白名单，禁止外部传入任意标识符）
FIX_ID_SCHEMA_TABLES = [
    "index_bar1d",
    "concept_bar1d",
    "concept_component",
    "limit_up_pool",
]

def sync_stock_list():
    """同步股票列表 (stock_list)"""
    print("Starting sync of stock list...")
    client = MomaApiClient()
    db = SessionLocal()
    
    try:
        data = client.get_stock_list()
        if not data:
            print("No stock list data fetched.")
            return

        print(f"Fetched {len(data)} stocks from API.")
        
        count = 0
        for item in data:
            dm = item.get("dm")
            mc = item.get("mc")
            jys = item.get("jys")
            
            if not dm or not jys:
                continue
                
            # 构建 instrument
            suffix = ""
            if jys == "sh":
                suffix = ".SH"
            elif jys == "sz":
                suffix = ".SZ"
            elif jys == "bj":
                suffix = ".BJ"
            else:
                # 未知交易所，跳过或记录
                continue
                
            instrument = f"{dm}{suffix}"
            
            stmt = insert(StockList).values(
                instrument=instrument,
                name=mc
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['instrument'],
                set_={
                    "name": stmt.excluded.name
                }
            )
            db.execute(stmt)
            count += 1
            
            if count % 1000 == 0:
                print(f"Processed {count} stocks...")
                db.commit()
                
        db.commit()
        print(f"Successfully synced {count} stocks to stock_list.")
        
    except Exception as e:
        db.rollback()
        print(f"Error syncing stock list: {e}")
        traceback.print_exc()
    finally:
        db.close()

def sync_category_tree():
    """同步行业、指数、概念树"""
    print("开始同步指数、行业、概念树...")
    client = MomaApiClient()
    db = SessionLocal()
    try:
        data = client.get_category_tree()
        if not data:
            print("未获取到分类树数据。")
            return

        for item in data:
            stmt = insert(CategoryTree).values(
                code=item.get("code"),
                name=item.get("name"),
                type1=item.get("type1"),
                type2=item.get("type2"),
                level=item.get("level"),
                pcode=item.get("pcode"),
                pname=item.get("pname"),
                isleaf=item.get("isleaf")
            )
            # 冲突时更新
            stmt = stmt.on_conflict_do_update(
                index_elements=['code'],
                set_={
                    "name": stmt.excluded.name,
                    "type1": stmt.excluded.type1,
                    "type2": stmt.excluded.type2,
                    "level": stmt.excluded.level,
                    "pcode": stmt.excluded.pcode,
                    "pname": stmt.excluded.pname,
                    "isleaf": stmt.excluded.isleaf
                }
            )
            db.execute(stmt)
        db.commit()
        print(f"成功同步 {len(data)} 条分类树记录。")
    except Exception as e:
        db.rollback()
        print(f"同步分类树出错: {e}")
        traceback.print_exc()
    finally:
        db.close()

def check_and_update_schema():
    """检查并更新数据库表结构 (简单的迁移)"""
    from sqlalchemy import text
    db = SessionLocal()
    try:
        # 检查 moma_stock_basic 表是否存在 updated_at 字段
        result = db.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='moma_stock_basic' AND column_name='updated_at'"))
        if not result.fetchone():
            print("Adding 'updated_at' column to moma_stock_basic table...")
            db.execute(text("ALTER TABLE moma_stock_basic ADD COLUMN updated_at DATE"))
            db.commit()
            print("Column added.")
    except Exception as e:
        print(f"Schema check failed: {e}")
        db.rollback()
    finally:
        db.close()

def sync_stock_basic(skip_existing=False):
    """同步股票基础信息"""
    print("Starting sync of stock basic info...")
    
    check_and_update_schema()
    
    client = MomaApiClient()
    db = SessionLocal()
    
    # 从本地数据库获取所有股票代码
    from sqlalchemy import text
    try:
        # 获取所有股票代码，确保顺序一致
        result = db.execute(text("SELECT instrument FROM stock_list ORDER BY instrument")).fetchall()
        stock_codes = [row[0] for row in result]
        print(f"Total stocks in list: {len(stock_codes)}")
        
        existing_records = {}
        if skip_existing:
            print("Fetching existing records to skip...")
            # 获取 ii 和 updated_at
            existing_result = db.execute(text("SELECT ii, updated_at FROM moma_stock_basic")).fetchall()
            existing_records = {row[0]: row[1] for row in existing_result}
            print(f"Found {len(existing_records)} existing records.")

        today = datetime.date.today()
        count = 0
        skipped = 0
        
        for code in stock_codes:
            # 提取数字代码用于比对
            pure_code = code.split('.')[0]
            
            if skip_existing and pure_code in existing_records:
                last_update = existing_records[pure_code]
                # 如果最后更新日期是今天，则跳过
                if last_update == today:
                    skipped += 1
                    continue

            try:
                data = client.get_stock_basic(code)
            except Exception as e:
                print(f"Failed to fetch {code}: {e}")
                continue
            
            # 处理返回数据可能是列表或字典的情况
            item = None
            if isinstance(data, list) and len(data) > 0:
                item = data[0]
            elif isinstance(data, dict):
                item = data
            
            if not item:
                continue
                
            stmt = insert(StockBasic).values(
                ii=item.get("ii"),
                ei=item.get("ei"),
                name=item.get("name"),
                od=item.get("od"),
                pc=item.get("pc"),
                up=item.get("up"),
                dp=item.get("dp"),
                fv=item.get("fv"),
                tv=item.get("tv"),
                pk=item.get("pk"),
                is_suspend=item.get("is"),
                updated_at=today
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['ii'],
                set_={
                    "name": stmt.excluded.name,
                    "pc": stmt.excluded.pc,
                    "up": stmt.excluded.up,
                    "dp": stmt.excluded.dp,
                    "fv": stmt.excluded.fv,
                    "tv": stmt.excluded.tv,
                    "is": getattr(stmt.excluded, 'is'),
                    "updated_at": stmt.excluded.updated_at
                }
            )
            db.execute(stmt)
            count += 1
            if count % 100 == 0:
                print(f"Synced {count} stocks (Skipped {skipped})...")
                db.commit()
                
        db.commit()
        print(f"Successfully synced {count} stocks. Skipped {skipped} existing records.")
    except Exception as e:
        db.rollback()
        print(f"Error syncing stock basic info: {e}")
        traceback.print_exc()
    finally:
        db.close()

def is_trading_day(db, date_obj):
    """检查是否为交易日"""
    from sqlalchemy import text
    try:
        # trading_calendar 表字段为 trade_date
        date_str = date_obj.strftime("%Y-%m-%d")
        result = db.execute(text(f"SELECT 1 FROM trading_calendar WHERE trade_date = '{date_str}' LIMIT 1")).fetchone()
        return result is not None
    except Exception as e:
        print(f"Error checking trading day: {e}")
        # 如果出错，默认尝试同步，以免漏掉数据
        return True

def sync_limit_down_pool(start_date_str, end_date_str=None):
    """同步跌停股池 (支持日期范围)"""
    if not end_date_str:
        end_date_str = start_date_str
        
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    print(f"Starting sync of limit down pool from {start_date} to {end_date}...")
    client = MomaApiClient()
    db = SessionLocal()
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        
        # 检查是否为交易日
        if not is_trading_day(db, current_date):
            print(f"Skipping {date_str} (Not a trading day).")
            current_date += datetime.timedelta(days=1)
            continue
            
        print(f"Syncing limit down pool for {date_str}...")
        
        try:
            data = client.get_limit_down_pool(date_str)
            if not data:
                print(f"No limit down data for {date_str}.")
            else:
                items = data if isinstance(data, list) else []
                for item in items:
                    stmt = insert(LimitDownPool).values(
                        trade_date=current_date,
                        dm=item.get("dm"),
                        mc=item.get("mc"),
                        p=item.get("p"),
                        zf=item.get("zf"),
                        cje=item.get("cje"),
                        lt=item.get("lt"),
                        zsz=item.get("zsz"),
                        pe=item.get("pe"),
                        hs=item.get("hs"),
                        lbc=item.get("lbc"),
                        lbt=item.get("lbt"),
                        zj=item.get("zj"),
                        fba=item.get("fba"),
                        zbc=item.get("zbc")
                    )
                    # 冲突时更新
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['trade_date', 'dm'],
                        set_={
                            "mc": stmt.excluded.mc,
                            "p": stmt.excluded.p,
                            "zf": stmt.excluded.zf,
                            "cje": stmt.excluded.cje,
                            "lt": stmt.excluded.lt,
                            "zsz": stmt.excluded.zsz,
                            "pe": stmt.excluded.pe,
                            "hs": stmt.excluded.hs,
                            "lbc": stmt.excluded.lbc,
                            "lbt": stmt.excluded.lbt,
                            "zj": stmt.excluded.zj,
                            "fba": stmt.excluded.fba,
                            "zbc": stmt.excluded.zbc
                        }
                    )
                    db.execute(stmt)
                db.commit()
                print(f"Synced {len(items)} records for {date_str}.")
                
        except Exception as e:
            db.rollback()
            print(f"Error syncing limit down pool for {date_str}: {e}")
            traceback.print_exc()
            
        current_date += datetime.timedelta(days=1)
        
    db.close()
    print("Sync limit down pool completed.")

def sync_limit_up_pool(start_date_str, end_date_str=None):
    """同步涨停股池 (支持日期范围)"""
    if not end_date_str:
        end_date_str = start_date_str
        
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    print(f"Starting sync of limit up pool from {start_date} to {end_date}...")
    client = MomaApiClient()
    db = SessionLocal()
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        
        # 检查是否为交易日
        if not is_trading_day(db, current_date):
            print(f"Skipping {date_str} (Not a trading day).")
            current_date += datetime.timedelta(days=1)
            continue
            
        print(f"Syncing limit up pool for {date_str}...")
        
        try:
            data = client.get_limit_up_pool(date_str)
            if not data:
                print(f"No limit up data for {date_str}.")
            else:
                for item in data:
                    stmt = insert(LimitUpPool).values(
                        trade_date=current_date,
                        dm=item.get("dm"),
                        mc=item.get("mc"),
                        p=item.get("p"),
                        zf=item.get("zf"),
                        cje=item.get("cje"),
                        lt=item.get("lt"),
                        zsz=item.get("zsz"),
                        hs=item.get("hs"),
                        lbc=item.get("lbc"),
                        fbt=item.get("fbt"),
                        lbt=item.get("lbt"),
                        zj=item.get("zj"),
                        zbc=item.get("zbc"),
                        tj=item.get("tj"),
                        hy=item.get("hy")
                    )
                    # 冲突时更新 hy 等字段
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['trade_date', 'dm'],
                        set_={
                            "hy": stmt.excluded.hy,
                            "mc": stmt.excluded.mc,
                            "p": stmt.excluded.p,
                            "zf": stmt.excluded.zf,
                            "cje": stmt.excluded.cje,
                            "lt": stmt.excluded.lt,
                            "zsz": stmt.excluded.zsz,
                            "hs": stmt.excluded.hs,
                            "lbc": stmt.excluded.lbc,
                            "fbt": stmt.excluded.fbt,
                            "lbt": stmt.excluded.lbt,
                            "zj": stmt.excluded.zj,
                            "zbc": stmt.excluded.zbc,
                            "tj": stmt.excluded.tj
                        }
                    )
                    db.execute(stmt)
                db.commit()
                print(f"Synced {len(data)} records for {date_str}.")
                
        except Exception as e:
            db.rollback()
            print(f"Error syncing limit up pool for {date_str}: {e}")
            traceback.print_exc()
            
        current_date += datetime.timedelta(days=1)
        
    db.close()
    print("Sync limit up pool completed.")

def sync_category_mapping(skip_existing=False):
    """同步股票和行业/概念的映射关系"""
    print("Starting sync of stock and industry/concept mapping...")
    client = MomaApiClient()
    db = SessionLocal()
    
    try:
        # 获取所有叶子节点的分类代码，测试模式仅取前3个
        from sqlalchemy import text
        # 按代码排序，保证顺序一致
        # 过滤掉期货、债券、港股等非A股股票相关的分类，避免大量 404
        sql = """
            SELECT code, name 
            FROM moma_category_tree 
            WHERE (pname IS NULL OR (
                pname NOT LIKE '%期货%' 
                AND pname NOT LIKE '%债券%' 
                AND pname NOT LIKE '%香港%' 
                AND pname NOT LIKE '%指数成分%'
                AND pname NOT LIKE '%A股-分类%'
            ))
            AND name NOT LIKE '%期货%'
            AND name NOT LIKE '%债券%'
            AND name NOT LIKE '%香港%'
            ORDER BY code
        """
        categories = db.execute(text(sql)).fetchall()
        print(f"Total categories to process: {len(categories)}")
        
        processed_categories = set()
        if skip_existing:
            print("Fetching processed categories to skip...")
            # 只要该分类下有至少一条映射记录，就认为该分类已处理
            # 注意：如果某个分类本身确实为空（无股票），这种逻辑会导致每次都重新请求该分类。
            # 但对于断点续传来说，这已经能跳过绝大多数已完成的工作。
            result = db.execute(text("SELECT DISTINCT category_code FROM moma_stock_category_mapping")).fetchall()
            processed_categories = {row[0] for row in result}
            print(f"Found {len(processed_categories)} processed categories.")

        count = 0
        skipped = 0
        
        for cat_code, cat_name in categories:
            if skip_existing and cat_code in processed_categories:
                skipped += 1
                continue

            # 跳过期货相关的分类 (ad_qh 等)
            # 观察到 ad_qh 的 type1=8, pcode=sqs_qh
            # 暂时通过 try-except 跳过 404
            
            try:
                # 对 category_code 进行 URL 编码处理，防止特殊字符导致 404
                import urllib.parse
                safe_cat_code = urllib.parse.quote(cat_code)
                data = client.get_stock_by_category(safe_cat_code)
            except Exception as e:
                # 如果是 404，说明该分类可能没有股票列表接口，或者接口地址不对
                # 记录日志并继续
                print(f"Skipping category {cat_code} ({cat_name}): {e}")
                continue

            if not data or not isinstance(data, list):
                # 即使为空，也继续下一个
                continue
                
            for item in data:
                # 接口返回了股票代码 dm (例如: 000001)
                stock_code = item.get("dm", "")
                if not stock_code:
                    continue
                
                # 过滤非A股代码 (必须是6位数字)
                if not (stock_code.isdigit() and len(stock_code) == 6):
                    continue
                    
                stmt = insert(StockCategoryMapping).values(
                    stock_code=stock_code,
                    category_code=cat_code
                )
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=['stock_code', 'category_code']
                )
                db.execute(stmt)
                count += 1
                
            db.commit()
            # print(f"[{cat_name}] Synced {len(data)} mappings.")
            
        print(f"Successfully synced {count} mappings. Skipped {skipped} categories.")
    except Exception as e:
        db.rollback()
        print(f"Error syncing category mapping: {e}")
        traceback.print_exc()
    finally:
        db.close()


def _fetch_hsindex_name_map(client: MomaApiClient) -> dict:
    """从摩码指数列表接口拉取 dm -> mc 映射，失败时用空 dict 由调用方回退默认名。

    参数:
        client: 已初始化的 MomaApiClient

    返回:
        dict: 键为指数代码如 000300.SH，值为中文名称
    """
    data = client.get_hsindex_list()
    name_map = {}
    if not isinstance(data, list):
        return name_map
    for item in data:
        dm = item.get("dm")
        if dm:
            name_map[dm] = item.get("mc") or ""
    return name_map


def _parse_hsindex_trade_date(t_raw) -> Optional[datetime.date]:
    """将摩码返回的 t 字段解析为 date（日线取日历日，忽略时分秒）。

    参数:
        t_raw: 如 '2026-01-26 00:00:00' 或 '2026-01-26'

    返回:
        date | None: 解析失败返回 None
    """
    if not t_raw:
        return None
    s = str(t_raw).strip()[:10]
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _moma_bar_to_index_row(item: dict, instrument: str, name: str, created_at: datetime.date) -> Optional[dict]:
    """摩码 hsindex 单条 K 线 -> index_bar1d 行字典。

    参数:
        item: API 返回的单条记录（o/h/l/c/v/a/pc/t）
        instrument: 指数代码，如 000300.SH
        name: 指数中文名
        created_at: 写入 created_at 列的日期（通常为同步当日）

    返回:
        dict | None: 可传给 SQL INSERT 的参数字典；日期无效时返回 None
    """
    trade_date = _parse_hsindex_trade_date(item.get("t"))
    if trade_date is None:
        return None

    pre_close = item.get("pc")
    close = item.get("c")
    change = None
    change_rati = None
    if pre_close is not None and close is not None:
        try:
            change = float(close) - float(pre_close)
            if float(pre_close) != 0:
                change_rati = change / float(pre_close)
        except (TypeError, ValueError):
            pass

    vol = item.get("v")
    volume = int(vol) if vol is not None else None

    def _f(key):
        v = item.get(key)
        return float(v) if v is not None else None

    return {
        "date": trade_date,
        "instrument": instrument,
        "name": name,
        "pre_close": float(pre_close) if pre_close is not None else None,
        "open": _f("o"),
        "high": _f("h"),
        "low": _f("l"),
        "close": _f("c"),
        "volume": volume,
        "amount": _f("a"),
        "change": change,
        "change_rati": change_rati,
        "created_at": created_at,
    }


def _get_index_bar_max_date(db, instrument: str) -> Optional[datetime.date]:
    """查询 index_bar1d 中某指数已有数据的最后交易日（用于增量起点）。

    参数:
        db: SQLAlchemy Session
        instrument: 指数代码

    返回:
        date | None: 无数据时返回 None
    """
    from sqlalchemy import text
    row = db.execute(
        text("SELECT MAX(date) FROM index_bar1d WHERE instrument = :inst"),
        {"inst": instrument},
    ).fetchone()
    return row[0] if row and row[0] else None


def sync_index_bar1d(
    start_date_str: str = "2023-01-01",
    end_date_str: Optional[str] = None,
    instruments: Optional[List[tuple]] = None,
) -> None:
    """同步沪深指数日线到 index_bar1d（摩码 hsindex/history 接口）。

    默认同步上证指数 000001.SH、沪深300 000300.SH。库中已有数据时从 MAX(date)+1
    增量拉取；写入前对 [sync_start, sync_end] 区间 DELETE 再 INSERT，避免表无唯一键重复。

    参数:
        start_date_str: 全量/首次回填起始日，格式 YYYY-MM-DD，默认 2023-01-01
        end_date_str: 结束日，默认今天
        instruments: [(instrument, 默认中文名), ...]；None 时用 DEFAULT_INDEX_INSTRUMENTS

    返回:
        无
    """
    if not end_date_str:
        end_date_str = datetime.date.today().strftime("%Y-%m-%d")

    sync_end = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    default_start = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
    targets = instruments or DEFAULT_INDEX_INSTRUMENTS
    created_at = datetime.date.today()

    client = MomaApiClient()
    name_map = _fetch_hsindex_name_map(client)
    db = SessionLocal()

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

    try:
        for instrument, default_name in targets:
            name = name_map.get(instrument) or default_name
            max_date = _get_index_bar_max_date(db, instrument)
            if max_date and max_date >= sync_end:
                print(f"[index-bar] {instrument} ({name}) 已至 {max_date}，无需更新。")
                continue

            # 增量：有历史则从下一自然日；否则从用户指定的 default_start
            if max_date:
                sync_start = max(default_start, max_date + datetime.timedelta(days=1))
            else:
                sync_start = default_start

            if sync_start > sync_end:
                print(f"[index-bar] {instrument} ({name}) 起始 {sync_start} 晚于结束 {sync_end}，跳过。")
                continue

            st_api = sync_start.strftime("%Y%m%d")
            et_api = sync_end.strftime("%Y%m%d")
            print(f"[index-bar] 拉取 {instrument} ({name}) {sync_start} ~ {sync_end} ...")

            raw = client.get_hsindex_history(instrument, "d", st=st_api, et=et_api)
            if not raw:
                print(f"[index-bar] {instrument} 无数据或请求失败。")
                continue
            if not isinstance(raw, list):
                print(f"[index-bar] {instrument} 返回格式异常: {type(raw)}")
                continue

            rows = []
            for item in raw:
                row = _moma_bar_to_index_row(item, instrument, name, created_at)
                if row:
                    rows.append(row)

            if not rows:
                print(f"[index-bar] {instrument} 解析后 0 行，跳过写入。")
                continue

            db.execute(delete_sql, {"inst": instrument, "d0": sync_start, "d1": sync_end})
            for row in rows:
                db.execute(insert_sql, row)
            db.commit()
            print(f"[index-bar] {instrument} 写入 {len(rows)} 行（区间 {sync_start} ~ {sync_end}）。")

        print("[index-bar] 指数日线同步完成。")
    except Exception as e:
        db.rollback()
        print(f"[index-bar] 同步失败: {e}")
        traceback.print_exc()
    finally:
        db.close()


def _table_has_id_column(db, table_name: str) -> bool:
    """检查表是否存在且包含 id 列。

    参数:
        db: SQLAlchemy Session
        table_name: 表名（须在 FIX_ID_SCHEMA_TABLES 白名单内）

    返回:
        bool: 存在 id 列返回 True
    """
    row = db.execute(
        text("""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :t AND column_name = 'id'
        """),
        {"t": table_name},
    ).fetchone()
    return row is not None


def _count_duplicate_ids(db, table_name: str) -> int:
    """统计非空 id 的重复组数（存在则不宜直接加主键）。

    参数:
        db: SQLAlchemy Session
        table_name: 表名

    返回:
        int: 重复 id 的组数，0 表示无重复
    """
    return db.execute(
        text(f"""
            SELECT COUNT(*) FROM (
                SELECT id FROM {table_name}
                WHERE id IS NOT NULL
                GROUP BY id HAVING COUNT(*) > 1
            ) dup
        """)
    ).scalar() or 0


def fix_table_id_schema(db, table_name: str) -> dict:
    """为单表补齐 id 序列、回填 NULL、设置 DEFAULT/NOT NULL/主键。

    与设计器一致：id 使用 {table}_id_seq，INSERT 不写 id 时自动 nextval。
    仅操作白名单表名，避免 SQL 注入。

    参数:
        db: SQLAlchemy Session（调用方负责 commit）
        table_name: 表名，必须在 FIX_ID_SCHEMA_TABLES 中

    返回:
        dict: total/has_id/null_id_filled/duplicate_id_groups 等统计

    异常:
        ValueError: 表不存在、无 id 列或存在重复 id
    """
    if table_name not in FIX_ID_SCHEMA_TABLES:
        raise ValueError(f"不允许修复表: {table_name}")
    if not _table_has_id_column(db, table_name):
        raise ValueError(f"表 {table_name} 不存在或没有 id 列")

    seq_name = f"{table_name}_id_seq"
    pk_name = f"{table_name}_pkey"

    before = db.execute(
        text(f"""
            SELECT COUNT(*) AS total,
                   COUNT(id) AS has_id,
                   COUNT(*) FILTER (WHERE id IS NULL) AS null_id
            FROM {table_name}
        """)
    ).mappings().one()

    dup_groups = _count_duplicate_ids(db, table_name)
    if dup_groups > 0:
        raise ValueError(f"{table_name}: 存在 {dup_groups} 组重复 id，请先清理后再修复")

    db.execute(text(f"CREATE SEQUENCE IF NOT EXISTS {seq_name}"))
    max_id = db.execute(text(f"SELECT COALESCE(MAX(id), 0) FROM {table_name}")).scalar() or 0
    # is_called=true：下一号从 max_id+1 起，避免与已有 id 冲突
    db.execute(text(f"SELECT setval('{seq_name}', :max_id, true)"), {"max_id": int(max_id)})

    null_before = int(before["null_id"] or 0)
    if null_before > 0:
        print(f"[fix-schema] {table_name} 回填 {null_before} 行 NULL id ...")
        db.execute(
            text(f"UPDATE {table_name} SET id = nextval('{seq_name}') WHERE id IS NULL")
        )

    db.execute(
        text(f"ALTER TABLE {table_name} ALTER COLUMN id SET DEFAULT nextval('{seq_name}'::regclass)")
    )
    db.execute(text(f"ALTER TABLE {table_name} ALTER COLUMN id SET NOT NULL"))
    db.execute(text(f"ALTER SEQUENCE {seq_name} OWNED BY {table_name}.id"))

    has_pk = db.execute(
        text("""
            SELECT 1 FROM pg_constraint c
            JOIN pg_class cl ON c.conrelid = cl.oid
            WHERE cl.relname = :t AND c.contype = 'p'
        """),
        {"t": table_name},
    ).fetchone()
    if not has_pk:
        db.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT {pk_name} PRIMARY KEY (id)"))

    after = db.execute(
        text(f"""
            SELECT COUNT(*) AS total,
                   COUNT(id) AS has_id,
                   COUNT(*) FILTER (WHERE id IS NULL) AS null_id
            FROM {table_name}
        """)
    ).mappings().one()

    return {
        "table": table_name,
        "total": int(before["total"]),
        "null_id_before": null_before,
        "null_id_after": int(after["null_id"] or 0),
        "max_id": db.execute(text(f"SELECT MAX(id) FROM {table_name}")).scalar(),
        "duplicate_id_groups": dup_groups,
    }


def fix_bar_tables_id_schema(tables: Optional[List[str]] = None) -> None:
    """批量修复 FIX_ID_SCHEMA_TABLES 中各表的 id 自增与主键。

    参数:
        tables: 要修复的表名列表；None 时修复全部默认四张表

    返回:
        无
    """
    targets = tables or FIX_ID_SCHEMA_TABLES
    db = SessionLocal()
    try:
        for table_name in targets:
            print(f"[fix-schema] 开始处理 {table_name} ...")
            stats = fix_table_id_schema(db, table_name)
            db.commit()
            print(
                f"[fix-schema] {table_name} 完成: 共 {stats['total']} 行, "
                f"回填 NULL id {stats['null_id_before']} 行, "
                f"当前 max(id)={stats['max_id']}, 剩余 NULL={stats['null_id_after']}"
            )
        print("[fix-schema] 全部表处理完成。")
    except Exception as e:
        db.rollback()
        print(f"[fix-schema] 失败: {e}")
        traceback.print_exc()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Moma API data to database")
    
    # Define arguments
    parser.add_argument("--init-db", action="store_true", help="Initialize database tables")
    parser.add_argument("--category-tree", action="store_true", help="Sync category tree (Index, Industry, Concept)")
    parser.add_argument("--stock-list", action="store_true", help="Sync stock list (stock_list table)")
    parser.add_argument("--stock-basic", action="store_true", help="Sync stock basic info")
    parser.add_argument("--skip-existing", action="store_true", help="Skip records that already exist in the database (for stock-basic)")
    parser.add_argument("--category-mapping", action="store_true", help="Sync stock-category mapping")
    parser.add_argument("--limit-up", type=str, metavar="YYYY-MM-DD", help="Sync limit up pool for specific date (or start date)")
    parser.add_argument("--limit-down", type=str, metavar="YYYY-MM-DD", help="Sync limit down pool for specific date (or start date)")
    parser.add_argument("--end-date", type=str, metavar="YYYY-MM-DD", help="End date for limit up/down pool sync (optional)")
    parser.add_argument("--index-bar", action="store_true", help="Sync index daily bars to index_bar1d (000001.SH, 000300.SH)")
    parser.add_argument("--start-date", type=str, default="2023-01-01", metavar="YYYY-MM-DD",
                        help="Start date for --index-bar (default: 2023-01-01)")
    parser.add_argument(
        "--fix-bar-schema",
        action="store_true",
        help="Fix id SERIAL/PK on index_bar1d, concept_bar1d, concept_component, limit_up_pool",
    )
    parser.add_argument("--all", action="store_true", help="Sync all basic data (tree, basic, mapping)")
    
    args = parser.parse_args()
    
    # Check if no arguments provided
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    if args.init_db:
        print("Initializing database tables...")
        Base.metadata.create_all(bind=engine)
        print("Database tables initialized.")

    if args.all:
        args.category_tree = True
        args.stock_list = True
        args.stock_basic = True
        args.category_mapping = True

    if args.category_tree:
        sync_category_tree()

    if args.stock_list:
        sync_stock_list()
        
    if args.stock_basic:
        sync_stock_basic(skip_existing=args.skip_existing)
        
    if args.limit_up:
        sync_limit_up_pool(args.limit_up, args.end_date)
        
    if args.limit_down:
        sync_limit_down_pool(args.limit_down, args.end_date)
        
    if args.category_mapping:
        sync_category_mapping(skip_existing=args.skip_existing)

    if args.index_bar:
        sync_index_bar1d(start_date_str=args.start_date, end_date_str=args.end_date)

    if args.fix_bar_schema:
        fix_bar_tables_id_schema()
