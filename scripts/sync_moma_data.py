import sys
import os
import argparse
import traceback
from sqlalchemy.dialects.postgresql import insert
import datetime

# 将项目根目录加入 sys.path 以便导入 scripts 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.db_session import engine, SessionLocal, Base
from scripts.models import CategoryTree, StockBasic, FinancialIndex, LimitUpPool, StockCategoryMapping, LimitDownPool, TechnicalMacd, TechnicalMa, TechnicalBoll, TechnicalKdj, StockList
from scripts.moma_api_client import MomaApiClient

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
