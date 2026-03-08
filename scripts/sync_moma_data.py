import sys
import os
import traceback
from sqlalchemy.dialects.postgresql import insert
import datetime

# 将项目根目录加入 sys.path 以便导入 scripts 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.db_session import engine, SessionLocal, Base
from scripts.models import CategoryTree, StockBasic, FinancialIndex, LimitUpPool, StockCategoryMapping, LimitDownPool, TechnicalMacd, TechnicalMa, TechnicalBoll, TechnicalKdj
from scripts.moma_api_client import MomaApiClient

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

def sync_stock_basic():
    """同步股票基础信息"""
    print("开始同步股票基础信息...")
    client = MomaApiClient()
    db = SessionLocal()
    
    # 从本地数据库获取所有股票代码
    from sqlalchemy import text
    try:
        result = db.execute(text("SELECT instrument FROM stock_list LIMIT 5")).fetchall()
        stock_codes = [row[0] for row in result]
        print(f"共需同步 {len(stock_codes)} 只股票 (测试模式，仅取前5只)")
        
        count = 0
        for code in stock_codes:
            # 提取数字代码
            pure_code = code.split('.')[0]
            data = client.get_stock_basic(pure_code)
            if not data or not isinstance(data, dict):
                continue
                
            item = data
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
                is_suspend=item.get("is")
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
                    "is_suspend": stmt.excluded.is_suspend
                }
            )
            db.execute(stmt)
            count += 1
            if count % 100 == 0:
                print(f"已同步 {count} 只股票基础信息...")
                db.commit()
                
        db.commit()
        print(f"成功同步 {count} 只股票基础信息。")
    except Exception as e:
        db.rollback()
        print(f"同步股票基础信息出错: {e}")
        traceback.print_exc()
    finally:
        db.close()

def sync_limit_down_pool(trade_date_str):
    """同步跌停股池"""
    print(f"开始同步 {trade_date_str} 跌停股池...")
    client = MomaApiClient()
    db = SessionLocal()
    
    try:
        data = client.get_limit_down_pool(trade_date_str)
        if not data:
            print(f"获取 {trade_date_str} 跌停股池数据失败或为空。")
            return
            
        items = data if isinstance(data, list) else []
        if not items:
            print(f"{trade_date_str} 无跌停股池数据。")
            return
            
        print(f"获取到 {len(items)} 条跌停股池数据，准备入库...")
        for item in items:
            stmt = insert(LimitDownPool).values(
                trade_date=trade_date_str,
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
            # 跌停股池每日股票数据唯一，发生冲突什么都不做（或更新部分字段，这里选择不做更新以防止影响历史）
            on_conflict_stmt = stmt.on_conflict_do_nothing(
                index_elements=['trade_date', 'dm']
            )
            db.execute(on_conflict_stmt)
            
        db.commit()
        print(f"{trade_date_str} 跌停股池数据同步完成！")
        
    except Exception as e:
        db.rollback()
        print(f"同步过程中发生异常: {e}")
        traceback.print_exc()
    finally:
        db.close()

def sync_limit_up_pool(trade_date_str):
    """同步涨停股池"""
    print(f"开始同步 {trade_date_str} 涨停股池...")
    client = MomaApiClient()
    db = SessionLocal()
    try:
        data = client.get_limit_up_pool(trade_date_str)
        if not data:
            print("未获取到涨停股池数据。")
            return
            
        trade_date = datetime.datetime.strptime(trade_date_str, "%Y-%m-%d").date()
        for item in data:
            stmt = insert(LimitUpPool).values(
                trade_date=trade_date,
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
            # 主键冲突时不做更新或部分更新
            stmt = stmt.on_conflict_do_nothing(
                index_elements=['trade_date', 'dm']
            )
            db.execute(stmt)
        db.commit()
        print(f"成功同步 {len(data)} 条涨停股记录。")
    except Exception as e:
        db.rollback()
        print(f"同步涨停股池出错: {e}")
        traceback.print_exc()
    finally:
        db.close()

def sync_category_mapping():
    """同步股票和行业/概念的映射关系"""
    print("开始同步股票与行业、概念的映射关系...")
    client = MomaApiClient()
    db = SessionLocal()
    
    try:
        # 获取所有叶子节点的分类代码，测试模式仅取前3个
        from sqlalchemy import text
        categories = db.execute(text("SELECT code, name FROM moma_category_tree LIMIT 3")).fetchall()
        print(f"共有 {len(categories)} 个分类节点待处理 (测试模式，仅取前3个)")
        
        count = 0
        for cat_code, cat_name in categories:
            data = client.get_stock_by_category(cat_code)
            if not data or not isinstance(data, list):
                continue
                
            for item in data:
                # 接口返回了股票代码 dm (例如: 000001)
                stock_code = item.get("dm", "")
                if not stock_code:
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
            print(f"[{cat_name}] 同步了 {len(data)} 条映射关系。")
            
        print(f"全部映射关系同步完成，共计 {count} 条。")
    except Exception as e:
        db.rollback()
        print(f"同步映射关系出错: {e}")
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    print("初始化数据库表结构...")
    Base.metadata.create_all(bind=engine)
    print("数据库表结构初始化完成。")
    
    # 同步数据
    # sync_category_tree()
    sync_stock_basic()
    # sync_limit_up_pool("2024-02-28")
    # sync_limit_down_pool("2024-02-28")
    sync_category_mapping()
