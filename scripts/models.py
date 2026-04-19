from sqlalchemy import Column, String, Integer, Float, Date, BigInteger
from scripts.db_session import Base

class StockList(Base):
    """股票列表 (现有表)"""
    __tablename__ = "stock_list"
    
    instrument = Column(String(20), primary_key=True, comment="股票代码 e.g. 000001.SZ")
    name = Column(String(50), comment="股票名称")

class CategoryTree(Base):
    """指数、行业、概念树"""
    __tablename__ = "moma_category_tree"
    
    code = Column(String(50), primary_key=True, index=True, comment="代码")
    name = Column(String(100), comment="名称")
    type1 = Column(Integer, comment="一级分类")
    type2 = Column(Integer, comment="二级分类")
    level = Column(Integer, comment="层级")
    pcode = Column(String(50), comment="父节点代码")
    pname = Column(String(100), comment="父节点名称")
    isleaf = Column(Integer, comment="是否为叶子节点")

class StockBasic(Base):
    """股票基础信息"""
    __tablename__ = "moma_stock_basic"
    
    ii = Column(String(20), primary_key=True, index=True, comment="股票代码")
    ei = Column(String(10), comment="市场代码")
    name = Column(String(50), comment="股票名称")
    od = Column(String(20), comment="上市日期")
    pc = Column(Float, comment="前收盘价格")
    up = Column(Float, comment="当日涨停价")
    dp = Column(Float, comment="当日跌停价")
    fv = Column(Float, comment="流通股本")
    tv = Column(Float, comment="总股本")
    pk = Column(Float, comment="最小价格变动单位")
    is_suspend = Column(Integer, name="is", comment="股票停牌状态")
    updated_at = Column(Date, comment="更新日期")

class FinancialIndex(Base):
    """财务主要指标"""
    __tablename__ = "moma_financial_index"
    
    stock_code = Column(String(20), primary_key=True, comment="股票代码")
    jzrq = Column(Date, primary_key=True, comment="截止日期")
    plrq = Column(Date, comment="披露日期")
    mgjyhdxjl = Column(Float, comment="每股经营活动现金流量")
    mgjzc = Column(Float, comment="每股净资产")
    jbmgsy = Column(Float, comment="基本每股收益")
    xsmgsy = Column(Float, comment="稀释每股收益")
    mgwfplr = Column(Float, comment="每股未分配利润")
    mgzbgjj = Column(Float, comment="每股资本公积金")
    kfmgsy = Column(Float, comment="扣非每股收益")
    jzcsyl = Column(Float, comment="净资产收益率")
    xsmlv = Column(Float, comment="销售毛利率")
    zyyrsrzz = Column(Float, comment="主营收入同比增长")
    jlrzz = Column(Float, comment="净利润同比增长")
    gsmgsyzzdjlrzz = Column(Float, comment="归属母公司净利润同比")
    kfjlrzz = Column(Float, comment="扣非净利润同比")
    mlv = Column(Float, comment="毛利率")
    jlv = Column(Float, comment="净利率")
    zcfzl = Column(Float, comment="资产负债比率")
    chzzl = Column(Float, comment="存货周转率")

class StockCategoryMapping(Base):
    """股票与指数、行业、概念的映射关系表"""
    __tablename__ = "moma_stock_category_mapping"
    
    stock_code = Column(String(20), primary_key=True, comment="股票代码 (如 000001)")
    category_code = Column(String(50), primary_key=True, comment="指数/行业/概念代码 (如 sw_yx)")
    
class LimitUpPool(Base):
    """涨停股池"""
    __tablename__ = "moma_limit_up_pool"
    
    trade_date = Column(Date, primary_key=True, comment="交易日期")
    dm = Column(String(20), primary_key=True, comment="代码")
    mc = Column(String(50), comment="名称")
    p = Column(Float, comment="价格")
    zf = Column(Float, comment="涨幅")
    cje = Column(Float, comment="成交额")
    lt = Column(Float, comment="流通市值")
    zsz = Column(Float, comment="总市值")
    hs = Column(Float, comment="换手率")
    lbc = Column(Integer, comment="连板数")
    fbt = Column(String(20), comment="首次封板时间")
    lbt = Column(String(20), comment="最后封板时间")
    zj = Column(Float, comment="封板资金")
    zbc = Column(Integer, comment="炸板次数")
    tj = Column(String(20), comment="涨停统计")
    hy = Column(String(50), comment="所属行业")

class LimitDownPool(Base):
    """跌停股池"""
    __tablename__ = "moma_limit_down_pool"
    
    trade_date = Column(Date, primary_key=True, comment="交易日期")
    dm = Column(String(20), primary_key=True, comment="代码")
    mc = Column(String(50), comment="名称")
    p = Column(Float, comment="价格")
    zf = Column(Float, comment="跌幅")
    cje = Column(Float, comment="成交额")
    lt = Column(Float, comment="流通市值")
    zsz = Column(Float, comment="总市值")
    pe = Column(Float, comment="动态市盈率")
    hs = Column(Float, comment="换手率")
    lbc = Column(Integer, comment="连续跌停次数")
    lbt = Column(String(20), comment="最后封板时间")
    zj = Column(Float, comment="封单资金")
    fba = Column(Float, comment="板上成交额")
    zbc = Column(Integer, comment="开板次数")

class TechnicalMacd(Base):
    """历史分时MACD (日线级别)"""
    __tablename__ = "moma_technical_macd"
    
    stock_code = Column(String(20), primary_key=True, comment="股票代码")
    trade_date = Column(Date, primary_key=True, comment="交易日期")
    dif = Column(Float, comment="DIF")
    dea = Column(Float, comment="DEA")
    macd = Column(Float, comment="MACD")

class TechnicalMa(Base):
    """历史分时MA (日线级别)"""
    __tablename__ = "moma_technical_ma"
    
    stock_code = Column(String(20), primary_key=True, comment="股票代码")
    trade_date = Column(Date, primary_key=True, comment="交易日期")
    ma5 = Column(Float, comment="MA5")
    ma10 = Column(Float, comment="MA10")
    ma20 = Column(Float, comment="MA20")
    ma30 = Column(Float, comment="MA30")
    ma60 = Column(Float, comment="MA60")

class TechnicalBoll(Base):
    """历史分时BOLL (日线级别)"""
    __tablename__ = "moma_technical_boll"
    
    stock_code = Column(String(20), primary_key=True, comment="股票代码")
    trade_date = Column(Date, primary_key=True, comment="交易日期")
    mid = Column(Float, comment="中轨线")
    upper = Column(Float, comment="上轨线")
    lower = Column(Float, comment="下轨线")

class TechnicalKdj(Base):
    """历史分时KDJ (日线级别)"""
    __tablename__ = "moma_technical_kdj"
    
    stock_code = Column(String(20), primary_key=True, comment="股票代码")
    trade_date = Column(Date, primary_key=True, comment="交易日期")
    k = Column(Float, comment="K值")
    d = Column(Float, comment="D值")
    j = Column(Float, comment="J值")

