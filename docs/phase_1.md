# Phase 1: 数据源准备 (Data Source Preparation) - 完成情况与测试计划

## 1. 目前完成情况 (Status: COMPLETED)

截至目前，第一阶段“数据源准备”的核心代码开发已全部完成。主要成果如下：

- **数据库连接与配置** (`scripts/db_session.py`)：使用 SQLAlchemy 实现了与本地 PostgreSQL 的连接池配置，并通过 `.env` 安全管理凭证。
- **ORM 模型设计** (`scripts/models.py`)：全面映射了摩码云服 API 的数据结构，包含：
  - `CategoryTree` (指数/行业/概念树)
  - `StockBasic` (股票基础信息)
  - `FinancialIndex` (财务主要指标)
  - `LimitUpPool` / `LimitDownPool` (涨跌停股池)
  - `StockCategoryMapping` (股票与行业/概念的多对多映射关系)
  - `TechnicalMacd`, `TechnicalMa`, `TechnicalBoll`, `TechnicalKdj` (技术指标日线表)
- **API 客户端封装** (`scripts/moma_api_client.py`)：
  - 实现了对摩码云服各类数据接口的请求封装。
  - 增加了 **全局间歇控制 (Throttling)** 和 **HTTP 429 限流自动退避重试 (Exponential Backoff)** 机制，确保大规模拉取数据的稳定性。
- **数据同步逻辑** (`scripts/sync_moma_data.py`)：
  - 实现了各类数据的全量/增量拉取与入库逻辑。
  - 采用 `on_conflict_do_update` / `on_conflict_do_nothing` 优雅处理数据重复和主键冲突问题。

---

## 2. 接下来的执行与测试计划

为了确保 Phase 1 的代码在实际环境中稳定运行，接下来需要进行系统的执行与测试。请按以下步骤进行：

### 测试步骤 1：环境与数据库初始化
- **目标**：验证数据库连接和表结构创建。
- **操作**：
  1. 确保本地 PostgreSQL 数据库已启动，且 `.env` 中的配置（Host, Port, User, Password, DB_NAME）正确无误。
  2. 运行建表脚本：`python -c "from scripts.models import Base; from scripts.db_session import engine; Base.metadata.create_all(bind=engine)"`
- **预期结果**：无报错退出，数据库中成功生成 `moma_` 开头的所有表。

### 测试步骤 2：API 连通性与限流机制测试
- **目标**：验证 API Token 是否有效，以及限流重试机制是否按预期工作。
- **操作**：
  1. 在 `scripts/sync_moma_data.py` 的 `__main__` 块中，单独取消注释并运行 `sync_category_tree()`。
  2. 执行：`python scripts/sync_moma_data.py`
- **预期结果**：控制台输出同步进度，若触发限流能看到 "API Rate Limited (429)... Retrying" 的日志，最终成功入库。

### 测试步骤 3：核心静态数据全量同步测试
- **目标**：同步全市场股票基础信息和行业概念映射（数据量较大）。
- **操作**：
  1. 确保本地 `stock_list` 表中已有基础股票代码数据。
  2. 在 `sync_moma_data.py` 中依次调用运行 `sync_stock_basic()` 和 `sync_category_mapping()`。
- **预期结果**：程序稳定运行，不因单只股票查询失败而中断，最终在数据库中查看到数千条股票基础信息和映射关系。

### 测试步骤 4：每日动态数据同步测试
- **目标**：测试涨跌停股池等每日更新的数据。
- **操作**：
  1. 指定一个最近的交易日（如 `2024-01-15`），在脚本中调用运行 `sync_limit_up_pool("2024-01-15")` 和 `sync_limit_down_pool("2024-01-15")`。
- **预期结果**：成功获取当日的涨跌停列表并入库，重复运行同一天的数据不会产生主键冲突报错。

### 测试步骤 5：数据完整性抽样校验
- **目标**：确保入库数据准确无误。
- **操作**：
  1. 使用 DBeaver 或 psql 连接数据库。
  2. 执行 SQL 查询：`SELECT * FROM moma_stock_category_mapping LIMIT 10;` 检查多对多关系。
  3. 执行 SQL 查询：`SELECT * FROM moma_limit_down_pool ORDER BY zj DESC LIMIT 10;` 检查跌停封单资金排序是否正确。

---

## 3. 本项目使用的数据库表清单

为了明确项目边界，以下是本项目核心依赖的数据库表。**目前数据库中的其他表，本项目暂时不使用。**

### 3.1 本地已有的基础表
1. **`kline_qfq` (日K线表 - 前复权)**
   - **用途**：用于量化选股策略（放量大涨、回调、回抽等特征识别）。
   - **注意与隐患**：后续如果通过摩码云服的“历史分时交易”接口 (`/hsstock/history/...`) 补充该表数据，该接口返回字段中**不包含换手率 (hs)**。如果策略强依赖换手率，后续可能需要通过 `成交量(v) / 流通股本(fv)` 自行计算，或结合其他行情指标接口获取。
2. **`stock_list` (股票列表)**
   - **用途**：全市场股票代码池。
   - **补充方式**：后续可通过摩码云服的“股票列表”接口 (`/hslt/list/...`) 进行增量补充和更新。
3. **`trading_calendar` (交易日历)**
   - **用途**：用于计算 T 日、T-22 日等交易日偏移逻辑。

### 3.2 本次基于摩码云服 API 新设计的表
1. **`moma_stock_basic`**：股票基础信息表（含流通股本、总股本等，可用于辅助计算换手率）。
2. **`moma_category_tree`**：指数、行业、概念树。
3. **`moma_stock_category_mapping`**：股票与行业/概念的多对多映射关系表。
4. **`moma_limit_up_pool`**：每日涨停股池。
5. **`moma_limit_down_pool`**：每日跌停股池。
6. **`moma_financial_index`**：财务主要指标表。
7. **技术指标系列表**：
   - `moma_technical_macd` (MACD)
   - `moma_technical_ma` (MA均线)
   - `moma_technical_boll` (BOLL布林带)
   - `moma_technical_kdj` (KDJ)

---

### 后续阶段预告
在 Phase 1 测试通过并完成初始数据积累后，我们将进入 **Phase 2: 量化选股脚本开发**，利用已入库的日 K 线和技术指标数据，实现基于“放量大涨-回调-回抽”特征的 TOP-20 股票筛选逻辑。