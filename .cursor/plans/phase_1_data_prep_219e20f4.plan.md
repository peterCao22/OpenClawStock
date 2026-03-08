---
name: "Phase 1: Data Prep"
overview: 构建并执行第一阶段“数据源准备”的具体计划。这涵盖了使用 SQLAlchemy 连接 PostgreSQL、设计并创建行业/概念/财务表结构，以及调用摩码云服 API 进行数据同步。
todos:
  - id: moma_api_docs
    content: 获取摩码云服 API 文档详情 (待用户提供)
    status: completed
  - id: db_connection
    content: 编写 SQLAlchemy 数据库连接配置 (db_session.py)
    status: completed
  - id: db_models
    content: 根据 API 文档定义数据表结构 ORM (models.py)
    status: completed
  - id: api_client
    content: 封装摩码云服 API 客户端 (moma_api_client.py)
    status: completed
  - id: sync_script
    content: 编写并测试主数据同步脚本 (sync_moma_data.py)
    status: completed
isProject: false
---

# Phase 1: 数据源准备 (Data Source Preparation) 执行计划

本计划详细说明了如何完成股票监控系统的第 1 步：数据源准备。

## 1. 数据库连接与 ORM 配置 (SQLAlchemy)

- **读取配置**: 使用 `python-dotenv` 读取项目根目录下的 `.env` 文件，获取 PostgreSQL 的连接信息（`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`）。
- **建立连接**: 编写 `scripts/db_session.py`（或类似的文件），使用 `SQLAlchemy` 创建 `Engine` 和 `SessionLocal`。使用 `psycopg2` 作为驱动。

## 2. 数据库表结构设计 (ORM Models)

根据摩码云服 API 的实际返回结构（需用户提供具体的文档详情后最终确认），使用 SQLAlchemy 的 declarative base 定义以下数据模型，并放置于 `scripts/models.py` 中：

*待确认：当用户提供 API 详细返回 JSON 示例后，我们将精确映射字段类型（如 VARCHAR, Float, Integer, Date）。*

初步构想的表结构：

- `**moma_industry` (行业表)**: 股票代码, 行业代码, 行业名称, 更新日期 等。
- `**moma_concept` (概念表)**: 股票代码, 概念代码, 概念名称, 更新日期 等。
- `**moma_finance` (财务基本信息表)**: 股票代码, 报告期, 市盈率(PE), 市净率(PB), 总市值, 流通市值, 营业收入, 净利润, 更新日期 等。

**建表策略**: 脚本运行时，如果表不存在，则使用 `Base.metadata.create_all(bind=engine)` 自动创建。

## 3. 摩码云服 API 客户端封装

- 编写 `scripts/moma_api_client.py`，封装针对摩码云服 API 的请求逻辑。
- **认证与参数**: 从 `.env` 获取 `MOMA_API_URL` 和 `MOMA_API_KEY`。为每个请求自动附加 Token。
- **重试与限流**: 实现基于 `.env` 中 `API_SLEEP_SECONDS` 的速率限制，以及基础的异常重试机制。

## 4. 数据同步与落库主脚本

- 编写 `scripts/sync_moma_data.py` 作为主入口。
- **流程**:
  1. 初始化数据库连接并确保表结构已创建。
  2. 调用 API 客户端，拉取最新的行业、概念、财务数据。
  3. 将 JSON 数据转换为 SQLAlchemy 模型对象（或直接使用 pandas `to_sql` 以提高批量插入性能）。
  4. 处理冲突：如果数据已存在（根据主键或唯一索引），则执行“UPSERT”（在 PostgreSQL 中使用 `ON CONFLICT DO UPDATE`）。
  5. 记录同步日志（总条数、成功、失败）至 `logs/` 目录。

## 5. 依赖管理

- 创建或更新 `requirements.txt`，确保包含以下库：
  - `SQLAlchemy`
  - `psycopg2-binary` (或 `psycopg2`)
  - `python-dotenv`
  - `requests`
  - `pandas` (可选，用于辅助批量插入)

## 行动项 (Todos)

1. [ ] 用户提供摩码云服的“概念、行业、财务信息”API 文档详情。
2. [ ] 编写并测试 `db_session.py` (SQLAlchemy 引擎建立)。
3. [ ] 根据 API 文档编写 `models.py` (定义 ORM 类与表结构)。
4. [ ] 编写 `moma_api_client.py` (API 请求封装)。
5. [ ] 编写 `sync_moma_data.py` (核心同步落库脚本) 并执行测试。

