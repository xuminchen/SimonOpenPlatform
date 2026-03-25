# 数据集成系统产品需求文档（PRD）

## 1. 文档信息
- 文档名称：开放平台 API 数据集成系统 PRD
- 文档版本：v1.0
- 创建日期：2026-03-25（Asia/Shanghai）
- 文档状态：生效中
- 适用范围：`wonderlab_api` 仓库（后端 `webapp/`，前端 `frontend/`，配置 `config/`）

---

## 2. 背景与问题定义
当前系统已具备基础的连接管理、任务执行与结果预览能力，但在“可规模化接入外部平台 API、稳定运行、可观测运维”方面仍存在缺口：
- 外部平台接口开发与执行链路需进一步产品化（开发、发布、调用、结果可见）
- 失败告警与通知闭环不足，问题发现与定位依赖人工
- 项目上线前缺少标准化“就绪度体检”
- 安全、质量与治理规范需要统一

本 PRD 目标是把系统提升为可用于生产的数据集成平台。

---

## 3. 产品愿景与目标
### 3.1 愿景
构建一个“配置驱动、可调度、可观测、可治理”的开放平台 API 数据集成系统。

### 3.2 业务目标（12 周）
- 平台接入效率：单平台接入从“按代码开发”缩短到“按配置开发 + 少量适配”
- 执行可见性：100% 任务具备可追溯执行记录与结果预览
- 问题发现时效：失败任务 1 分钟内触发告警
- 上线可靠性：所有新项目上线前完成就绪度检查并留痕

### 3.3 成功指标（KPI）
- 接口发布成功率（Builder -> Published）>= 95%
- 任务执行成功率（排除上游限流、授权失效等外因）>= 90%
- 平均故障恢复时间（MTTR）下降 40%
- 就绪度检查覆盖率 = 100%

---

## 4. 用户角色与核心场景
### 4.1 角色
- 产品经理：定义平台能力、审批上线、追踪稳定性
- 数据工程师：配置接口、调度策略、排障与补数
- 数据分析/运营：查看执行状态、预览结果、校验数据
- 运维/平台管理员：管理凭证、安全策略、告警通道

### 4.2 核心场景
1. 新平台 API 接入：配置请求、鉴权、分页、提取规则 -> 发布 stream
2. 建立项目：绑定凭证、目标库、stream、调度计划
3. 执行采集：Run Now / Routine / Backfill
4. 查看结果：执行状态、错误详情、预览表格、原始响应
5. 失败治理：触发告警、定位原因、重试与修复
6. 上线检查：系统自动输出“可上线/不可上线”结论

---

## 5. 产品范围（Scope）
### 5.1 In Scope（本期）
- 外部平台 API 的配置化开发与调用（Builder + Runtime）
- Connection/Project/Stream 全链路执行
- 执行结果可视化（状态 + 预览 + 原始响应）
- 告警中心（告警通道、事件记录、失败通知）
- 项目就绪度评估（Readiness）
- 基础目标库管理与留存清理策略

### 5.2 Out of Scope（后续）
- 复杂任务编排（跨项目 DAG）
- 多租户隔离与细粒度 RBAC（先保留基础权限）
- 高级数据质量规则引擎（本期先提供基础检查）

---

## 6. 功能架构
系统模块分为 8 层：
1. 平台与凭证层：平台注册、凭证同步、Token 管理
2. 接口开发层（Builder）：请求/鉴权/分页/提取规则配置
3. 连接建模层（Connection/Project/Stream）：任务边界定义
4. 执行调度层（Routine/Backfill/Run Now）：任务触发与执行
5. 数据落地层（Destination）：目标库写入与文件管理
6. 可视化层（Console）：执行历史、预览、错误明细
7. 运维治理层（Alerts + Readiness）：告警与上线体检
8. 平台基础层：DB、配置、调度器、安全策略

---

## 7. 关键业务流程
### 7.1 外部接口开发到执行闭环
1. 在 Builder 配置 API 请求规则并测试
2. 发布为 `platform_api_stream`
3. 在 Project 中选择并绑定 stream
4. 手动 Run Now 或按调度执行
5. 查看 execution 结果（状态、错误、原始响应、提取记录）

### 7.2 失败处理闭环
1. 执行失败或上游全部失败
2. 生成告警事件（含项目、stream、execution、错误摘要）
3. 按告警通道投递 webhook
4. 控制台可查询历史告警与投递状态

### 7.3 上线前体检流程
1. 触发项目 Readiness 检查
2. 逐项输出 PASS/WARN/FAIL（凭证、目标、stream、调度、告警等）
3. 仅 `ready=true` 的项目允许进入生产调度

---

## 8. 数据模型（产品视角）
### 8.1 已有核心实体
- PlatformAccount（平台账号/凭证）
- PlatformApiStream（Builder 发布的接口流）
- SyncProject / SyncStreamTask / SyncExecutionInstance（项目、流任务、执行实例）
- DestinationProfile（目标配置）

### 8.2 本期新增实体
- AlertChannel：告警渠道（webhook 为主）
- AlertEvent：告警事件（失败上下文、投递结果）

---

## 9. 外部平台接口开发与调用（重点）
### 9.1 功能要求
- 可通过配置定义外部 API：URL、Method、Headers、Query、Body
- 可配置鉴权策略：Header/Query/Body 注入
- 可配置分页策略：none/cursor/offset（按平台扩展）
- 可配置提取策略：JSONPath/selector 抽取 records
- 支持测试预览：请求预览、原始返回、提取结果、推断字段

### 9.2 执行要求
- 发布后的 stream 必须可被 Connection 执行引擎直接调用
- 结果统一落入 execution result_payload
- 失败场景需保留上下文（请求窗口、响应摘要、错误信息）

### 9.3 可见性要求
- 前端必须可查看：
  - 最后执行状态
  - 错误信息
  - 预览表格（rows/columns）
  - 原始 JSON（raw_response）

---

## 10. 模块详细需求
### 10.1 平台与凭证管理
- 平台配置增删改查
- 凭证同步与 token 刷新
- 凭证状态可视化（ready/missing/partial）

### 10.2 Builder（接口开发台）
- Stream 发布管理（按平台筛选）
- 测试运行与结果预览
- 支持最低限度的字段推断

### 10.3 Connection/Project/Stream 管理
- 项目生命周期（创建、查看、状态控制）
- stream 批量配置
- routine 与 backfill 两类执行模式

### 10.4 执行中心
- 执行实例状态：PENDING/RUNNING/SUCCESS/FAILED
- 保存请求与结果快照
- 可按项目查看执行历史

### 10.5 目标库与留存策略
- 目标配置管理
- 本地托管文件管理（浏览、下载、删除）
- 留存策略（启停、保留天数、手动执行）

### 10.6 告警中心（新增，P0）
- 告警通道 CRUD
- 告警通道连通性测试
- 告警事件列表（按时间、项目、状态筛选）
- 执行失败自动触发告警投递

### 10.7 项目就绪度评估（新增，P0）
- 检查项：
  - 平台是否有效
  - 凭证是否可用
  - stream 是否完整
  - 调度配置是否有效
  - 目标是否可写
  - 告警通道是否可用
- 输出统一结构：
  - `ready` 总结论
  - `checks[]` 明细（PASS/WARN/FAIL）

---

## 11. 非功能需求
### 11.1 安全
- 凭证需采用生产可用加密方案（当前 XOR 仅用于开发阶段）
- 敏感字段日志脱敏
- Webhook URL 与 token 等敏感配置加密存储

### 11.2 稳定性
- 执行失败不可吞错
- 上游全部失败需标记 FAILED 并产出告警
- 调度线程池资源可控，关闭时可优雅收敛

### 11.3 可观测性
- 执行链路可追踪（请求窗口、结果、错误）
- 告警事件可审计
- 关键指标可导出（成功率、失败率、平均时长）

### 11.4 性能
- 单项目 execution 列表查询在 200 条规模下保持快速响应
- 预览 API 默认限流（如 `limit<=500`）

---

## 12. API 规划（增量）
### 12.1 告警中心
- `GET /api/v1/alerts/channels`
- `POST /api/v1/alerts/channels`
- `PUT /api/v1/alerts/channels/{id}`
- `POST /api/v1/alerts/channels/test`
- `GET /api/v1/alerts/events`

### 12.2 项目体检
- `GET /api/v1/connections/projects/{project_id}/readiness`

---

## 13. 里程碑计划
### M1（1 周）
- 外部接口执行链路打通（Builder 发布 -> Connection 执行）
- 执行结果统一可见

### M2（1 周）
- 告警中心（通道 + 事件 + 失败自动投递）

### M3（1 周）
- 项目就绪度评估
- 文档与验收用例完善

### M4（后续）
- 安全加固与质量规则
- 权限与审计

---

## 14. 验收标准（DoD）
- 至少 1 个外部平台 stream 从“配置 -> 发布 -> Run Now -> 预览”全链路成功
- 执行失败可在 1 分钟内触发告警，并在告警事件列表可查
- 项目 readiness 输出结构化检查结论，能清晰指示不可上线原因
- 关键 API 有单元测试覆盖并通过

---

## 15. 风险与应对
- 上游 API 变更风险：增加平台配置版本与灰度验证
- 鉴权失效风险：强化 token 刷新与失效告警
- 误配置风险：上线前强制 readiness 检查
- 运维噪音风险：告警去重与降噪策略

---

## 16. 代码改动记录规则（强制）
从本文件生效起，**后续所有代码改动必须同步记录到本 PRD**，记录位置为“17. 变更日志”。

每条记录至少包含：
- 日期时间（Asia/Shanghai）
- 需求/问题背景
- 改动文件列表
- 改动摘要（做了什么）
- 改动原因（为什么做）
- 验证结果（测试/手测）
- 影响范围与回滚说明

记录模板：

```md
### [YYYY-MM-DD HH:mm] 变更标题
- 背景：
- 改动文件：
  - /abs/path/a.py
  - /abs/path/b.tsx
- 改动摘要：
- 原因：
- 验证：
- 影响与回滚：
```

---

## 17. 变更日志
### [2026-03-25 00:00] PRD 初始化
- 背景：明确系统产品化目标，并约束后续开发过程文档化。
- 改动文件：
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/prd.md
- 改动摘要：创建完整 PRD（目标、范围、架构、模块、流程、里程碑、验收、风险、变更规则）。
- 原因：作为后续迭代的统一产品与实现基线。
- 验证：文档已落库，内容可读。
- 影响与回滚：仅新增文档，无运行时影响；删除该文件即可回滚。

### [2026-03-25 22:35] 应用与凭证 Token 刷新失败状态联动（前后端）
- 背景：在“应用与凭证”中，token 自动刷新失败时，前端列表未同步为失败状态，导致运维不可见。
- 改动文件：
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/webapp/services/token_refresh.py
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/webapp/routers/accounts.py
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/frontend/src/App.jsx
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/webapp/tests/test_token_refresh_failure_status.py
- 改动摘要：
  - 新增失败状态落库逻辑：`token_status=refresh_failed`，并写入 `last_refresh_at`、`last_error`。
  - 调度器自动刷新失败与手动刷新失败都写回凭证源（`api_credentials`）与 DB 配置，保证状态一致。
  - 前端单条/批量“更新 Token”失败时，列表行即时更新为 `refresh_failed`，避免仅 toast 提示导致状态丢失。
  - 凭证扫描接口补齐 DB 分支的 `token_status/token_updated_at` 透传，不再固定回退为 `ready/missing`。
- 原因：失败状态必须可观测、可追踪，避免误判凭证健康度。
- 验证：
  - `python -m unittest webapp.tests.test_token_refresh_failure_status` 通过。
  - `npm run build` 通过。
- 影响与回滚：
  - 影响“应用与凭证”Token 状态展示与刷新失败持久化逻辑。
  - 回滚可按文件级回退上述 4 个文件，恢复原有“仅日志/提示”的行为。

### [2026-03-25 22:56] Connection 编辑支持修改授权账号（app_id）
- 背景：Connection 的 `Edit` 仅支持编辑 stream 配置，不支持修改已绑定授权账号。
- 改动文件：
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/webapp/schemas.py
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/webapp/services/connections.py
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/webapp/routers/connections.py
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/frontend/src/App.jsx
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/frontend/src/components/modals/ConnectionWizardModal.jsx
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/webapp/tests/test_connection_project_app_ids.py
- 改动摘要：
  - 新增接口：`PUT /api/v1/connections/projects/{project_id}/app-ids`。
  - 新增请求模型：`SyncProjectAppIdsUpdateRequest`。
  - 新增服务方法：`update_project_app_ids`，对 `app_ids` 做去重规范化，并同步 `project.app_id/app_ids_json`。
  - 前端 Connection 编辑工作区新增“授权账号 (app_id)”可编辑区（搜索/多选/全选），保存时先更新账号绑定，再更新 stream 配置。
  - 编辑弹窗文案更新为“编辑 Connection Project”，明确支持账号编辑。
- 原因：授权账号是 Connection 核心建模边界，需支持在线变更以避免重新建项目。
- 验证：
  - `python -m unittest webapp.tests.test_connection_project_app_ids webapp.tests.test_token_refresh_failure_status` 通过。
  - `npm run build` 通过。
  - 本地 UI 验证：进入 `Connection -> Edit` 可见“授权账号 (app_id)”区块并可勾选。
- 影响与回滚：
  - 影响 Connection 编辑流程与项目账号绑定更新能力。
  - 回滚可删除新增接口与前端编辑区块，恢复“仅 stream 可编辑”模式。

### [2026-03-25 23:10] Connection Detail 的 Sync Mode 中文化展示
- 背景：Connection Detail 的 `Sync Mode` 列显示技术值（`incremental/full_refresh` 或大写形态），不符合业务阅读习惯。
- 改动文件：
  - /Users/xuminchen/Desktop/WonderLab工作文件/Project/wonderlab_api/frontend/src/App.jsx
- 改动摘要：
  - 新增 `syncModeLabel` 映射函数：
    - `incremental/INCREMENTAL` -> `增量同步`
    - `full_refresh/FULL_REFRESH` -> `全量覆盖`
  - Connection Detail 表格改为展示中文标签，不变更后端存储值。
- 原因：提升可读性，减少业务侧对内部枚举值的理解成本。
- 验证：
  - `npm run build` 通过。
  - 页面强制刷新后，`Sync Mode` 列显示“增量同步/全量覆盖”。
- 影响与回滚：
  - 仅前端展示层变化，无接口/数据结构变更。
  - 回滚可移除映射函数并恢复直接展示 `stream.sync_mode`。
