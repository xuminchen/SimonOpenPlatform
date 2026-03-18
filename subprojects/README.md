# 子工程功能拆分（API 重构后）

说明：本目录聚焦 API 请求能力，当前仅保留 2 个业务模块。

运行说明：见 `subprojects/WORKSPACE.md`，每个模块目录含可执行 `run.sh` 与 `requirements.txt`。  
当前状态：`ads_report`、`orders_management` 两个 API 模块均提供统一 runner。  
配置状态：任务清单均配置在 `subprojects/<module>/project/tasks.toml`。
凭据状态：API 密钥统一读取 `config/api_credentials.json`（模板见 `config/api_credentials.example.json`）。

## ads_report 广告报表聚合域
- 定位：合并原千川与多渠道广告，统一广告投放 API 报表链路。
- 文档：`subprojects/ads_report/README.md`
- 清单：`subprojects/ads_report/FILES.txt`

## orders_management 订单管理域
- 定位：抖店/TikTok Shop/微信小店订单相关 API 拉取与同步。
- 文档：`subprojects/orders_management/README.md`
- 清单：`subprojects/orders_management/FILES.txt`
