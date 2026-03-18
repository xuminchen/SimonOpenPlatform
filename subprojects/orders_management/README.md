# 订单管理域（orders_management）

## 模块定位
统一承载订单与交易相关 API 流程，覆盖 TikTok Shop、微信小店、Kwai 与 Shopify。

## 子工程统一入口
- 规范实现：`subprojects/orders_management/project/unified_runner.py`
- 包装脚本：`subprojects/orders_management/run.sh`
- 示例：`bash subprojects/orders_management/run.sh dry-run core`

## 主要任务分组
- 订单核心链路：`core`、`orders`
- 外部渠道链路：`external`（Kwai + Shopify）
- 全量预设：`all`

## 建议入口命令（示例）
- `bash subprojects/orders_management/run.sh orders`
- `bash subprojects/orders_management/run.sh external`
- `bash subprojects/orders_management/run.sh dry-run all`

## 代码来源目录
- `subprojects/orders_management/TiktokShop/**/*.py`
- `subprojects/orders_management/tiktok_package/*.py`
- `subprojects/orders_management/wechatShop/*.py`
- `subprojects/orders_management/kwai_v3/*.py`
- `subprojects/orders_management/shopify/*.py`
