# WonderLab API 模块架构（精简版）

## 总体运行模式
核心链路：
1. 定时触发（crontab/shell）
2. 调用外部平台 API 拉取数据
3. 清洗/转换
4. 写入数据库
5. 触发 BI 刷新或通知

## ads_report 广告报表聚合域
- 目标：统一编排多渠道广告平台报表任务。
- 规范入口：`subprojects/ads_report/project/unified_runner.py`
- 关键 profile：`core`、`global`、`all`
- 主要代码目录：`subprojects/ads_report/red`、`subprojects/ads_report/xingtu`、`subprojects/ads_report/TencentAdvertised`、`subprojects/ads_report/meta`

## orders_management 订单管理域
- 目标：统一编排 TikTok/微信小店/Kwai/Shopify 订单与交易相关 API 流程。
- 规范入口：`subprojects/orders_management/project/unified_runner.py`
- 关键 profile：`core`、`orders`、`external`、`all`
- 主要代码目录：`subprojects/orders_management/TiktokShop`、`subprojects/orders_management/tiktok_package`、`subprojects/orders_management/wechatShop`、`subprojects/orders_management/kwai_v3`、`subprojects/orders_management/shopify`

## 服务调用层
- 服务封装：`subprojects/gateway/module_gateway.py`
- 用途：统一调用 `ads_report`、`orders_management`（支持 dry-run 和可控真实执行）
