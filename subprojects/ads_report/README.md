# 广告报表聚合域（ads_report）

## 模块定位
统一承载广告投放相关 API 报表链路。

## 子工程统一入口
- 规范实现：`subprojects/ads_report/project/unified_runner.py`
- 包装脚本：`subprojects/ads_report/run.sh`
- 示例：`bash subprojects/ads_report/run.sh dry-run all`

## 主要任务分组
- 多渠道广告核心：`core`
- 全局广告链路：`global`

## 建议入口命令（示例）
- `bash subprojects/ads_report/run.sh core`
- `bash subprojects/ads_report/run.sh global`
- `bash subprojects/ads_report/run.sh dry-run all`

## 代码来源目录
- `subprojects/ads_report/xingtu/*.py`
- `subprojects/ads_report/TencentAdvertised/*.py`
- `subprojects/ads_report/meta/*.py`
