# modules 对外调用层

该目录是 WonderLab 功能模块的统一对外调用入口。

## 目标
- 外部系统只依赖 `modules/`，不直接耦合内部脚本目录。
- 每个功能域一个独立包，便于后续网站/API服务按模块接入。

## 包列表
- `modules.orders_management`
- `modules.ads_report`

## 通用能力
每个子包都暴露以下函数：
- `info()`
- `list_profiles()`
- `list_tasks()`
- `run(...)`
- `run_profile(profile, dry_run=True, timeout_seconds=1800)`
- `run_task(task_id, dry_run=True, timeout_seconds=1800)`

## 使用示例
```python
from modules import ads_report, get_module

# 1) 直接按子包调用
print(ads_report.list_profiles())
res = ads_report.run_profile("core", dry_run=True)
print(res["ok"], res["return_code"])

# 2) 动态按名称调用
mod = get_module("orders_management")
print(mod.list_tasks())
res = mod.run_task("tiktokshop_orders", dry_run=True)
print(res["duration_seconds"])
```

## 说明
- `dry_run=True` 时只做执行预演，不运行真实业务脚本。
- 真实执行请设置 `dry_run=False`，并确保依赖环境完整。
