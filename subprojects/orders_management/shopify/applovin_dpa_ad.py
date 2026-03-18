# -*- coding: utf-8 -*-
import requests
from datetime import datetime, timedelta
import time
import gosql_v3
from subprojects._shared.core.api_credentials import get_credentials


class AppLovinReportingAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://r.applovin.com/report"

    def get_recent_days_range(self, days=2):
        """
        获取最近几天的日期范围
        默认返回 (前天, 昨天)
        """
        yesterday = datetime.now() - timedelta(days=1)
        start_day = datetime.now() - timedelta(days=days)

        return start_day.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")


    def get_report(self, start_date=None, end_date=None, report_type="advertiser", format_type="json", columns=None, having=None, limit=None, offset=None):
        """获取AppLovin报告数据"""

        params = {
            'api_key': self.api_key,
            'start': start_date,
            'end': end_date,
            'columns': ','.join(columns),
            'format': format_type,
            'report_type': report_type
        }

        if having:
            params['having'] = having
        if limit:
            params['limit'] = str(limit)
        if offset:
            params['offset'] = str(offset)

        try:
            response = requests.get(self.base_url, params=params, timeout=120)

            if response.status_code != 200:
                return None, response.text

            if format_type == "json":
                data = response.json()
                if isinstance(data, dict) and 'results' in data:
                    return data['results'], None
                else:
                    return None, str(data)
            else:
                return response.text, None

        except Exception as e:
            return None, str(e)

    def get_date_range_data_with_pagination(self, start_date, end_date, page_size=1000, max_pages=100000):
        """获取日期范围内所有数据（带分页和重试机制）"""
        columns = [
            # 基础维度字段
            "day", "campaign", "ad", "platform", "country", "device_type",

            # 核心表现指标
            "impressions", "clicks", "cost", "conversions", "sales",
            "ctr", "conversion_rate", "average_cpc", "average_cpa",

            # 广告系列信息
            "campaign_type", "campaign_ad_type", "campaign_id_external",
            "creative_set", "ad_creative_type", "ad_type", "placement_type",
            "creative_set_id",

            # 7天表现指标（保留，因为这是行业标准）
            "roas_7d", "total_rev_7d", "sales_7d",

            # 新增基础字段
            "bidding_and_billing_method",
            "campaign_bid_goal", "campaign_package_name", "campaign_roas_goal",
            "campaign_store_id", "custom_page_id", "external_placement_id",
            "first_purchase", "optimization_day_target", "size",
            "target_event", "target_event_count", "traffic_source",

            # 精选的0d时间序列指标 - 当天即时效果
            "ad_rev_0d",  # 当天广告收入 - 看即时变现效果
            "ad_roas_0d",  # 当天广告ROAS - 即时回报率
            "roas_0d",  # 当天总ROAS - 综合回报率
            "iap_rev_0d",  # 当天IAP收入 - 即时内购收入
            "iap_roas_0d",  # 当天IAP ROAS - 内购回报率
            "total_rev_0d",  # 当天总收入 - 总收益
            "sales_0d",  # 当天销售数量 - 转化量
            "cpp_0d",  # 当天每次购买成本 - 成本效率
            "unique_purchasers_0d",  # 当天唯一购买者 - 用户质量
        ]

        all_data = []
        current_page = 0
        total_records = 0

        while current_page < max_pages:
            offset = current_page * page_size
            print(f"正在获取 {start_date} 到 {end_date} 第 {current_page + 1} 页数据 (offset: {offset})...")

            data, error = self.get_report(
                start_date=start_date,
                end_date=end_date,
                columns=columns,
                limit=page_size,
                offset=offset
            )

            if error:
                print(f"{start_date} 到 {end_date} 第 {current_page + 1} 页获取失败: {error}")
                # 返回已获取的数据和错误信息，让外层决定是否重试
                return all_data, f"第{current_page + 1}页获取失败: {error}"

            if not data:
                print(f"{start_date} 到 {end_date} 第 {current_page + 1} 页没有数据，停止分页")
                break

            # 处理当前页数据
            page_records = []
            for item in data:
                data_dict = {
                    # === 基础信息 ===
                    "day": item.get("day", ""),
                    "campaign": item.get("campaign", ""),
                    "ad": item.get("ad", ""),
                    "platform": item.get("platform", ""),
                    "country": item.get("country", ""),
                    "device_type": item.get("device_type", ""),

                    # === 核心表现指标 ===
                    "impressions": item.get("impressions"),
                    "clicks": item.get("clicks"),
                    "cost": item.get("cost"),
                    "conversions": item.get("conversions"),
                    "sales": item.get("sales"),

                    # === 率值指标 ===
                    "ctr": item.get("ctr"),
                    "conversion_rate": item.get("conversion_rate"),
                    "average_cpc": item.get("average_cpc"),
                    "average_cpa": item.get("average_cpa"),

                    # === 广告系列信息 ===
                    "campaign_type": item.get("campaign_type", ""),
                    "campaign_ad_type": item.get("campaign_ad_type", ""),
                    "campaign_id_external": item.get("campaign_id_external", ""),
                    "creative_set": item.get("creative_set", ""),
                    "ad_creative_type": item.get("ad_creative_type", ""),
                    "ad_type": item.get("ad_type", ""),
                    "placement_type": item.get("placement_type", ""),
                    "creative_set_id": item.get("creative_set_id", ""),

                    # === 7天表现指标 ===
                    "roas_7d": item.get("roas_7d"),
                    "total_rev_7d": item.get("total_rev_7d"),
                    "sales_7d": item.get("sales_7d"),

                    # === 新增基础字段 ===
                    "app_id_external": item.get("app_id_external", ""),
                    "application": item.get("application", ""),
                    "bidding_and_billing_method": item.get("bidding_and_billing_method", ""),
                    "campaign_bid_goal": item.get("campaign_bid_goal"),
                    "campaign_package_name": item.get("campaign_package_name", ""),
                    "campaign_roas_goal": item.get("campaign_roas_goal"),
                    "campaign_store_id": item.get("campaign_store_id", ""),
                    "custom_page_id": item.get("custom_page_id", ""),
                    "external_placement_id": item.get("external_placement_id", ""),
                    "first_purchase": item.get("first_purchase"),
                    "optimization_day_target": item.get("optimization_day_target", ""),
                    "size": item.get("size", ""),
                    "target_event": item.get("target_event", ""),
                    "target_event_count": item.get("target_event_count"),
                    "traffic_source": item.get("traffic_source", ""),

                    # === 0d即时效果指标 ===
                    "ad_rev_0d": item.get("ad_rev_0d"),
                    "ad_roas_0d": item.get("ad_roas_0d"),
                    "roas_0d": item.get("roas_0d"),
                    "iap_rev_0d": item.get("iap_rev_0d"),
                    "iap_roas_0d": item.get("iap_roas_0d"),
                    "total_rev_0d": item.get("total_rev_0d"),
                    "sales_0d": item.get("sales_0d"),
                    "cpp_0d": item.get("cpp_0d"),
                    "unique_purchasers_0d": item.get("unique_purchasers_0d"),
                }
                page_records.append(data_dict)

            current_page_count = len(page_records)
            all_data.extend(page_records)
            total_records += current_page_count

            # print(
            #     f"{start_date} 到 {end_date} 第 {current_page + 1} 页获取到 {current_page_count} 条数据，累计 {total_records} 条")

            # 如果当前页数据少于page_size，说明已经是最后一页
            if current_page_count < page_size:
                print(f"{start_date} 到 {end_date} 数据已全部获取完毕，共 {total_records} 条")
                break

            current_page += 1

            # 添加延迟避免请求过于频繁
            time.sleep(1)

        return all_data, None

    def get_data_by_one_day_batches(self, start_date_str, end_date_str, max_retries_per_day=3):
        """按每天一个批次获取数据并入库（带重试机制）"""
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        current_date = start_date
        total_all_data = []

        batch_count = 0

        while current_date <= end_date:
            batch_count += 1
            current_date_str = current_date.strftime("%Y-%m-%d")

            # 获取当前批次数据（同一天），带重试机制
            retry_count = 0
            batch_data = None
            error_msg = None

            while retry_count <= max_retries_per_day:
                batch_data, error = self.get_date_range_data_with_pagination(current_date_str, current_date_str)

                if error:
                    retry_count += 1
                    if retry_count <= max_retries_per_day:
                        print(
                            f"❌ 第 {batch_count} 批次数据获取失败，等待10秒后重新获取整天的数据... (第 {retry_count} 次重试)")
                        time.sleep(10)
                        # 清空之前获取的数据，重新开始
                        batch_data = None
                    else:
                        error_msg = error
                        print(f"❌ 第 {batch_count} 批次数据获取失败，已达到最大重试次数: {error}")
                else:
                    break  # 成功获取数据，跳出重试循环

            if batch_data:
                print(f"✅ 第 {batch_count} 批次成功获取到 {len(batch_data)} 条数据")

            # 立即入库当前批次的数据
                first_sql = f"delete from applovin_dpa_ad_performance where day = '{current_date_str}'"
                gosql_v3.api_to_sql(batch_data, "applovin_dpa_ad_performance",first_execute_sql=first_sql)
                print(f"✅ 第 {batch_count} 批次数据已成功存入数据库")
                total_all_data.extend(batch_data)

            else:
                if error_msg:
                    print(f"❌ 第 {batch_count} 批次最终获取失败: {error_msg}")
                else:
                    print(f"⚠️ 第 {batch_count} 批次没有获取到数据")

            # 移动到下一天
            current_date += timedelta(days=1)

            # 批次之间的延迟
            if current_date <= end_date:
                print(f"等待3秒后处理下一天...")
                time.sleep(3)

        return total_all_data


def main():
    api_key = "11JVbL2KiyRKBVE617PWK7vqauxPMUJF75L_U32JIOMrOUHvXSMNXn50VCyWDZPUoLP1n92Te4kXQVTUTetye0"
    client = AppLovinReportingAPI(api_key)

    # 调用新封装的方法，获取前天和昨天的字符串
    # 这里的 start_date 会变成 T-2，end_date 会变成 T-1
    start_date, end_date = client.get_recent_days_range(days=2)

    # start_date = '2026-03-04'
    # end_date = '2026-03-04'  # 可以调整结束日期

    # 按每天批次查询并入库
    print(f"开始按每天批次获取数据，日期范围: {start_date} 到 {end_date}")
    all_data = client.get_data_by_one_day_batches(start_date, end_date)

    if all_data:
        requests.get(get_credentials("bi_refresh_urls", "ds_s7f19de86b1a541eab4e4b9a", required=True))

        print(f"\n✅ 所有批次数据获取完成！总共获取到 {len(all_data)} 条数据")



if __name__ == "__main__":
    # 运行数据采集
    main()