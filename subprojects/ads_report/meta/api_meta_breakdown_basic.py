# -*- coding: utf-8 -*-
import requests  # 用于发送HTTP网络请求
import time  # 用于处理时间延迟和休眠
import gosql_v3  # 自定义的数据库操作模块
from datetime import datetime, timedelta  # 用于处理日期和时间计算
from requests.adapters import HTTPAdapter  # 用于配置HTTP适配器
from urllib3.util.retry import Retry  # 用于配置网络重试策略
from subprojects._shared.core.api_credentials import get_credentials

# ==================== 全局配置信息 ====================
# Facebook Graph API 的访问令牌 (Access Token)
TOKEN = "EAAUE5mrETGoBQKXTkzRoamSReLi5xzW5ScBiEDjoFiuO6PToiqrQF8zHPZCWNtAm443ufhPSY0Mz0ejrCUKpJ2Kvzn3lt3nSVZBkfLnyZCsgDDXqp5z7f2ql5qaCIJGqS7kfaaZAUMcDiAiIsI9FfH9O4AYkKoG47lsZAIbH5jRsgVBZBZBNmjJUGsZB49weaQZDZD"

# 数据库表名映射字典：根据不同的细分维度(breakdown)决定存入哪张表
# 键(Key)是API的breakdown参数，值(Value)是数据库中的表名
TABLE_MAPPING = {
    "age": "api_meta_breakdown_age",  # 年龄维度数据表
    "gender": "api_meta_breakdown_gender",  # 性别维度数据表
    "region": "api_meta_breakdown_region"  # 地区维度数据表
}

# 请求API时需要获取的基础字段列表
# 注意：actions 和 action_values 是嵌套字段，包含具体的转化和互动数据
API_FIELDS = [
    "campaign_name", "adset_name", "ad_name", "date_start", "objective",
    "spend", "impressions", "actions", "action_values"
]

# ==================== 数据库工具函数 ====================
def safe_database_insert(data, table_name, max_retries=3, first_execute_sql=None):
    """
    功能：安全的数据库插入函数，包含错误重试机制。
    参数：
      - data: 要插入的数据列表
      - table_name: 目标表名
      - max_retries: 最大重试次数，默认3次
      - first_execute_sql: 插入前优先执行的SQL语句（通常用于删除旧数据）
    """
    # 开始循环尝试，最多尝试 max_retries 次
    for attempt in range(max_retries):
        try:
            # 如果数据存在
            if data:
                # 如果提供了预执行SQL（如DELETE语句），则先执行它
                if first_execute_sql:
                    gosql_v3.api_to_sql(data, table_name, first_execute_sql=first_execute_sql)
                else:
                    # 否则直接插入数据
                    gosql_v3.api_to_sql(data, table_name)
                return True  # 插入成功，返回True
            return False  # 数据为空，返回False
        except Exception as e:
            # 捕获异常
            if attempt < max_retries - 1:
                # 如果还没达到最大重试次数，计算等待时间
                wait_time = 2 * (attempt + 1)
                print(f"数据库插入失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                print(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)  # 休眠等待
            else:
                # 达到最大重试次数，打印错误并放弃
                print(f"数据库插入失败，已达到最大重试次数 {max_retries}")
                return False
    return False


# ==================== 网络工具函数 ====================
def create_session():
    """
    功能：创建一个带有自动重试机制的 requests 会话。
    作用：增强网络请求的稳定性，遇到50x错误自动重试。
    """
    session = requests.Session()  # 创建会话对象
    # 定义重试策略：总共重试5次，重试间隔指数增长，针对500/502/503/504错误强制重试
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    # 创建HTTP适配器
    adapter = HTTPAdapter(max_retries=retries)
    # 将适配器挂载到 https 和 http 协议上
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session  # 返回配置好的会话


# 初始化全局会话对象
session = create_session()


def get_ad_accounts():
    """
    功能：获取当前Token下的广告账户列表。
    """
    url = "https://graph.facebook.com/v20.0/me/adaccounts"  # 接口地址
    params = {"fields": "id,name", "access_token": TOKEN}  # 请求参数
    try:
        # 发送GET请求，超时时间30秒
        response = session.get(url, params=params, timeout=120)
        # 解析JSON并返回 data 列表，如果出错返回空列表
        return response.json().get('data', [])
    except Exception as e:
        # 打印错误信息
        print(f"获取账户列表失败: {e}")
        return []


def parse_complex_actions(item):
    """
    功能：核心解析函数。从复杂的 actions 列表中提取具体的指标。
    新增了：互动(Engagement)、内容查看(View Content)及对应价值。
    """
    # 初始化结果字典，所有指标默认值为 0
    res = {
        # --- 基础电商指标 ---
        "link_clicks": 0,  # 链接点击
        "adds_to_cart": 0,  # 加入购物车
        "checkouts_initiated": 0,  # 发起结账
        "landing_page_views": 0,  # 落地页浏览
        "website_purchases": 0,  # 网站购买(次数)
        "website_purchases_value": 0.0,  # 网站购买(金额)

        # --- 视频指标 ---
        "video_view": 0,  # 视频观看(3秒)
        "video_play": 0,  # 视频播放(开始)
        "video_25": 0, "video_50": 0, "video_75": 0, "video_95": 0, "video_100": 0,  # 视频进度

        # --- 新增：互动指标 (Engagement) ---
        "page_engagement": 0,  # 主页互动 (Page Engagement)
        "post_engagement": 0,  # 帖子互动 (Post Engagement)
        "post_reaction": 0,  # 帖子心情/反应 (Post Reaction)
        "like": 0,  # 主页点赞 (Page Likes)
        "post_net_like": 0,  # 净增点赞 (onsite_conversion.post_net_like)
        "post_interaction_gross": 0,  # 总互动数 (post_interaction_gross)

        # --- 新增：内容查看指标 (View Content) ---
        "view_content": 0,  # 查看内容 (View Content)
        "omni_view_content": 0,  # 全渠道查看内容 (Omni View Content)


        # --- 新增：其他细分技术指标 ---
        "leads_view_content": 0,  # 线索类查看 (offsite_content_view_add_meta_leads)

        # --- 新增：内容查看价值 ---
        "view_content_value": 0.0  # 查看内容产生的价值 (Value)
    }

    # 1. 遍历 actions 列表 (处理次数/计数)
    if 'actions' in item:
        for act in item['actions']:
            val = int(act.get('value', 0))  # 获取计数值
            atype = act.get('action_type')  # 获取动作类型名称

            # --- 原有逻辑：点击与转化 ---
            if atype == 'link_click':
                res["link_clicks"] = val
            elif 'add_to_cart' in atype:
                res["adds_to_cart"] = val
            elif 'initiate_checkout' in atype:
                res["checkouts_initiated"] = val
            elif 'landing_page_view' in atype:  # 包含 omni_landing_page_view 等
                res["landing_page_views"] = val
            elif 'purchase' in atype:
                res["website_purchases"] = val

            # --- 新增逻辑：互动类 (Engagement) ---
            elif atype == 'page_engagement':
                res["page_engagement"] = val
            elif atype == 'post_engagement':
                res["post_engagement"] = val
            elif atype == 'post_reaction':
                res["post_reaction"] = val
            elif atype == 'like':
                res["like"] = val
            elif atype == 'onsite_conversion.post_net_like':
                res["post_net_like"] = val
            elif atype == 'post_interaction_gross':
                res["post_interaction_gross"] = val

            # --- 新增逻辑：内容查看类 (View Content) ---
            elif atype == 'view_content':
                res["view_content"] = val
            elif atype == 'omni_view_content':
                res["omni_view_content"] = val

            elif atype == 'offsite_content_view_add_meta_leads':
                res["leads_view_content"] = val

            # --- 原有逻辑：视频播放 ---
            elif atype == 'video_view':
                res["video_view"] = val
            elif atype == 'video_play':
                res["video_play"] = val
            elif 'video_p25' in atype:
                res["video_25"] = val
            elif 'video_p50' in atype:
                res["video_50"] = val
            elif 'video_p75' in atype:
                res["video_75"] = val
            elif 'video_p95' in atype:
                res["video_95"] = val
            elif 'video_p100' in atype:
                res["video_100"] = val

    # 2. 遍历 action_values 列表 (处理金额/价值)
    if 'action_values' in item:
        for v in item['action_values']:
            atype = v.get('action_type', '')  # 获取动作类型
            val = float(v.get('value', 0))  # 获取金额值

            # 提取购买金额
            if 'purchase' in atype:
                res["website_purchases_value"] = val

            # --- 新增：提取 View Content 金额 ---
            # 匹配 item 中的 'offsite_conversion.fb_pixel_view_content' 或其他变体
            # 这里使用 broad match 'view_content' 来捕获所有相关价值，或者你可以指定 precise match
            elif 'view_content' in atype:
                # 注意：如果多个view_content类型都有值，这里会覆盖，通常取最大的或最后的一个
                # 在你的数据中，它们都是 '118'，所以直接赋值即可
                res["view_content_value"] = val

    return res


def get_insights_async(account_id, breakdown, start_date, end_date):
    """
    功能：向Facebook API发送异步报表请求，并轮询等待结果。
    参数：
      - account_id: 广告账户ID
      - breakdown: 分组维度 (age/gender/region)
      - start_date/end_date: 时间范围
    """
    base_url = f"https://graph.facebook.com/v20.0/{account_id}/insights"  # 基础API路径

    # 构造API所需的 JSON 格式时间范围字符串
    time_range = f"{{'since':'{start_date}','until':'{end_date}'}}"

    # 组装请求参数
    params = {
        "fields": ",".join(API_FIELDS),  # 请求的字段列表
        "level": "ad",  # 数据层级：具体到每条广告(ad)
        "breakdowns": breakdown,  # 分组维度
        "time_increment": 1,  # 按天分拆数据
        "time_range": time_range,  # 时间范围
        "access_token": TOKEN  # 认证Token
    }

    print(f"提交 {breakdown} 异步任务 ({start_date} 至 {end_date})...")

    try:
        # 发送POST请求创建异步任务
        resp = session.post(base_url, params=params, timeout=30).json()

        # 检查是否成功返回 report_run_id
        if 'report_run_id' not in resp:
            print(f"提交失败: {resp}")
            return []

        report_run_id = resp['report_run_id']  # 获取任务ID
        check_url = f"https://graph.facebook.com/v20.0/{report_run_id}"  # 状态检查URL

        # --- 轮询等待任务完成 ---
        while True:
            # 检查任务状态
            status_resp = session.get(check_url, params={"access_token": TOKEN}, timeout=120).json()
            percent = status_resp.get('async_percent_completion', 0)  # 进度百分比
            status = status_resp.get('async_status', '')  # 任务状态

            if status == 'Job Completed': break  # 任务完成，跳出循环
            if status in ['Job Failed', 'Job Skipped']:  # 任务失败
                print(f"任务失败: {status_resp}")
                return []
            time.sleep(2)  # 任务未完成，休眠2秒后再次检查

        # --- 获取数据结果 ---
        result_url = f"https://graph.facebook.com/v20.0/{report_run_id}/insights"  # 结果下载URL
        results = []  # 存储最终数据的列表
        data_resp = session.get(result_url, params={"access_token": TOKEN}, timeout=60).json()

        # --- 处理分页数据 ---
        while True:
            if 'data' in data_resp:
                for item in data_resp['data']:
                    # 1. 提取基础维度字段
                    row = {
                        "date": item.get("date_start"),  # 日期
                        "account_id": account_id,  # 账户ID
                        "campaign_name": item.get("campaign_name"),  # 广告系列名
                        "ad_set_name": item.get("adset_name"),  # 广告组名
                        "ad_name": item.get("ad_name"),  # 广告名
                        "objective": item.get("objective"),  # 广告目标
                        "breakdown_type": breakdown,  # 维度类型
                        "breakdown_value": item.get(breakdown),  # 维度值
                        "amount_spend": float(item.get("spend", 0)),  # 花费
                        "impressions": int(item.get("impressions", 0))  # 展示数
                    }
                    # 2. 调用解析函数，提取互动、转化等复杂指标
                    row.update(parse_complex_actions(item))

                    # 3. 将处理好的行加入结果列表
                    results.append(row)

                # 4. 检查是否有下一页 (Pagination)
                if 'paging' in data_resp and 'next' in data_resp['paging']:
                    # 分页请求重试机制
                    max_retries = 3
                    for i in range(max_retries):
                        try:
                            # 请求下一页数据
                            data_resp = session.get(data_resp['paging']['next'], timeout=60).json()
                            break  # 成功则跳出重试循环
                        except Exception as e:
                            print(f"分页请求波动，第 {i + 1} 次重试... {e}")
                            time.sleep(2)
                            if i == max_retries - 1: raise e  # 最后一次重试失败则抛出异常
                else:
                    break  # 没有下一页，结束循环
            else:
                break  # 数据为空，结束循环
        return results
    except Exception as e:
        print(f"获取 {breakdown} 数据网络错误: {e}")
        return []


def main():
    """
    主程序入口
    """
    # 1. 设置时间范围 (逻辑：获取过去7天的数据)
    end_date_obj = datetime.now()
    start_date_obj = end_date_obj - timedelta(days=3)  # 获取最近7天(包含今天)
    end_date_str = end_date_obj.strftime("%Y-%m-%d")  # 格式化结束日期
    start_date_str = start_date_obj.strftime("%Y-%m-%d")  # 格式化开始日期
    # start_date_str ='2024-01-01'
    # end_date_str = '2025-12-20'
    print(f"启动 Meta 异步抓取工具 | 范围: {start_date_str} 到 {end_date_str}")

    # 获取账户列表
    accounts = get_ad_accounts()
    if not accounts:
        print("未获取到广告账户")
        return

    # 2. 遍历每一个广告账户
    for acc in accounts:
        print(f"\n======== 处理账户: {acc['name']} ({acc['id']}) ========")

        # 3. 遍历三个维度 (Age, Gender, Region)
        for breakdown_key, table_name in TABLE_MAPPING.items():

            # 调用异步函数获取数据
            processed_data = get_insights_async(acc['id'], breakdown_key, start_date_str, end_date_str)

            # 如果有数据返回，进行入库操作
            if processed_data:
                print(f"{breakdown_key} 数据获取成功: {len(processed_data)} 行")

                # 构建删除 SQL (幂等性设计：删除当前账户、当前时间段内的数据，防止重复插入)
                delete_sql = f"DELETE FROM {table_name} WHERE date >= '{start_date_str}' AND account_id = '{acc['id']}'"

                # 执行入库，传入删除SQL作为前置操作
                if safe_database_insert(
                        data=processed_data,
                        table_name=table_name,
                        max_retries=3,
                        first_execute_sql=delete_sql
                ):
                    print(f"{breakdown_key} 数据已入库:{table_name}")
                else:
                    print(f"{breakdown_key} 数据入库失败")
            else:
                print(f"{breakdown_key} 无数据返回")

    print("所有任务执行完毕")
    requests.get(
        get_credentials("bi_refresh_urls", "ds_ffdacbfee35974178be00cf1", required=True))
    requests.get(
        get_credentials("bi_refresh_urls", "ds_f5fcb3010438d4c8e9fa2851", required=True))
    requests.get(
        get_credentials("bi_refresh_urls", "ds_k0e7d85fec89e45d98d40ae4", required=True))


if __name__ == "__main__":
    main()