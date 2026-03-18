import json
import logging
import os

import requests

from AccessToken import AccessToken
from Utils import Utils
from star_requests import StarRequest
from subprojects._shared.core.api_credentials import get_credentials


class DouDataInterface:
    Access_Token_01 = ''
    Access_Token_02 = ''

    request_num = 0

    date_list = []

    request_failed_list = []

    def __init__(self):
        self.credential_accounts = self._load_dou_accounts()
        self.access_tokens = []
        token_provider = AccessToken()
        for index, account in enumerate(self.credential_accounts, start=1):
            token = token_provider.refresh_token_new(account["app_id"], account["secret"])
            self.access_tokens.append(token)
            setattr(self, "Access_Token_{0:02d}".format(index), token)

        LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
        DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
        today_time = Utils().get_now_time('%Y-%m-%d')
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
        os.makedirs(log_dir, exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(log_dir, 'dou_plus_log_' + today_time + '.log'),
            level=logging.INFO,
            format=LOG_FORMAT,
            datefmt=DATE_FORMAT,
        )

        for i in range(15):
            if i == 0:
                continue
            self.date_list.append(str(Utils().get_X_time_ago(i)))

    @staticmethod
    def _load_dou_accounts():
        configured = get_credentials("xhs_juguang", "dou_plus_accounts", default=[])
        accounts = []
        if isinstance(configured, dict):
            iterable = configured.items()
        elif isinstance(configured, list):
            iterable = ((str(item.get("name", "")), item) for item in configured if isinstance(item, dict))
        else:
            iterable = []

        for cfg_name, item in iterable:
            if not isinstance(item, dict):
                continue
            app_id = str(item.get("app_id", "")).strip()
            secret = str(item.get("secret", "")).strip()
            if app_id and secret:
                accounts.append({"app_id": app_id, "secret": secret, "name": str(cfg_name).strip()})

        if accounts:
            return accounts

        apps = get_credentials("xhs_juguang", "apps", default={})
        if isinstance(apps, dict):
            for name, item in apps.items():
                if not isinstance(item, dict):
                    continue
                lower_name = str(name).lower()
                if "dou" not in lower_name:
                    continue
                app_id = str(item.get("app_id", "")).strip()
                secret = str(item.get("secret", "")).strip()
                if app_id and secret:
                    accounts.append({"app_id": app_id, "secret": secret, "name": str(name)})

        if not accounts:
            raise ValueError("Missing xhs_juguang dou+ accounts in config/api_credentials.json")
        return accounts

    def get_account_info(self, access_token):
        account_string_id_list = []
        url = 'https://ad.oceanengine.com/open_api/oauth2/advertiser/get/'
        header = {"Content-Type": "application/json"}

        params = {
            "access_token": access_token
        }

        response = requests.get(url, headers=header, params=params).json()
        account_info_list = response.get('data').get('list')
        for account_info in account_info_list:
            account_string_id_list.append((account_info.get('account_string_id'), account_info.get('advertiser_name')))

        return account_string_id_list

    def get_order_list(self, account_info, access_token):
        order_list = []
        ad_list = []
        item_list = []

        url = 'https://api.oceanengine.com/open_api/v3.0/douplus/order/list/'

        header = {"Content-Type": "application/json",
                  "Access-Token": access_token}
        # 请求Body（字典形式储存）
        params = {
            "aweme_sec_uid": account_info[0],
            "page_size": 100,
            "page": 1

        }

        response_json = StarRequest().request_get(url, header, params)
        # print(response_json)
        data_list = response_json.get('data').get('order_list')

        result_order_list, result_ad_list, result_item_list = self.dou_order_process(data_list, account_info)
        order_list.extend(result_order_list)
        ad_list.extend(result_ad_list)
        item_list.extend(result_item_list)

        page_info = response_json.get('data').get('page_info')
        total_page = page_info.get('total_page')
        if total_page > 1:
            for page_num in range(total_page+1):
                if page_num < 1:
                    continue
                params['page'] = page_num
                data_json = StarRequest().request_get(url, header, params)
                data_list = data_json.get('data').get('order_list')
                result_order_list, result_ad_list, result_item_list = self.dou_order_process(data_list, account_info)
                order_list.extend(result_order_list)
                ad_list.extend(result_ad_list)
                item_list.extend(result_item_list)

        return order_list, ad_list, item_list

    def dou_order_process(self, data_list, account_info):
        result_list = []
        result_ad_list = []
        result_item_list = []

        for data in data_list:
            order = data.get('order')  # 订单信息
            if order is None:
                order = {}
            order_id = order.get('order_id')  # 订单id
            task_id = order.get('task_id')  # 订单id（页面上显示）
            scene_type = order.get('scene_type')  # 营销目标，枚举值：LIVE 直播、VIDEO 短视频
            order_create_time = order.get('order_create_time')  # 订单创建时间，时间格式yyyy-mm-dd hh:mm:ss
            task_status = order.get('task_status')  # 任务状态
            budget = order.get('budget')  # 投放金额，单位：分
            live_scene = order.get('live_scene')  # 直播场景

            ad_list = data.get('ad_list')  # 计划列表
            if ad_list is None:
                ad_list = []
            for ad_info in ad_list:
                ad_id = ad_info.get('ad_id')  # 计划ID
                ad_status = ad_info.get('ad_status')  # 计划状态
                is_fans = ad_info.get('is_fans')  # 是否粉丝必见计划
                ad_budget = ad_info.get('budget')  # 投放金额，单位：分
                external_action = ad_info.get('external_action')  # 优化目标
                delivery_time = ad_info.get('delivery_time')  # 期望投放时长（小时）
                bid_mode = ad_info.get('bid_mode')  # 出价模式
                cpa_bid = ad_info.get('cpa_bid')  # 转化出价，单位：分
                audience = ad_info.get('audience')  # 定向信息
                if audience is None:
                    audience = {}
                gender = audience.get('gender')  # 性别
                age = audience.get('age')  # 年龄
                if age is not None:
                    age = str(age)
                district = audience.get('district')  # 地域，枚举值：BUSINESS 商圈、COUNTRY 区县、PROVINCE省市
                city = audience.get('city')  # 城市
                if city is not None:
                    city = str(city)
                province = audience.get('province')  # 省份
                if province is not None:
                    province = str(province)
                interest_categories = audience.get('interest_categories')  # 兴趣类目词
                if interest_categories is not None:
                    interest_categories = str(interest_categories)
                author_pkgs = audience.get('author_pkgs')  # 抖音达人ID列表
                if author_pkgs is not None:
                    author_pkgs = str(author_pkgs)
                delivery_type = audience.get('delivery_type')  # 定向模式
                business = audience.get('business')  # 商圈
                if business is not None:
                    business = str(business)
                platform = audience.get('platform')  # 设备平台：IOS、安卓、不限
                if platform is not None:
                    platform = str(platform)

                result_ad_list.append((order_id, ad_id, ad_status, is_fans, ad_budget, external_action, delivery_time,
                        bid_mode, cpa_bid, gender, age, district, city, province, interest_categories, author_pkgs,
                        delivery_type, business, platform))

            item_info_list = data.get('item_info_list')  # 视频信息
            if item_info_list is None:
                item_info_list = []

            for item_info in item_info_list:
                aweme_author_name = item_info.get('aweme_author_name')  # 抖音号昵称
                aweme_author_unique_id = item_info.get('aweme_author_unique_id')  # 抖音号
                aweme_author_avatar = item_info.get('aweme_author_avatar')  # 抖音号头像
                if aweme_author_avatar is not None:
                    aweme_author_avatar = str(aweme_author_avatar)
                aweme_item_id = item_info.get('aweme_item_id')  # 视频ID
                aweme_item_title = item_info.get('aweme_item_title')  # 视频标题
                aweme_item_cover = item_info.get('aweme_item_cover')  # 视频封面
                if aweme_item_cover is not None:
                    aweme_item_cover = str(aweme_item_cover)

                result_item_list.append((order_id, aweme_author_name, aweme_author_unique_id, aweme_author_avatar,
                                        aweme_item_id, aweme_item_title, aweme_item_cover))

            live_room_info = data.get('live_room_info')  # 直播间信息
            if live_room_info is None:
                live_room_info = {}
            room_id = live_room_info.get('room_id')  # 直播间ID
            room_title = live_room_info.get('room_title')  # 直播间标题
            room_cover = live_room_info.get('room_cover')  # 直播间封面
            if room_cover is not None:
                room_cover = str(room_cover)
            room_status = live_room_info.get('room_status')  # 直播间状态

            result_list.append((account_info[0], account_info[1], order_id, task_id, scene_type, order_create_time,
                                task_status, budget, live_scene, room_id, room_title, room_cover, room_status))

        return result_list, result_ad_list, result_item_list

    def get_order_report_list(self, account_info, select_date, access_token):
        result_list = []
        url = 'https://api.oceanengine.com/open_api/v3.0/douplus/order/report/'

        header = {"Content-Type": "application/json",
                  "Access-Token": access_token}
        # 请求Params（字典形式储存）
        params = {
            "aweme_sec_uid": account_info[0],
            "stat_time": json.dumps({
                "begin_time": select_date,
                "end_time": select_date
            }),
            "group_by": json.dumps(["GROUP_BY_AD_ID"]),
            "page_size": 100,
            "page": 1
        }

        # print(json.dumps(params))

        response_json = StarRequest().request_get(url, header, params)
        # print(response_json)
        data_list = response_json.get('data').get('order_metrics')

        result_list.extend(self.order_report_data_process(data_list, account_info, select_date))

        page_info = response_json.get('data').get('page_info')
        total_page = page_info.get('total_page')
        if total_page > 1:
            for page_num in range(total_page+1):
                if page_num < 1:
                    continue
                params['page'] = page_num
                data_json = StarRequest().request_get(url, header, params)
                data_list = data_json.get('data').get('order_metrics')

                result_list.extend(self.order_report_data_process(data_list, account_info, select_date))

        return result_list

    def order_report_data_process(self, data_list, account_info, select_date):
        result_list = []
        for data in data_list:
            dimension_data = data.get('dimension_data')  # 数据纬度
            if dimension_data is None:
                dimension_data = {}
            order_id = dimension_data.get('order_id')  # 订单ID
            advertiser_id = dimension_data.get('advertiser_id')  # 账户ID
            ad_id = dimension_data.get('ad_id')  # 计划ID
            is_fans = dimension_data.get('is_fans')  # 是否粉丝必见计划
            item_id = dimension_data.get('item_id')  # 视频ID
            room_id = dimension_data.get('room_id')  # 直播间ID

            metrics_data = data.get('metrics_data')  # 指标值
            if metrics_data is None:
                metrics_data = {}
            stat_cost = metrics_data.get('stat_cost')  # 消耗
            total_play = metrics_data.get('total_play')  # 播放次数
            dy_follow = metrics_data.get('dy_follow')  # 新增粉丝数
            dy_share = metrics_data.get('dy_share')  # 分享次数
            dy_comment = metrics_data.get('dy_comment')  # 评论次数
            custom_like = metrics_data.get('custom_like')  # 点赞次数
            dy_home_visited = metrics_data.get('dy_home_visited')  # 主页访问次数
            play_duration_5s_rank = metrics_data.get('play_duration_5s_rank')  # 5s完播率
            dp_target_convert_cnt = metrics_data.get('dp_target_convert_cnt')  # 转化数
            custom_convert_cost = metrics_data.get('custom_convert_cost')  # 转化成本
            show_cnt = metrics_data.get('show_cnt')  # 直播间展示数
            live_click_source_cnt = metrics_data.get('live_click_source_cnt')  # 直播间新增观众数
            live_gift_uv = metrics_data.get('live_gift_uv')  # 直播间打赏观众人数
            live_gift_amount = metrics_data.get('live_gift_amount')  # 直播间音浪收入
            live_comment_cnt = metrics_data.get('live_comment_cnt')  # 直播间评论次数
            douplus_live_follow_count = metrics_data.get('douplus_live_follow_count')  # 直播间新增粉丝量
            live_gift_cnt = metrics_data.get('live_gift_cnt')  # 直播间打赏次数

            result_list.append((account_info[0], account_info[1], select_date, order_id, advertiser_id, ad_id, is_fans, item_id,
                                room_id, stat_cost, total_play, dy_follow, dy_share, dy_comment, custom_like,
                                dy_home_visited, play_duration_5s_rank, dp_target_convert_cnt, custom_convert_cost,
                                show_cnt, live_click_source_cnt, live_gift_uv, live_gift_amount, live_comment_cnt,
                                douplus_live_follow_count, live_gift_cnt))

        return result_list

    def start_account(self, token):
        order_info_sql = '''
        INSERT INTO dou_order_info (account_string_id, advertiser_name, order_id, task_id, scene_type, order_create_time,
            task_status, budget, live_scene, room_id, room_title, room_cover, room_status)
            values %s
            on conflict (order_id) do update set 
            account_string_id = excluded.account_string_id, 
            advertiser_name = excluded.advertiser_name,
            task_id = excluded.task_id,
            scene_type = excluded.scene_type,
            order_create_time = excluded.order_create_time,
            task_status = excluded.task_status,
            budget = excluded.budget,
            live_scene = excluded.live_scene,
            room_id = excluded.room_id,
            room_title = excluded.room_title,
            room_cover = excluded.room_cover,
            room_status = excluded.room_status
        '''

        order_info_ad_sql = '''
        INSERT INTO dou_order_ad_info (order_id, ad_id, ad_status, is_fans, ad_budget, external_action, delivery_time, 
            bid_mode, cpa_bid, gender, age, district, city, province, interest_categories, author_pkgs, delivery_type, 
            business, platform)
            values %s
            on conflict (order_id, ad_id) do update set 
            
            ad_status = excluded.ad_status,
            is_fans = excluded.is_fans,
            ad_budget = excluded.ad_budget,
            external_action = excluded.external_action,
            delivery_time = excluded.delivery_time,
            bid_mode = excluded.bid_mode,
            cpa_bid = excluded.cpa_bid,
            gender = excluded.gender,
            age = excluded.age,
            district = excluded.district,
            city = excluded.city,
            province = excluded.province,
            interest_categories = excluded.interest_categories,
            author_pkgs = excluded.author_pkgs,
            delivery_type = excluded.delivery_type,
            business = excluded.business,
            platform = excluded.platform
        '''

        order_info_item_sql = '''
        INSERT INTO dou_order_item_info (order_id, aweme_author_name, aweme_author_unique_id, aweme_author_avatar, 
        aweme_item_id, aweme_item_title, aweme_item_cover)
            values %s
            on conflict (order_id, aweme_item_id, aweme_author_unique_id) do update set 
            aweme_author_name = excluded.aweme_author_name,
            aweme_author_avatar = excluded.aweme_author_avatar,
            aweme_item_id = excluded.aweme_item_id,
            aweme_item_title = excluded.aweme_item_title,
            aweme_item_cover = excluded.aweme_item_cover
        '''

        order_report_sql = '''
        INSERT INTO dou_order_report_data (account_string_id, advertiser_name, busi_date, order_id, advertiser_id, ad_id, is_fans, item_id, room_id,
            stat_cost, total_play, dy_follow, dy_share, dy_comment, custom_like, dy_home_visited, play_duration_5s_rank,
            dp_target_convert_cnt, custom_convert_cost, show_cnt, live_click_source_cnt, live_gift_uv, live_gift_amount,
            live_comment_cnt, douplus_live_follow_count, live_gift_cnt)
            values %s
            on conflict (order_id, busi_date, advertiser_id, ad_id) do update set 
            account_string_id = excluded.account_string_id,
            advertiser_name = excluded.advertiser_name,
            is_fans = excluded.is_fans,
            item_id = excluded.item_id,
            room_id = excluded.room_id,
            stat_cost = excluded.stat_cost,
            total_play = excluded.total_play,
            dy_follow = excluded.dy_follow,
            dy_share = excluded.dy_share,
            dy_comment = excluded.dy_comment,
            custom_like = excluded.custom_like,
            dy_home_visited = excluded.dy_home_visited,
            play_duration_5s_rank = excluded.play_duration_5s_rank,
            dp_target_convert_cnt = excluded.dp_target_convert_cnt,
            custom_convert_cost = excluded.custom_convert_cost,
            show_cnt = excluded.show_cnt,
            live_click_source_cnt = excluded.live_click_source_cnt,
            live_gift_uv = excluded.live_gift_uv,
            live_gift_amount = excluded.live_gift_amount,
            live_comment_cnt = excluded.live_comment_cnt,
            douplus_live_follow_count = excluded.douplus_live_follow_count,
            live_gift_cnt = excluded.live_gift_cnt
        '''

        account_id_list = self.get_account_info(token)
        for account_info in account_id_list:
            # 查询订单列表
            order_info_list, order_info_ad_list, order_info_item_list = self.get_order_list(account_info, token)
            # 订单信息落库
            Utils().conn_pgdb(order_info_sql, Utils().data_check(order_info_list))
            # 订单下计划信息落库
            Utils().conn_pgdb(order_info_ad_sql, Utils().data_check(order_info_ad_list))
            # 订单下视频信息落库，因抖音号头像变动，需要手动去重
            order_info_item_check_list = []
            order_info_item_check_dict = {}
            for order_info_item in order_info_item_list:
                if (order_info_item[0], order_info_item[4]) not in order_info_item_check_list:
                    order_info_item_check_list.append((order_info_item[0], order_info_item[4]))
                    order_info_item_check_dict[(order_info_item[0], order_info_item[4])] = order_info_item
            Utils().conn_pgdb(order_info_item_sql, order_info_item_check_dict.values())
            # 获取订单数据报表
            for date in self.date_list:
                order_report_list = self.get_order_report_list(account_info, date, token)
                Utils().conn_pgdb(order_report_sql, Utils().data_check(order_report_list))

            print('开始落库')

    def start(self):
        ddi = self
        # 财遇见你
        logging.info("================== Dou+程序开始执行 ==================")
        for idx, token in enumerate(self.access_tokens, start=1):
            ddi.start_account(token)
            account_name = self.credential_accounts[idx - 1].get("name") or "dou_plus_account_{0:02d}".format(idx)
            logging.info("================== Dou+程序【%s】执行完成 ==================", account_name)
        logging.info("================== Dou+程序执行完成 ==================")

if __name__ == '__main__':
    # DouDataInterface().get_order_list('')
    # DouDataInterface().get_account_info(DouDataInterface().Access_Token_01)
    # DouDataInterface().get_order_report_list('', '2024-04-10')
    DouDataInterface().start()
