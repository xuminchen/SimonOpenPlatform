import logging
import os

import requests

from AccessToken import AccessToken
from Utils import Utils
from star_requests import StarRequest
from subprojects._shared.core.api_credentials import get_credentials


class DataInterface:

    Access_Token = ''

    request_num = 0

    date_list = []

    request_failed_list = []

    def __init__(self):
        self.credential_accounts = self._load_star_accounts()
        self.access_tokens = []
        token_provider = AccessToken()
        for index, account in enumerate(self.credential_accounts, start=1):
            token = token_provider.refresh_token_new(account["app_id"], account["secret"])
            self.access_tokens.append(token)
            # Backward compatibility for existing script call-sites.
            setattr(self, "Access_Token_{0:02d}".format(index), token)

        LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
        DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
        today_time = Utils().get_now_time('%Y-%m-%d')
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
        os.makedirs(log_dir, exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(log_dir, 'xt_log_' + today_time + '.log'),
            level=logging.INFO,
            format=LOG_FORMAT,
            datefmt=DATE_FORMAT,
        )

        for i in range(8):
            if i == 0:
                continue
            self.date_list.append(str(Utils().get_X_time_ago(i)))

    @staticmethod
    def _load_star_accounts():
        configured = get_credentials("xhs_juguang", "star_accounts", default=[])
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

        # Fallback for legacy configs: read from xhs_juguang.apps and exclude douplus/demo entries.
        apps = get_credentials("xhs_juguang", "apps", default={})
        if isinstance(apps, dict):
            for name, item in apps.items():
                if not isinstance(item, dict):
                    continue
                lower_name = str(name).lower()
                if "dou" in lower_name:
                    continue
                app_id = str(item.get("app_id", "")).strip()
                secret = str(item.get("secret", "")).strip()
                if app_id and secret:
                    accounts.append({"app_id": app_id, "secret": secret, "name": str(name)})

        if not accounts:
            raise ValueError("Missing xhs_juguang star accounts in config/api_credentials.json")
        return accounts

    # 获取帐户授权下可用的star_id
    def get_advertiser_id(self, app_id, app_secret, access_token):

        advertiser_id_list = []

        # 请求地址url
        url = 'https://ad.oceanengine.com/open_api/oauth2/advertiser/get/'
        # 请求Header（字典形式储存）
        header = {"Content-Type": "application/json"}
        # 请求Body（字典形式储存）
        params = {
            "access_token": access_token,
            "app_id": app_id,
            "secret": app_secret
        }

        # 发送GET请求
        r = requests.get(url, params=params, headers=header)
        data_json = r.json()
        print(r.text)
        data_list = data_json.get('data').get('list')
        for data in data_list:
            account_role = data.get('account_role')
            if account_role != 'PLATFORM_ROLE_STAR':
                continue
            advertiser_id = data.get('advertiser_id')
            advertiser_id_list.append(advertiser_id)

        return advertiser_id_list

    # 获取星图客户任务列表（获取任务id）
    def get_star_client(self, star_id, access_token):
        result_list = []
        # 请求地址url
        url = 'https://ad.oceanengine.com/open_api/2/star/demand/list/'
        # 请求Header（字典形式储存）
        header = {
            "Access-Token": access_token
        }
        # 请求Body（字典形式储存）
        params = {
            "star_id": star_id,
            "page": 1,
            "page_size": 50
        }

        # 发送GET请求
        data_json = StarRequest().request_get(url, header, params)
        print(data_json)
        data_list = data_json.get('data').get('list')

        result_list.extend(self.star_client_data_process(star_id, data_list))

        page_info = data_json.get('data').get('page_info')
        total_page = page_info.get('total_page')
        if total_page > 1:
            for page_num in range(total_page):
                if page_num < 1:
                    continue
                params['page'] = page_num
                data_json = StarRequest().request_get(url, header, params)
                data_list = data_json.get('data').get('list')
                result_list.extend(self.star_client_data_process(star_id, data_list))

        return Utils().data_remove_duplicates(result_list)

    # 获取任务数据处理
    def star_client_data_process(self, star_id, data_list):
        result_list = []
        for data in data_list:
            demand_name = Utils().Unicode_to_zh(data.get('demand_name'))  # 任务名称
            demand_id = data.get('demand_id')  # 任务id
            create_time = data.get('create_time')  # 任务创建时间，格式：%Y-%m-%d %H:%M:%S
            component_type = data.get('component_type')  # 组件类型
            universal_settlement_type = data.get('universal_settlement_type')  # 结算方式

            result_list.append((star_id, demand_id, demand_name, create_time, component_type, universal_settlement_type))

        return result_list

    # 获取星图客户任务订单列表（订单id）
    def star_task_order(self, star_id, demand_id, Access_Token):
        result_list = []
        # 请求地址url
        url = 'https://api.oceanengine.com/open_api/2/star/demand/order/list/'
        # 请求Header（字典形式储存）
        header = {
            "Access-Token": Access_Token
        }
        # 请求Body（字典形式储存）
        params = {
            "star_id": star_id,
            "demand_id": demand_id,
            "page": 1,
            "page_size": 50
        }

        # 发送GET请求
        data_json = StarRequest().request_get(url, header, params)
        data_list = data_json.get('data').get('list')
        result_list.extend(self.task_order_process(star_id, data_list))

        page_info = data_json.get('data').get('page_info')
        total_page = page_info.get('total_page')
        if total_page > 1:
            for page_num in range(total_page):
                if page_num < 1:
                    continue
                params['page'] = page_num
                data_json = StarRequest().request_get(url, header, params)
                data_list = data_json.get('data').get('list')
                result_list.extend(self.task_order_process(star_id, data_list))

        return Utils().data_remove_duplicates(result_list)

    def task_order_process(self, star_id, data_list):
        result_list = []
        if data_list is None:
            return None

        for data in data_list:
            author_id = data.get('author_id')  # 达人id
            author_name = data.get('author_name')  # 达人名称
            avatar_uri = data.get('avatar_uri')  # 达人URL
            campaign_id = data.get('campaign_id')  # 需求id
            create_time = data.get('create_time')  # 订单创建时间，格式：%Y-%m-%d %H:%M:%S
            demand_id = data.get('demand_id')  # 任务id
            head_image_uri = data.get('head_image_uri')  # 封面图
            item_id = data.get('item_id')  # 视频id，与星图平台前端video_url中展现的视频id一致，每个视频唯一
            order_id = data.get('order_id')  # 订单id
            release_time = data.get('release_time')  # 指派任务产出物发布时间
            title = data.get('title')  # 作品名称
            universal_order_status = data.get('universal_order_status')  # 订单状态
            video_id = data.get('video_id')  # 视频id，每个视频唯一（建议使用item_id）
            video_url = data.get('video_url')  # 视频链接

            result_list.append((star_id, author_id, author_name, avatar_uri, campaign_id, create_time, demand_id, head_image_uri,
                                item_id, order_id, release_time, title, universal_order_status, video_id, video_url))
        return result_list

    # 投后线索（暂未做相关投放）
    def star_after_vote_clue(self, star_id, order_id, Access_Token):

        # 请求地址url
        url = 'https://ad.oceanengine.com/open_api/2/star/clue/get/'
        # 请求Header（字典形式储存）
        header = {
            "Access-Token": Access_Token
        }
        # 请求Body（字典形式储存）
        params = {
            "star_id": star_id,
            "order_id": order_id,
            "page": 1,
            "page_size": 50
        }

        # 发送GET请求
        data_json = StarRequest().request_get(url, header, params)
        # data_json = requests.get(url, headers=header, params=params)
        return data_json

    # 分析报表
    def star_after_vote_analyze(self, star_id, order_id, Access_Token):
        # 请求地址url
        url = 'https://ad.oceanengine.com/open_api/2/star/report/order_overview/get/'
        # 请求Header（字典形式储存）
        header = {
            "Access-Token": Access_Token
        }
        # 请求Body（字典形式储存）
        params = {
            "star_id": star_id,
            "order_id": order_id,
        }

        # 发送GET请求
        data_json = StarRequest().request_get(url, header, params)
        print(data_json)
        data = data_json.get('data')
        return self.analyze_data_process(star_id, order_id, data)

    def analyze_data_process(self, star_id, order_id, data):
        if data is None:
            return None

        comment = data.get("comment")  # 舆情表现
        high_frequency_words = str(comment.get("high_frequency_words"))  # 热词top10
        neg_rate = comment.get("neg_rate")  # 负向评论率（neg_rate/100）%
        neu_rate = comment.get("neu_rate")  # 中立评论率（neu_rate/100）%
        pos_rate = comment.get("pos_rate")  # 正向评论率（pos_rate/100）%

        convert = data.get("convert")  # 转化表现
        click = convert.get("click")  # 组件点击量
        ctr = convert.get("ctr")  # 组件点击率 (ctr/100)%
        show_num =convert.get("show")  # 组件展示量

        cost_effectiveness = data.get("cost_effectiveness")  # 性价比表现
        cpm = cost_effectiveness.get("cpm")  # 千次播放成本(分)
        play = cost_effectiveness.get("play")  # 播放次数
        price = cost_effectiveness.get("price")  # 订单金额(分)

        creative = data.get("creative")  # 创意表现
        finish_rate = creative.get("finish_rate")  # 完播率 (finish_rate/100)%
        five_s_play_rate = creative.get("five_s_play_rate")  # 有效播放率（播放5s以上记为有效播放）(five_s_play_rate/100)%
        play_rate = creative.get("play_rate")  # 平均播放率（=用户观看该任务视频的平均观看时长/视频总时长）(play_rate/100)%

        spread = data.get("spread")  # 传播表现
        comment_num = spread.get("comment")  # 评论量
        like_num = spread.get("like")  # 点赞量
        spread_play = spread.get("play")  # 播放量
        share_num = spread.get("share")  # 分享量

        update_time = data.get("update_time")  # 数据更新时间，格式%Y-%m-%d %H:%M:%S

        return (star_id, order_id, high_frequency_words, neg_rate, neu_rate, pos_rate, click, ctr, show_num, cpm, play,
                price, finish_rate, five_s_play_rate, play_rate, comment_num, like_num, spread_play, share_num, update_time)

    def start(self, app_id, app_secret, access_token):
        # 订单投后分析列表
        star_after_vote_analyze_data_list = []
        star_task_order_list_result = []

        # 帐户id列表
        star_id_list = self.get_advertiser_id(app_id, app_secret, access_token)
        for star_id in star_id_list:
            # 任务信息列表
            demand_info_list = self.get_star_client(star_id, access_token)
            for demand_info in demand_info_list:
                # 任务订单列表
                star_task_order_list = self.star_task_order(demand_info[0], demand_info[1], access_token)
                star_task_order_list_result.extend(star_task_order_list)
                for task_order in star_task_order_list:
                    if task_order[12] != 'FINISHED':
                        continue
                    if Utils().get_date_diff(Utils().get_now_time('%Y-%m-%d %H:%M:%S'), task_order[10]) > 100:
                        continue

                    print(task_order[10])
                    star_after_vote_analyze_data_list.append(self.star_after_vote_analyze(task_order[0], task_order[9], access_token))

        # Utils().data_remove_duplicates(star_after_vote_analyze_data_list)
        # Utils().data_remove_duplicates1(star_task_order_list_result)

        # 数据落库操作
        demand_info_sql = '''
            INSERT INTO jl_star_demand_info (star_id,demand_id,demand_name,create_time,component_type,universal_settlement_type) 
            values %s
            on conflict (star_id, demand_id) do update set 
            demand_name = excluded.demand_name,
            create_time = excluded.create_time,
            component_type = excluded.component_type,
            universal_settlement_type = excluded.universal_settlement_type
                                    
        '''

        star_task_order_sql = '''
            INSERT INTO jl_star_demand_order_info (star_id,author_id,author_name,avatar_uri,campaign_id,create_time, 
            demand_id,head_image_uri,item_id,order_id,release_time,title,universal_order_status,video_id,video_url)
            values %s
            on conflict (star_id, demand_id, order_id, campaign_id, author_id, video_id) do update set 
            author_name = excluded.author_name,
            avatar_uri = excluded.avatar_uri,
            create_time = excluded.create_time,
            head_image_uri = excluded.head_image_uri,
            item_id = excluded.item_id,
            release_time = excluded.release_time,
            title = excluded.title,
            universal_order_status = excluded.universal_order_status,
            video_url = excluded.video_url
        '''

        analyze_data_sql = '''
            INSERT INTO jl_star_analyze_report (star_id,order_id,high_frequency_words,neg_rate,neu_rate,pos_rate,click, 
            ctr,show_num,cpm,play,price,finish_rate,five_s_play_rate,play_rate,comment_num,like_num,spread_play,share_num,update_time)
            values %s
            on conflict (star_id, order_id, update_time) do update set 
            high_frequency_words = excluded.high_frequency_words,
            neg_rate = excluded.neg_rate,
            neu_rate = excluded.neu_rate,
            pos_rate = excluded.pos_rate,
            click = excluded.click,
            ctr = excluded.ctr,
            show_num = excluded.show_num,
            cpm = excluded.cpm,
            play = excluded.play,
            price = excluded.price,
            finish_rate = excluded.finish_rate,
            five_s_play_rate = excluded.five_s_play_rate,
            play_rate = excluded.play_rate,
            comment_num = excluded.comment_num,
            like_num = excluded.like_num,
            spread_play = excluded.spread_play,
            share_num = excluded.share_num,
            update_time = excluded.update_time
        '''

        Utils().conn_pgdb(demand_info_sql, demand_info_list)
        Utils().conn_pgdb(star_task_order_sql, Utils().data_remove_duplicates1(star_task_order_list_result))
        Utils().conn_pgdb(analyze_data_sql, Utils().data_remove_duplicates(star_after_vote_analyze_data_list))

if __name__ == '__main__':
    runner = DataInterface()
    for idx, account in enumerate(runner.credential_accounts):
        runner.start(account["app_id"], account["secret"], runner.access_tokens[idx])

    logging.info("================== 星图程序执行完成 ==================")
