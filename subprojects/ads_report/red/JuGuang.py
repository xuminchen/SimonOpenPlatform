import datetime
import json
import getToken
from subprojects._shared.core.http_client import HttpClient, HttpRequestConfig
from subprojects._shared.core import db_client as gosql_v2
from subprojects._shared.core import db_client as gosql_v3
from subprojects._shared.core.api_credentials import get_credentials

HTTP_CLIENT = HttpClient(
    HttpRequestConfig(
        timeout_seconds=45,
        max_retries=4,
        retry_interval_seconds=1.5,
    )
)


def post_json(url, headers, payload, event_name):
    result = HTTP_CLIENT.request_json(
        method="post",
        url=url,
        headers=headers,
        data=json.dumps(payload),
        success_checker=lambda body: isinstance(body, dict) and ("data" in body or body.get("success") in (True, "true", "True")),
        event_name=event_name,
    )
    if not result.ok:
        raise RuntimeError("POST failed: {0}, {1}".format(url, result.error or result.message))
    return result.data


def get_json(url, event_name):
    result = HTTP_CLIENT.request_json(method="get", url=url, event_name=event_name)
    if not result.ok:
        raise RuntimeError("GET failed: {0}, {1}".format(url, result.error or result.message))
    return result.data

app_info = get_credentials("xhs_juguang", "apps", default={})


def get_plan_data(access_token, shop_name, advertiser_name, advertiser_id, start_date, end_date):
    url = "https://adapi.xiaohongshu.com/api/open/jg/data/report/offline/campaign"
    header = {
        "content-type": "application/json",
        "Access-Token": access_token
    }
    data = {
        "advertiser_id": advertiser_id,
        "start_date": start_date,
        "end_date": end_date,
        "page_size": 500,
        "page_num": 1,
    }
    more_data = []
    while True:
        resp = post_json(url=url, headers=header, payload=data, event_name="xhs_campaign_report")
        if not resp['data']['data_list']:
            break
        for i in resp['data']['data_list']:
            i = dict(i)
            one_data = {
                'time': i['time'],
                'placement': i.get('placement', '-'),
                'optimize_target': i.get('optimize_target', '-'),
                'promotion_target': i.get('promotion_target', '-'),
                'bidding_strategy': i.get('bidding_strategy', '-'),
                'build_type': i.get('build_type', '-'),
                'marketing_target': i.get('marketing_target', '-'),
                'campaign_id': i.get('campaign_id', '-'),
                'campaign_name': i.get('campaign_name', '-'),
                'page_id': i.get('page_id', '-'),
                'item_id': i.get('item_id', '-'),
                'live_red_id': i.get('live_red_id', '-'),
                'fee': i.get('fee', '-'),
                'impression': i.get('impression', '-'),
                'click': i.get('click', '-'),
                'ctr': i.get('ctr', '-'),
                'acp': i.get('acp', '-'),
                'cpm': i.get('cpm', '-'),
                'like': i.get('like', '-'),
                'comment': i.get('comment', '-'),
                'collect': i.get('collect', '-'),
                'follow': i.get('follow', '-'),
                'share': i.get('share', '-'),
                'interaction': i.get('interaction', '-'),
                'cpi': i.get('cpi', '-'),
                'action_button_click': i.get('action_button_click', '-'),
                'action_button_ctr': i.get('action_button_ctr', 0),
                'screenshot': i.get('screenshot', 0),
                'pic_save': i.get('pic_save', 0),
                'clk_live_entry_pv': i.get('clk_live_entry_pv', 0),
                'clk_live_entry_pv_cost': i.get('clk_live_entry_pv_cost', 0),
                'clk_live_avg_view_time': i.get('clk_live_avg_view_time', 0),
                'clk_live_all_follow': i.get('clk_live_all_follow', 0),
                'clk_live_5s_entry_pv': i.get('clk_live_5s_entry_pv', 0),
                'clk_live_5s_entry_uv_cost': i.get('clk_live_5s_entry_uv_cost', 0),
                'clk_live_comment': i.get('clk_live_comment', 0),
                'search_cmt_click': i.get('search_cmt_click', 0),
                'search_cmt_click_cvr': i.get('search_cmt_click_cvr', 0),
                'search_cmt_after_read': i.get('search_cmt_after_read', 0),
                'search_cmt_after_read_avg': i.get('search_cmt_after_read_avg', 0),
                'goods_visit': i.get('goods_visit', 0),
                'goods_visit_price': i.get('goods_visit_price', 0),
                'seller_visit': i.get('seller_visit', 0),
                'seller_visit_price': i.get('seller_visit_price', 0),
                'shopping_cart_add': i.get('shopping_cart_add', 0),
                'success_goods_order': i.get('success_goods_order', 0),
                'click_order_cvr': i.get('click_order_cvr', 0),
                'purchase_order_price_7d': i.get('purchase_order_price_7d', 0),
                'purchase_order_gmv_7d': i.get('purchase_order_gmv_7d', 0),
                'purchase_order_roi_7d': i.get('purchase_order_roi_7d', 0),
                'clk_live_room_order_num': i.get('clk_live_room_order_num', 0),
                'live_average_order_cost': i.get('live_average_order_cost', 0),
                'clk_live_room_rgmv': i.get('clk_live_room_rgmv', 0),
                'clk_live_room_roi': i.get('clk_live_room_roi', 0),
                'shop_name': shop_name,
                'advertiser_id': advertiser_id,
                'advertiser_name': advertiser_name
            }
            more_data.append(one_data)

        if len(resp['data']['data_list']) < 500:
            break
        else:
            data['page_num'] += 1

    return more_data


def get_all_advertiser_plan_data(start_date=None, end_date=None):
    if not start_date:
        end_date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.datetime.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')

    query_sql = "SELECT access_token,shop_name,advertiser_id,advertiser_name FROM xhs_token_info where platform = '聚光'"
    shop_data = []
    for adv_info in gosql_v3.execute_query(query_sql):
        print(adv_info)
        access_token, shop_name, advertiser_id, advertiser_name = adv_info
        shop_data.extend(get_plan_data(access_token=access_token, shop_name=shop_name, advertiser_name=advertiser_name,
                                       advertiser_id=advertiser_id, start_date=start_date, end_date=end_date))

    first_sql = f"DELETE FROM `api_xhs_jg_plan_data` WHERE `time` BETWEEN '{start_date}' AND '{end_date}'"
    print(first_sql)
    gosql_v2.api_to_sql(shop_data, 'api_xhs_jg_plan_data', need_clean='yes', first_execute_sql=first_sql)
    get_json(
        get_credentials("bi_refresh_urls", "ds_c10b4b761b66d475e9679cd5", required=True),
        event_name="xhs_bi_refresh_campaign",
    )


def get_idea_data(access_token, shop_name, advertiser_name, advertiser_id, start_date, end_date):
    url = "https://adapi.xiaohongshu.com/api/open/jg/data/report/offline/creative"
    header = {
        "content-type": "application/json",
        "Access-Token": access_token
    }
    data = {
        "advertiser_id": advertiser_id,
        "start_date": start_date,
        "end_date": end_date,
        "page_size": 500,
        "page_num": 1,
    }

    more_data = []
    while True:
        resp = post_json(url=url, headers=header, payload=data, event_name="xhs_creative_report")
        if not resp['data']['data_list']:
            break

        for i in resp['data']['data_list']:
            i = dict(i)
            one_data = {
                'campaign_id': i.get('campaign_id', '-'),
                'campaign_name': i.get('campaign_name', '-'),
                'unit_id': i.get('unit_id', '-'),
                'unit_name': i.get('unit_name', '-'),
                'creativity_id': i.get('creativity_id', '-'),
                'creativity_name': i.get('creativity_name', '-'),
                'creativity_image': i.get('creativity_image', '-'),
                'note_id': i.get('note_id', '-'),
                'time': i['time'],
                'placement': i.get('placement', '-'),
                'optimize_target': i.get('optimize_target', '-'),
                'promotion_target': i.get('promotion_target', '-'),
                'bidding_strategy': i.get('bidding_strategy', '-'),
                'build_type': i.get('build_type', '-'),
                'marketing_target': i.get('marketing_target', '-'),
                'page_id': i.get('page_id', '-'),
                'item_id': i.get('item_id', '-'),
                'live_red_id': i.get('live_red_id', '-'),
                'fee': i.get('fee', 0),
                'impression': i.get('impression', 0),
                'click': i.get('click', 0),
                'ctr': i.get('ctr', 0),
                'acp': i.get('acp', 0),
                'cpm': i.get('cpm', 0),
                'like': i.get('like', 0),
                'comment': i.get('comment', 0),
                'collect': i.get('collect', 0),
                'follow': i.get('follow', 0),
                'share': i.get('share', 0),
                'interaction': i.get('interaction', 0),
                'cpi': i.get('cpi', 0),
                'action_button_click': i.get('action_button_click', 0),
                'action_button_ctr': i.get('action_button_ctr', 0),
                'screenshot': i.get('screenshot', 0),
                'pic_save': i.get('pic_save', 0),
                'reserve_pv': i.get('reserve_pv', 0),
                'clk_live_entry_pv': i.get('clk_live_entry_pv', 0),
                'clk_live_entry_pv_cost': i.get('clk_live_entry_pv_cost', 0),
                'clk_live_avg_view_time': i.get('clk_live_avg_view_time', 0),
                'clk_live_all_follow': i.get('clk_live_all_follow', 0),
                'clk_live_5s_entry_pv': i.get('clk_live_5s_entry_pv', 0),
                'clk_live_5s_entry_uv_cost': i.get('clk_live_5s_entry_uv_cost', 0),
                'clk_live_comment': i.get('clk_live_comment', 0),
                'search_cmt_click': i.get('search_cmt_click', 0),
                'search_cmt_click_cvr': i.get('search_cmt_click_cvr', 0),
                'search_cmt_after_read': i.get('search_cmt_after_read', 0),
                'search_cmt_after_read_avg': i.get('search_cmt_after_read_avg', 0),
                'goods_visit': i.get('goods_visit', 0),
                'goods_visit_price': i.get('goods_visit_price', 0),
                'seller_visit': i.get('seller_visit', 0),
                'seller_visit_price': i.get('seller_visit_price', 0),
                'shopping_cart_add': i.get('shopping_cart_add', 0),
                'add_cart_price': i.get('add_cart_price', 0),
                'presale_order_num_7d': i.get('presale_order_num_7d', 0),
                'presale_order_gmv_7d': i.get('presale_order_gmv_7d', 0),
                'goods_order': i.get('goods_order', 0),
                'goods_order_price': i.get('goods_order_price', 0),
                'rgmv': i.get('rgmv', 0),
                'roi': i.get('roi', 0),
                'success_goods_order': i.get('success_goods_order', 0),
                'click_order_cvr': i.get('click_order_cvr', 0),
                'purchase_order_price_7d': i.get('purchase_order_price_7d', 0),
                'purchase_order_gmv_7d': i.get('purchase_order_gmv_7d', 0),
                'purchase_order_roi_7d': i.get('purchase_order_roi_7d', 0),
                'clk_live_room_order_num': i.get('clk_live_room_order_num', 0),
                'live_average_order_cost': i.get('live_average_order_cost', 0),
                'clk_live_room_rgmv': i.get('clk_live_room_rgmv', 0),
                'clk_live_room_roi': i.get('clk_live_room_roi', 0),
                'shop_name': shop_name,
                'advertiser_id': advertiser_id,
                'advertiser_name': advertiser_name
            }

            more_data.append(one_data)

        if len(resp['data']['data_list']) < 500:
            break
        else:
            data['page_num'] += 1

    return more_data


def get_all_advertiser_idea_data(start_date=None, end_date=None):
    if not start_date:
        end_date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.datetime.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')

    query_sql = "SELECT access_token,shop_name,advertiser_id,advertiser_name FROM xhs_token_info where platform = '聚光'"
    shop_data = []
    for adv_info in gosql_v3.execute_query(query_sql):
        print(adv_info)
        access_token, shop_name, advertiser_id, advertiser_name = adv_info
        shop_data.extend(get_idea_data(access_token=access_token, shop_name=shop_name, advertiser_name=advertiser_name,
                                       advertiser_id=advertiser_id, start_date=start_date, end_date=end_date))

    first_sql = f"DELETE FROM `api_xhs_jg_idea_data` WHERE `time` BETWEEN '{start_date}' AND '{end_date}'"
    print(first_sql)
    gosql_v2.api_to_sql(shop_data, 'api_xhs_jg_idea_data', need_clean='yes', first_execute_sql=first_sql)
    get_json(
        get_credentials("bi_refresh_urls", "ds_b065414b127004abe9663f28", required=True),
        event_name="xhs_bi_refresh_creative",
    )

    # for k, v in app_info.items():
    #     app = getToken.Xhs(app_id=v["app_id"], secret=v["secret"], auth_code=v.get("auth_code", ""), )
    #     token, advertiser_list = app.get_token()
    #     shop_data = []
    #     for j in advertiser_list:
    #         advertiser_id = j['advertiser_id']
    #         advertiser_name = j['advertiser_name']
    #
    #         shop_data.extend(get_idea_data(access_token=token, shop_name=k, advertiser_name=advertiser_name,
    #                                        advertiser_id=advertiser_id, start_date=start_date, end_date=end_date))
    #
    #     first_sql = f"DELETE FROM `api_xhs_jg_idea_data` WHERE `time` BETWEEN '{start_date}' AND '{end_date}' AND `shop_name` = '{k}'"
    #     print(first_sql)
    #     gosql_v2.api_to_sql(shop_data, 'api_xhs_jg_idea_data', need_clean='yes', first_execute_sql=first_sql)
    #     requests.get(
    #         get_credentials("bi_refresh_urls", "ds_b065414b127004abe9663f28", required=True))
    #


def get_note_list(access_token=None, shop_name='', advertiser_name='',
                  advertiser_id='150177'):
    if not access_token:
        raise ValueError("access_token is required")
    url = "https://adapi.xiaohongshu.com/api/open/jg/note/list"
    header = {
        "content-type": "application/json",
        "Access-Token": access_token
    }
    note_type_list = [1, 2]
    placement_type_list = [1, 2, 3, 4]
    market_target_type = [4, 9, 16]
    for a in note_type_list:
        note_type = a
        for b in placement_type_list:
            placement_type = b
            for c in market_target_type:
                market_target = c
                print(note_type, placement_type, market_target)
                data = {
                    "advertiser_id": advertiser_id,
                    "note_type": note_type,
                    "placement_type": placement_type,
                    "market_target": market_target,
                    "page": 1,
                    "page_size": 100
                }
                while True:

                    resp = post_json(url=url, headers=header, payload=data, event_name="xhs_note_list")

                    print(len(resp['data']['notes']), data['page'])
                    if resp['data'] and len(resp['data']['notes']) == 100:
                        data['page'] += 1
                    else:
                        print('最后一次跑完了，我退！')
                        break


def get_account_info(access_token, shop_name, advertiser_name, advertiser_id, ):
    url = "https://adapi.xiaohongshu.com/api/open/jg/account/balance/info"
    header = {
        "content-type": "application/json",
        "Access-Token": access_token
    }
    data = {
        "advertiser_id": advertiser_id,
    }

    resp = post_json(url=url, headers=header, payload=data, event_name="xhs_account_balance")
    d = dict(resp['data'])
    data = {
        'total_balance': d.get('total_balance', 0) / 100,
        'cash_balance': d.get('cash_balance', 0) / 100,
        'return_balance': d.get('return_balance', 0) / 100,
        'freeze_balance': d.get('freeze_balance', 0) / 100,
        'compensate_return_balance': d.get('compensate_return_balance', 0) / 100,
        'available_balance': d.get('available_balance', 0) / 100,
        'today_spend': d.get('today_spend', 0) / 100,
        'static_time': datetime.datetime.today().strftime('%Y-%m-%d'),
        'shop_name': shop_name,
        'advertiser_id': advertiser_id,
        'advertiser_name': advertiser_name
    }
    return data


def get_all_account_info():
    query_sql = "SELECT access_token,shop_name,advertiser_id,advertiser_name FROM xhs_token_info where platform = '聚光'"
    shop_data = []
    for adv_info in gosql_v3.execute_query(query_sql):
        print(adv_info)
        access_token, shop_name, advertiser_id, advertiser_name = adv_info
        shop_data.append(
            get_account_info(access_token=access_token, shop_name=shop_name, advertiser_name=advertiser_name,
                             advertiser_id=advertiser_id))

    gosql_v2.api_to_sql(shop_data, 'api_xhs_jg_account_info')

    get_json(
        get_credentials("bi_refresh_urls", "ds_tb0ab3663d2144c0e8693f7d", required=True),
        event_name="xhs_bi_refresh_account",
    )
    # print(shop_data)


