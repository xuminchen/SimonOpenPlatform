import datetime

import requests
import json
import JuGuang


def get_plan_data(access_token, shop_name, advertiser_name, advertiser_id, start_date, end_date):
    url = "https://adapi.xiaohongshu.com/api/open/wind/data/report/offline/campaign"
    header = {
        "content-type": "application/json",
        "Access-Token": access_token
    }
    data = {
        "advertiser_id": advertiser_id,
        "start_date": start_date,
        "end_date": end_date,
        "columns": ['fee', 'impression', 'click', 'ctr', 'acp', 'cpm', 'like', 'comment', 'collect', 'follow', 'share',
                    'interaction',
                    'cpi', 'action_button_click', 'action_button_ctr', 'screenshot', 'pic_save', 'search_cmt_click',
                    'search_cmt_click_cvr',
                    'search_cmt_after_read_avg', 'search_cmt_after_read', 'reserve_pv', 'live_subscribe_cnt',
                    'live_subscribe_cnt_cost',
                    'live_watch_cnt', 'live_watch_cnt_cost', 'live_watch_duration_avg', 'live_follow_cnt',
                    'live_5s_watch_cnt',
                    'live_5s_watch_cnt_cost', 'live_cmt_cnt', 'live_30s_watch_cnt', 'live_30s_watch_cnt_cost',
                    'goods_view_num',
                    'goods_view_num_cost', 'goods_add_cart_num', 'goods_add_cart_num_cost', 'total_order_num_7d',
                    'total_order_num_7d_cost',
                    'total_order_gmv_7d', 'total_order_roi_7d', 'deal_order_num_7d', 'deal_order_num_7d_cost',
                    'deal_order_gmv_7d',
                    'deal_order_roi_7d', 'live_direct_purchase_order_num_24h',
                    'live_direct_purchase_order_num_24h_cost',
                    'live_direct_purchase_order_gmv_24h', 'live_direct_purchase_order_roi_24h',
                    'live_direct_deal_order_num_24h', 'live_direct_deal_order_num_24h_cost',
                    'live_direct_deal_order_gmv_24h', 'live_direct_deal_order_roi_24h',
                    'new_seller_goods_view_num', 'new_seller_deal_order_num_7d', 'new_seller_deal_order_gmv_7d'],
        "page_size": 500,
        "page_num": 1,
    }
    more_data = []
    while True:
        resp = requests.post(url=url, data=json.dumps(data), headers=header).json()
        # print(resp)
        data_list = resp['data'].get('data_list', [])

        if not data_list:
            break

        for i in resp['data']['data_list']:
            i = dict(i)
            one_data = {
                'screenshot': i.get('screenshot', 0),
                'live_watch_cnt': i.get('live_watch_cnt', 0),
                'search_cmt_after_read_avg': i.get('search_cmt_after_read_avg', 0),
                'live_subscribe_cnt': i.get('live_subscribe_cnt', 0),
                'deal_order_num_7d': i.get('deal_order_num_7d', 0),
                'deal_order_gmv_7d': i.get('deal_order_gmv_7d', 0),
                'live_direct_purchase_order_num_24h': i.get('live_direct_purchase_order_num_24h', 0),
                'live_direct_deal_order_gmv_24h': i.get('live_direct_deal_order_gmv_24h', 0),
                'click': i.get('click', 0),
                'search_cmt_click_cvr': i.get('search_cmt_click_cvr', 0),
                'live_cmt_cnt': i.get('live_cmt_cnt', 0),
                'goods_add_cart_num': i.get('goods_add_cart_num', 0),
                'total_order_num_7d': i.get('total_order_num_7d', 0),
                'total_order_num_7d_cost': i.get('total_order_num_7d_cost', 0),
                'fee': i.get('fee', 0),
                'like': i.get('like', 0),
                'live_5s_watch_cnt_cost': i.get('live_5s_watch_cnt_cost', 0),
                'deal_order_num_7d_cost': i.get('deal_order_num_7d_cost', 0),
                'total_order_gmv_7d': i.get('total_order_gmv_7d', 0),
                'search_cmt_after_read': i.get('search_cmt_after_read', 0),
                'live_watch_cnt_cost': i.get('live_watch_cnt_cost', 0),
                'live_watch_duration_avg': i.get('live_watch_duration_avg', 0),
                'optimize_target': i.get('optimize_target', 0),
                'cpi': i.get('cpi', 0),
                'search_cmt_click': i.get('search_cmt_click', 0),
                'live_direct_deal_order_roi_24h': i.get('live_direct_deal_order_roi_24h', 0),
                'cpm': i.get('cpm', 0),
                'action_button_click': i.get('action_button_click', 0),
                'live_direct_deal_order_num_24h_cost': i.get('live_direct_deal_order_num_24h_cost', 0),
                'campaign_name': i.get('campaign_name', "-"),
                'interaction': i.get('interaction', 0),
                'goods_add_cart_num_cost': i.get('goods_add_cart_num_cost', 0),
                'new_seller_goods_view_num': i.get('new_seller_goods_view_num', 0),
                'pic_save': i.get('pic_save', 0),
                'live_subscribe_cnt_cost': i.get('live_subscribe_cnt_cost', 0),
                'ctr': i.get('ctr', 0),
                'share': i.get('share', 0),
                'live_30s_watch_cnt': i.get('live_30s_watch_cnt', 0),
                'live_30s_watch_cnt_cost': i.get('live_30s_watch_cnt_cost', 0),
                'action_button_ctr': i.get('action_button_ctr', 0),
                'live_5s_watch_cnt': i.get('live_5s_watch_cnt', 0),
                'acp': i.get('acp', 0),
                'comment': i.get('comment', 0),
                'collect': i.get('collect', 0),
                'live_direct_purchase_order_roi_24h': i.get('live_direct_purchase_order_roi_24h', 0),
                'live_direct_deal_order_num_24h': i.get('live_direct_deal_order_num_24h', 0),
                'new_seller_deal_order_num_7d': i.get('new_seller_deal_order_num_7d', 0),
                'campaign_id': i.get('campaign_id', "-"),
                'impression': i.get('impression', 0),
                'new_seller_deal_order_gmv_7d': i.get('new_seller_deal_order_gmv_7d', 0),
                'live_follow_cnt': i.get('live_follow_cnt', 0),
                'total_order_roi_7d': i.get('total_order_roi_7d', 0),
                'deal_order_roi_7d': i.get('deal_order_roi_7d', 0),
                'live_direct_purchase_order_gmv_24h': i.get('live_direct_purchase_order_gmv_24h', 0),
                'follow': i.get('follow', 0),
                'goods_view_num_cost': i.get('goods_view_num_cost', 0),
                'goods_view_num': i.get('goods_view_num', 0),
                'live_direct_purchase_order_num_24h_cost': i.get('live_direct_purchase_order_num_24h_cost', 0),
                'time': i['time'],
                'marketing_target': i.get('marketing_target', 0),
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


query_sql = "SELECT access_token,shop_name,advertiser_id,advertiser_name FROM xhs_token_info where platform = '乘风'"
shop_data = []
for adv_info in JuGuang.gosql_v3.execute_query(query_sql):
    print(adv_info)
    access_token, shop_name, advertiser_id, advertiser_name = adv_info
    start_time = (datetime.datetime.now() - datetime.timedelta(days=40)).strftime("%Y-%m-%d")
    end_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    shop_data.extend(get_plan_data(access_token=access_token, shop_name=shop_name, advertiser_name=advertiser_name,
                                   advertiser_id=advertiser_id, start_date=start_time, end_date=end_time))

JuGuang.gosql_v3.api_to_sql(json_data=shop_data, sql_name="api_xhs_cf_plan_data")