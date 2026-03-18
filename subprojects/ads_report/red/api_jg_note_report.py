import requests
import datetime
import JuGuang


def get_note_report(access_token, advertiser_id, advertiser_name, field_list, start_date, end_date):
    url = "https://adapi.xiaohongshu.com/api/open/jg/data/report/offline/note"
    headers = {
        "Access-Token": access_token
    }
    params = {
        "advertiser_id": advertiser_id,
        "time_unit": "DAY",
        "start_date": start_date,
        "end_date": end_date,
        "page_size": 500,
        "page_num": 1,
    }
    note_data = []
    while True:
        response = requests.post(url, headers=headers, params=params)
        response_json = response.json()
        try:
            data_num = len(response_json["data"]["data_list"])
        except Exception as e:
            print(e)
            print(response_json)
            break

        for item in response_json["data"]["data_list"]:
            merged_data = {
                "advertiser_id": advertiser_id,
                "advertiser_name": advertiser_name,
                **{field: item.get(field) for field in field_list},
            }

            note_data.append(merged_data)

        if data_num < 500:
            break
        else:
            params["page_num"] += 1

    return note_data


query_sql = "SELECT access_token,shop_name,advertiser_id,advertiser_name FROM xhs_token_info where platform = '聚光'"
shop_data = []
for adv_info in JuGuang.gosql_v3.execute_query(query_sql):
    access_token, shop_name, advertiser_id, advertiser_name = adv_info
    end_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    start_time = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    # start_time = '2025-06-01'
    # end_time = '2025-06-30'
    fields_list = ['add_cart_price', 'clk_live_comment', 'invoke_app_engagement_cost', 'seller_visit_price',
                   'clk_live_5s_entry_pv', 'purchase_order_price_7d', 'external_goods_visit_7', 'fee', 'msg_leads_cost',
                   'external_goods_order_rate_15', 'external_goods_visit_rate_7', 'follow', 'clk_live_entry_pv',
                   'invoke_app_enter_store_cost', 'search_cmt_after_read_avg', 'clk_live_entry_pv_cost',
                   'external_roi_7',
                   'rgmv', 'initiative_message_cpl', 'clk_live_all_follow', 'invoke_app_open_cost', 'impression',
                   'click_order_cvr', 'valid_leads_cpl', 'initiative_message', 'click', 'leads_cpl',
                   'message_consult_cpl',
                   'live_average_order_cost', 'add_wechat_suc_count', 'like', 'clk_live_room_roi',
                   'purchase_order_gmv_7d',
                   'external_rgmv_30', 'time', 'interaction', 'goods_visit_price', 'wechat_talk_count', 'i_user_price',
                   'pic_save', 'goods_visit', 'message_consult', 'external_goods_order_price_15',
                   'purchase_order_roi_7d',
                   'add_wechat_cost', 'external_goods_order_rate_7', 'shop_poi_click_num', 'note_image', 'leads_cvr',
                   'note_jump_url', 'landing_page_visit', 'external_roi_15', 'invoke_app_engagement_cnt',
                   'invoke_app_payment_roi', 'collect', 'share', 'ti_user_num', 'leads', 'search_cmt_click',
                   'presale_order_num_7d', 'seller_visit', 'clk_live_room_rgmv', 'search_cmt_after_read',
                   'external_goods_order_7', 'external_goods_order_price_7', 'external_roi_30', 'ti_user_price',
                   'valid_leads', 'clk_live_avg_view_time', 'presale_order_gmv_7d', 'invoke_app_payment_unit_price',
                   'wechat_talk_cost', 'ctr', 'cpm', 'leads_button_impression', 'message', 'external_rgmv_7',
                   'invoke_app_open_cnt', 'external_goods_visit_price_7', 'cpi', 'shopping_cart_add', 'msg_leads_num',
                   'invoke_app_payment_cnt', 'shop_poi_page_pv', 'note_title', 'comment', 'message_user',
                   'clk_live_room_order_num', 'external_goods_order_15', 'external_goods_order_price_30',
                   'invoke_app_payment_amount', 'external_rgmv_15', 'clk_live_5s_entry_uv_cost', 'add_wechat_count',
                   'action_button_ctr', 'note_id', 'search_invoke_button_click_cost', 'shop_poi_page_visit_price',
                   'roi',
                   'external_goods_order_30', 'invoke_app_enter_store_cnt', 'shop_poi_page_navigate_click',
                   'i_user_num',
                   'goods_order', 'search_cmt_click_cvr', 'reserve_pv', 'add_wechat_suc_cost', 'screenshot',
                   'goods_order_price', 'external_goods_order_rate_30', 'acp', 'action_button_click',
                   'success_goods_order',
                   'invoke_app_payment_cost', 'search_invoke_button_click_cnt']
    report_data = get_note_report(access_token, advertiser_id, advertiser_name, fields_list,
                                  start_time, end_time)
    JuGuang.gosql_v3.api_to_sql(json_data=report_data, sql_name="api_xhs_jg_note_report")
