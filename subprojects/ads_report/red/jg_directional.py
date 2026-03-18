import datetime

import JuGuang
import requests
import json
import getToken

campaign_enable_map = {
    0: "暂停", 1: "开启"
}
marketing_target_map = {
    3: "商品销量_日常推广", 4: "产品种草", 8: "直播推广_日常推广", 9: "客资收集", 10: '抢占赛道',
    14: '直播推广_直播预热', 15: "商品销量_店铺拉新"
}
placement_map = {
    1: "信息流", 2: "搜索", 4: "全站智投", 7: "视频内流"
}
optimize_target_map = {
    0: "点击量",
    1: "互动量",
    3: "表单提交量",
    4: "商品成单量",
    5: "私信咨询量",
    6: "直播间观看量",
    11: "商品访客量",
    12: "落地页访问量",
    13: "私信开口量",
    14: "有效观看量",
    18: "站外转化量",
    20: "TI人群规模",
    21: "行业商品成单",
    23: "直播预热量",
    24: "直播间成交",
    25: "直播间支付ROI",
}
constraint_type_map = {
    -1: "无", 101: "自动控制", 0: "点击成本控制", 1: "互动成本控制", 3: "表单提交成本控制", 5: "私信咨询成本控制",
    11: "访客成本控制", 13: "私信开口成本控制", 14: "有效观播成本控制", 17: "ROI控制", 23: "预热成本控制",
    50: "私信留资成本控制"
}
limit_day_budget_map = {
    0: "不限预算", 1: "指定预算",
}
budget_state_map = {
    0: "计划预算不足", 1: "计划预算充足",
}
smart_switch_map = {
    0: "关闭", 1: "开启"
}
pacing_mode_map = {
    1: "匀速投放", 2: "加速投放"
}
build_type_map = {
    0: "标准搭建", 1: "省心智投"
}
feed_flag_map = {
    0: "否", 1: "是"
}
search_flag_map = {
    0: "否", 1: "是"
}
time_period_type_map = {
    0: "全时段", 1: "自定义时间段"
}
unit_filter_state_map = {
    10: "有效", 4: "暂停", 2: "未开始", 3: "已结束", 5: "处于暂停时段", 6: "已被计划暂停", 8: "计划预算不足",
    11: "账户日预算不足", 7: "现金余额不足", 1: "已删除"
}
unit_enable_map = {
    0: "暂停", 1: "开启"
}


def get_realtime_target_lst(access_token, shop_name, advertiser_name, advertiser_id, start_date, end_date):
    # 定向层级实时数据
    url = "https://adapi.xiaohongshu.com/api/open/jg/data/report/realtime/target"
    header = {
        "content-type": "application/json",
        "Access-Token": access_token
    }
    data = {
        "advertiser_id": advertiser_id,
        "start_date": start_date,
        "end_date": end_date,
        "page_size": 100,
        "page_num": 1,
    }
    base_target_dto_more_data = []
    base_campaign_dto_more_data = []
    base_unit_dto_more_data = []
    more_data = []
    while True:
        resp = requests.post(url=url, data=json.dumps(data), headers=header).json()
        if resp["target_dtos"]:
            for j in resp["target_dtos"]:
                base_target_dto_one_data = {
                    'unit_id': j["base_target_dto"].get("unit_id"),
                    'target_id': j["base_target_dto"].get("target_id"),
                    'target_name': j["base_target_dto"].get("target_name"),
                    'target_status': j["base_target_dto"].get("target_status"),
                    'campaign_id': j["base_target_dto"].get("campaign_id"),
                    'pacing_mode': j["base_target_dto"].get("campaign_id"),
                    'shop_name': shop_name,
                    'advertiser_id': advertiser_id,
                    'advertiser_name': advertiser_name
                }
                base_campaign_dto_one_data = {
                    'pacing_mode': pacing_mode_map.get(j["base_campaign_dto"].get("pacing_mode", "-")),
                    'feed_flag': feed_flag_map.get(j["base_campaign_dto"].get("feed_flag", "-")),
                    'build_type': build_type_map.get(j["base_campaign_dto"].get("build_type", "-")),
                    'campaign_id': j["base_campaign_dto"].get("campaign_id"),
                    'campaign_enable': campaign_enable_map.get(j["base_campaign_dto"].get("campaign_enable", "-")),
                    'marketing_target': marketing_target_map.get(j["base_campaign_dto"].get("marketing_target", "-")),
                    'origin_campaign_day_budget': j["base_campaign_dto"].get("origin_campaign_day_budget"),
                    'campaign_create_time': j["base_campaign_dto"].get("campaign_create_time"),
                    'smart_switch': smart_switch_map.get(j["base_campaign_dto"].get("smart_switch", "-")),
                    'expire_time': j["base_campaign_dto"].get("expire_time"),
                    'promotion_target': j["base_campaign_dto"].get("promotion_target"),
                    'constraint_value': j["base_campaign_dto"].get("constraint_value"),
                    'limit_day_budget': limit_day_budget_map.get(j["base_campaign_dto"].get("limit_day_budget", "-")),
                    'campaign_day_budget': j["base_campaign_dto"].get("campaign_day_budget"),
                    'campaign_name': j["base_campaign_dto"].get("campaign_name"),
                    'campaign_filter_state': j["base_campaign_dto"].get("campaign_filter_state"),
                    'placement': placement_map.get(j["base_campaign_dto"].get("placement", "-")),
                    'optimize_target': optimize_target_map.get(j["base_campaign_dto"].get("optimize_target", "-")),
                    'start_time': j["base_campaign_dto"].get("start_time"),
                    'search_flag': search_flag_map.get(j["base_campaign_dto"].get("search_flag", "-")),
                    'bidding_strategy': j["base_campaign_dto"].get("bidding_strategy"),
                    'constraint_type': constraint_type_map.get(j["base_campaign_dto"].get("constraint_type", "-")),
                    'budget_state': budget_state_map.get(j["base_campaign_dto"].get("budget_state", "-")),
                    'time_period_type': time_period_type_map.get(j["base_campaign_dto"].get("time_period_type", "-")),
                    'shop_name': shop_name,
                    'advertiser_id': advertiser_id,
                    'advertiser_name': advertiser_name
                }
                base_unit_dto_one_data = {
                    'unit_enable': unit_enable_map.get(j["base_unit_dto"].get("unit_enable", "-")),
                    'campaign_id': j["base_unit_dto"].get("campaign_id"),
                    'event_bid': j["base_unit_dto"].get("event_bid"),
                    'unit_id': j["base_unit_dto"].get("unit_id"),
                    'unit_name': j["base_unit_dto"].get("unit_name"),
                    'unit_create_time': j["base_unit_dto"].get("unit_create_time"),
                    'unit_filter_state': unit_filter_state_map.get(j["base_unit_dto"].get("unit_filter_state", "-")),
                    'shop_name': shop_name,
                    'advertiser_id': advertiser_id,
                    'advertiser_name': advertiser_name
                }
                one_data = {
                    'page_cmt_click': j["data"].get("page_cmt_click"),
                    'invoke_app_payment_cnt': j["data"].get("invoke_app_payment_cnt"),
                    'app_activate_ctr': j["data"].get("app_activate_ctr"),
                    'like': j["data"].get("like"),
                    'word_click_rank_first': j["data"].get("word_click_rank_first"),
                    'word_click_rate_third': j["data"].get("word_click_rate_third"),
                    'clk_live_entry_pv_cost': j["data"].get("clk_live_entry_pv_cost"),
                    'app_download_button_click_ctr': j["data"].get("app_download_button_click_ctr"),
                    'leads_out_in_3seconds': j["data"].get("leads_out_in_3seconds"),
                    'clk_live_entry_uv': j["data"].get("clk_live_entry_uv"),
                    'message_driving_open_ratio': j["data"].get("message_driving_open_ratio"),
                    'leads_cpl': j["data"].get("leads_cpl"),
                    'message_consult': j["data"].get("message_consult"),
                    'app_download_button_click_cost': j["data"].get("app_download_button_click_cost"),
                    'message_cmt_ctr': j["data"].get("message_cmt_ctr"),
                    'click_order_cvr': j["data"].get("click_order_cvr"),
                    'pis_times': j["data"].get("pis_times"),
                    'shop_poi_page_visit_price': j["data"].get("shop_poi_page_visit_price"),
                    'app_key_action_cost': j["data"].get("app_key_action_cost"),
                    'purchase_order_price_7d': j["data"].get("purchase_order_price_7d"),
                    'external_leads': j["data"].get("external_leads"),
                    'shop_poi_page_navigate_click': j["data"].get("shop_poi_page_navigate_click"),
                    'external_goods_visit_7': j["data"].get("external_goods_visit_7"),
                    'external_goods_order_7': j["data"].get("external_goods_order_7"),
                    'conversion_cost': j["data"].get("conversion_cost"),
                    'buy_now': j["data"].get("buy_now"),
                    'live_average_order_cost': j["data"].get("live_average_order_cost"),
                    'external_rgmv_15': j["data"].get("external_rgmv_15"),
                    'external_goods_order_30': j["data"].get("external_goods_order_30"),
                    'word_avg_location': j["data"].get("word_avg_location"),
                    'shop_poi_page_pv': j["data"].get("shop_poi_page_pv"),
                    'external_rgmv_24h': j["data"].get("external_rgmv_24h"),
                    'first_app_pay_cnt': j["data"].get("first_app_pay_cnt"),
                    'app_activate_amount_3d': j["data"].get("app_activate_amount_3d"),
                    'action_button_ctr': j["data"].get("action_button_ctr"),
                    'word_price_third': j["data"].get("word_price_third"),
                    'retention_1d_cnt': j["data"].get("retention_1d_cnt"),
                    'page_cmt_imp': j["data"].get("page_cmt_imp"),
                    'app_activate_cnt': j["data"].get("app_activate_cnt"),
                    'app_register_cnt': j["data"].get("app_register_cnt"),
                    'shopping_cart_add': j["data"].get("shopping_cart_add"),
                    'goods_visit': j["data"].get("goods_visit"),
                    'initiative_message_cpl': j["data"].get("initiative_message_cpl"),
                    'word_click_rate_first': j["data"].get("word_click_rate_first"),
                    'wechat_copy_succ_cnt': j["data"].get("wechat_copy_succ_cnt"),
                    'invoke_app_payment_amount': j["data"].get("invoke_app_payment_amount"),
                    'shop_poi_click_num': j["data"].get("shop_poi_click_num"),
                    'external_goods_order_rate_30': j["data"].get("external_goods_order_rate_30"),
                    'goods_order_price': j["data"].get("goods_order_price"),
                    'clk_live_room_roi': j["data"].get("clk_live_room_roi"),
                    'user_page_view_uv': j["data"].get("user_page_view_uv"),
                    'word_impression_rank_all': j["data"].get("word_impression_rank_all"),
                    'identity_certi_cnt': j["data"].get("identity_certi_cnt"),
                    'msg_leads_num': j["data"].get("msg_leads_num"),
                    'search_invoke_button_click_cost': j["data"].get("search_invoke_button_click_cost"),
                    'app_pay_roi': j["data"].get("app_pay_roi"),
                    'goods_visit_price': j["data"].get("goods_visit_price"),
                    'external_leads_cpl': j["data"].get("external_leads_cpl"),
                    'page_second_jump_ctr': j["data"].get("page_second_jump_ctr"),
                    'live_like': j["data"].get("live_like"),
                    'clk_live_entry_pv': j["data"].get("clk_live_entry_pv"),
                    'word_impression_rank_third': j["data"].get("word_impression_rank_third"),
                    'skip_ratio': j["data"].get("skip_ratio"),
                    'search_cmt_after_read_avg': j["data"].get("search_cmt_after_read_avg"),
                    'current_app_pay_cost': j["data"].get("current_app_pay_cost"),
                    'message_user': j["data"].get("message_user"),
                    'invoke_app_payment_cost': j["data"].get("invoke_app_payment_cost"),
                    'word_click_rank_third': j["data"].get("word_click_rank_third"),
                    'seeding_cnt': j["data"].get("seeding_cnt"),
                    'external_goods_order_price_7': j["data"].get("external_goods_order_price_7"),
                    'search_invoke_button_click_cnt': j["data"].get("search_invoke_button_click_cnt"),
                    'live_dgmv': j["data"].get("live_dgmv"),
                    'presale_order_num_7d': j["data"].get("presale_order_num_7d"),
                    'page_cmt_ctr': j["data"].get("page_cmt_ctr"),
                    'message_second_jump_ctr': j["data"].get("message_second_jump_ctr"),
                    'word_click_rank_all': j["data"].get("word_click_rank_all"),
                    'retention_3d_cnt': j["data"].get("retention_3d_cnt"),
                    'over_ratio': j["data"].get("over_ratio"),
                    'success_goods_order': j["data"].get("success_goods_order"),
                    'live_follow': j["data"].get("live_follow"),
                    'external_goods_order_price_15': j["data"].get("external_goods_order_price_15"),
                    'action_button_click': j["data"].get("action_button_click"),
                    'screenshot': j["data"].get("screenshot"),
                    'external_goods_order_rate_7': j["data"].get("external_goods_order_rate_7"),
                    'external_goods_order_24h': j["data"].get("external_goods_order_24h"),
                    'app_activate_amount_1d_roi': j["data"].get("app_activate_amount_1d_roi"),
                    'external_roi_24h': j["data"].get("external_roi_24h"),
                    'leads_cvr': j["data"].get("leads_cvr"),
                    'message_reply_in_3min_rate': j["data"].get("message_reply_in_3min_rate"),
                    'clk_live_avg_view_time': j["data"].get("clk_live_avg_view_time"),
                    'page_second_jump_click': j["data"].get("page_second_jump_click"),
                    'live_entry_pv': j["data"].get("live_entry_pv"),
                    'search_cmt_click_cvr': j["data"].get("search_cmt_click_cvr"),
                    'external_goods_order_rate_24h': j["data"].get("external_goods_order_rate_24h"),
                    'jd_active_user_num_cvr': j["data"].get("jd_active_user_num_cvr"),
                    'app_download_button_click_cnt': j["data"].get("app_download_button_click_cnt"),
                    'retention_7d_cnt': j["data"].get("retention_7d_cnt"),
                    'ctr': j["data"].get("ctr"),
                    'avg_duration_time': j["data"].get("avg_duration_time"),
                    'word_impression_rate_first': j["data"].get("word_impression_rate_first"),
                    'goods_visit_rate': j["data"].get("goods_visit_rate"),
                    'external_goods_order_price_24h': j["data"].get("external_goods_order_price_24h"),
                    'first_app_pay_ctr': j["data"].get("first_app_pay_ctr"),
                    'app_pay_amount': j["data"].get("app_pay_amount"),
                    'skip_num': j["data"].get("skip_num"),
                    'comment': j["data"].get("comment"),
                    'clk_live_all_rgmv': j["data"].get("clk_live_all_rgmv"),
                    'word_impression_rank_first': j["data"].get("word_impression_rank_first"),
                    'invoke_app_enter_store_cnt': j["data"].get("invoke_app_enter_store_cnt"),
                    'external_goods_visit_rate_7': j["data"].get("external_goods_visit_rate_7"),
                    'impression': j["data"].get("impression"),
                    'current_app_pay_cnt': j["data"].get("current_app_pay_cnt"),
                    'live_avg_view_time': j["data"].get("live_avg_view_time"),
                    'invoke_app_payment_unit_price': j["data"].get("invoke_app_payment_unit_price"),
                    'jd_active_user_num_cpl': j["data"].get("jd_active_user_num_cpl"),
                    'leads': j["data"].get("leads"),
                    'click': j["data"].get("click"),
                    'valid_leads_cpl': j["data"].get("valid_leads_cpl"),
                    'live_order_rate': j["data"].get("live_order_rate"),
                    'word_impression_rate_all': j["data"].get("word_impression_rate_all"),
                    'clk_live_comment': j["data"].get("clk_live_comment"),
                    'external_goods_visit_price_7': j["data"].get("external_goods_visit_price_7"),
                    'external_rgmv_30': j["data"].get("external_rgmv_30"),
                    'presale_order_gmv_7d': j["data"].get("presale_order_gmv_7d"),
                    'fst_message_reply_in_45s_rate': j["data"].get("fst_message_reply_in_45s_rate"),
                    'user_page_message_user_cnt': j["data"].get("user_page_message_user_cnt"),
                    'invoke_app_engagement_cnt': j["data"].get("invoke_app_engagement_cnt"),
                    'follow': j["data"].get("follow"),
                    'seeding_cpl': j["data"].get("seeding_cpl"),
                    'external_roi_15': j["data"].get("external_roi_15"),
                    'clk_live_interaction': j["data"].get("clk_live_interaction"),
                    'external_rgmv_7': j["data"].get("external_rgmv_7"),
                    'reserve_p_v': j["data"].get("reserve_p_v"),
                    'external_goods_visit_24h': j["data"].get("external_goods_visit_24h"),
                    'invoke_app_open_cnt': j["data"].get("invoke_app_open_cnt"),
                    'invoke_app_open_cost': j["data"].get("invoke_app_open_cost"),
                    'cpm': j["data"].get("cpm"),
                    'cpi': j["data"].get("cpi"),
                    'initiative_message': j["data"].get("initiative_message"),
                    'message_second_jump_click': j["data"].get("message_second_jump_click"),
                    'pis_cpl': j["data"].get("pis_cpl"),
                    'purchase_order_roi_7d': j["data"].get("purchase_order_roi_7d"),
                    'first_app_pay_cost': j["data"].get("first_app_pay_cost"),
                    'interaction': j["data"].get("interaction"),
                    'message_cmt_click': j["data"].get("message_cmt_click"),
                    'clk_live_5s_entry_uv': j["data"].get("clk_live_5s_entry_uv"),
                    'leads_button_impression': j["data"].get("leads_button_impression"),
                    'message_fst_reply_time_avg': j["data"].get("message_fst_reply_time_avg"),
                    'live_entry_uv': j["data"].get("live_entry_uv"),
                    'clk_live_all_follow': j["data"].get("clk_live_all_follow"),
                    'share': j["data"].get("share"),
                    'page_second_jump_imp': j["data"].get("page_second_jump_imp"),
                    'goods_order_rate': j["data"].get("goods_order_rate"),
                    'phone_call_cnt': j["data"].get("phone_call_cnt"),
                    'app_pay_cost_7d': j["data"].get("app_pay_cost_7d"),
                    'search_cmt_click': j["data"].get("search_cmt_click"),
                    'wechat_copy_cnt': j["data"].get("wechat_copy_cnt"),
                    'external_goods_order_rate_15': j["data"].get("external_goods_order_rate_15"),
                    'invoke_app_payment_roi': j["data"].get("invoke_app_payment_roi"),
                    'app_activate_amount_1d': j["data"].get("app_activate_amount_1d"),
                    'message_second_jump_imp': j["data"].get("message_second_jump_imp"),
                    'user_page_view_pv': j["data"].get("user_page_view_pv"),
                    'word_impression_rate_third': j["data"].get("word_impression_rate_third"),
                    'over_num': j["data"].get("over_num"),
                    'seller_visit_price': j["data"].get("seller_visit_price"),
                    'clk_live_all_order_num': j["data"].get("clk_live_all_order_num"),
                    'app_activate_cost': j["data"].get("app_activate_cost"),
                    'app_register_cost': j["data"].get("app_register_cost"),
                    'app_pay_cnt_7d': j["data"].get("app_pay_cnt_7d"),
                    'fee': j["data"].get("fee"),
                    'pic_save': j["data"].get("pic_save"),
                    'goods_order': j["data"].get("goods_order"),
                    'clk_live_5s_entry_uv_cost': j["data"].get("clk_live_5s_entry_uv_cost"),
                    'id': j["data"].get("id"),
                    'invoke_app_engagement_cost': j["data"].get("invoke_app_engagement_cost"),
                    'external_roi_7': j["data"].get("external_roi_7"),
                    'app_register_ctr': j["data"].get("app_register_ctr"),
                    'roi': j["data"].get("roi"),
                    'clk_live_room_rgmv': j["data"].get("clk_live_room_rgmv"),
                    'seeding_cvr': j["data"].get("seeding_cvr"),
                    'external_goods_visit_rate_24h': j["data"].get("external_goods_visit_rate_24h"),
                    'invoke_app_enter_store_cost': j["data"].get("invoke_app_enter_store_cost"),
                    'app_key_action_cnt': j["data"].get("app_key_action_cnt"),
                    'live_rgmv': j["data"].get("live_rgmv"),
                    'clk_live_room_follow': j["data"].get("clk_live_room_follow"),
                    'clk_live_room_order_num': j["data"].get("clk_live_room_order_num"),
                    'quality_score': j["data"].get("quality_score"),
                    'app_key_action_ctr': j["data"].get("app_key_action_ctr"),
                    'add_cart_price': j["data"].get("add_cart_price"),
                    'rgmv': j["data"].get("rgmv"),
                    'message': j["data"].get("message"),
                    'live_order_user': j["data"].get("live_order_user"),
                    'live_gift': j["data"].get("live_gift"),
                    'word_price_first': j["data"].get("word_price_first"),
                    'phone_call_succ_cnt': j["data"].get("phone_call_succ_cnt"),
                    'app_activate_amount_7d': j["data"].get("app_activate_amount_7d"),
                    'user_page_message_cnt': j["data"].get("user_page_message_cnt"),
                    'live_comment': j["data"].get("live_comment"),
                    'clk_live_5s_entry_pv': j["data"].get("clk_live_5s_entry_pv"),
                    'clk_live_all_roi': j["data"].get("clk_live_all_roi"),
                    'search_cmt_after_read': j["data"].get("search_cmt_after_read"),
                    'external_roi_30': j["data"].get("external_roi_30"),
                    'jd_active_user_num': j["data"].get("jd_active_user_num"),
                    'collect': j["data"].get("collect"),
                    'landing_page_visit': j["data"].get("landing_page_visit"),
                    'message_consult_cpl': j["data"].get("message_consult_cpl"),
                    'live_audience_cost': j["data"].get("live_audience_cost"),
                    'live_share': j["data"].get("live_share"),
                    'msg_leads_cost': j["data"].get("msg_leads_cost"),
                    'valid_leads': j["data"].get("valid_leads"),
                    'message_cmt_imp': j["data"].get("message_cmt_imp"),
                    'external_goods_order_15': j["data"].get("external_goods_order_15"),
                    'acp': j["data"].get("acp"),
                    'seller_visit': j["data"].get("seller_visit"),
                    'live_order_num': j["data"].get("live_order_num"),
                    'pis_cvr': j["data"].get("pis_cvr"),
                    'commodity_buy_cnt': j["data"].get("commodity_buy_cnt"),
                    'external_goods_order_price_30': j["data"].get("external_goods_order_price_30"),
                    'purchase_order_gmv_7d': j["data"].get("purchase_order_gmv_7d"),
                    'live_note_pv': j["data"].get("live_note_pv"),
                    'word_click_rate_all': j["data"].get("word_click_rate_all"),
                    'external_goods_visit_price_24h': j["data"].get("external_goods_visit_price_24h"),
                    'app_activate_amount_7d_roi': j["data"].get("app_activate_amount_7d_roi"),
                    'app_activate_amount_3d_roi': j["data"].get("app_activate_amount_3d_roi"),
                    'shop_name': shop_name,
                    'advertiser_id': advertiser_id,
                    'advertiser_name': advertiser_name
                }
                base_target_dto_more_data.append(base_target_dto_one_data)
                base_campaign_dto_more_data.append(base_campaign_dto_one_data)
                base_unit_dto_more_data.append(base_unit_dto_one_data)
                more_data.append(one_data)
                # print(base_unit_dto_one_data)
                # print(one_data)

        if resp["page"]["page_index"] * 100 >= resp["page"]["total_count"]:
            break
        else:
            data["page_num"] += 1

    JuGuang.gosql_v3.api_to_sql(json_data=base_target_dto_more_data, sql_name="api_xhs_jg_target_report_target")
    JuGuang.gosql_v3.api_to_sql(json_data=base_campaign_dto_more_data, sql_name="api_xhs_jg_target_report_campaign")
    JuGuang.gosql_v3.api_to_sql(json_data=base_unit_dto_more_data, sql_name="api_xhs_jg_target_report_unit")
    JuGuang.gosql_v3.api_to_sql(json_data=more_data, sql_name="api_xhs_jg_target_report_data_indicators")


today = datetime.datetime.now().strftime("%Y-%m-%d")
query_sql = "SELECT access_token,shop_name,advertiser_id,advertiser_name FROM xhs_token_info where platform = '聚光'"
shop_data = []
for adv_info in JuGuang.gosql_v3.execute_query(query_sql):
    print(adv_info)
    access_token, shop_name, advertiser_id, advertiser_name = adv_info
    get_realtime_target_lst(access_token=access_token, shop_name=shop_name, advertiser_name=advertiser_name,
                            advertiser_id=advertiser_id, start_date=today, end_date=today)

