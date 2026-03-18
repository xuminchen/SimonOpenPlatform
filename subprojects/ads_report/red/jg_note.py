import JuGuang
import datetime
import json
import requests
import getToken
from subprojects._shared.core import db_client as gosql_v3
from subprojects._shared.core.api_credentials import get_credentials

status_map = {
    0: "正常",
    1: "不匹配",
    2: "非法",
    3: "已删除",
    4: "反作弊处罚等级",
    8: "私信笔记",
    9: "淘宝笔记",
    10: "设置隐私",
    11: "抽奖笔记",
    15: "反作弊处罚等级",
    20: "笔记需要强绑spu",
    26: "反作弊处罚等级",
    37: "高展拒绝",
    "-": "未知"
}
note_content_map = {
    1: "图文笔记",
    2: "视频笔记",
    "-": "未知"
}

cooperate_component_type_map = {
    1: "评论区商品组件",
    2: "评论区店铺组件",
    3: "评论区搜索组件",
    4: "评论区私信组件",
    6: "评论区POI组件",
    "-": "未知"
}
crowd_creation_note_map = {
    0: "否",
    1: "是",
    "-": "未知"
}
high_quality_map = {
    0: "否",
    1: "是",
    "-": "未知"
}
high_potential_map = {
    0: "否",
    1: "是",
    "-": "未知"
}


def get_note_list(access_token=None, shop_name='', advertiser_name='', advertiser_id=None):
    if not access_token:
        access_token = get_credentials("xhs_juguang", "apps", "jg_note_default", "access_token", default="")
    if not advertiser_id:
        advertiser_id = get_credentials("xhs_juguang", "apps", "jg_note_default", "advertiser_id", default="")
    if not access_token or not advertiser_id:
        raise ValueError("Missing xhs_juguang.apps.jg_note_default.access_token/advertiser_id in config/api_credentials.json")

    url = "https://adapi.xiaohongshu.com/api/open/jg/note/list"
    header = {
        "content-type": "application/json",
        "Access-Token": access_token
    }
    note_type_list = [1, 2]
    more_data = []
    for a in note_type_list:
        note_type = a
        data = {
            "advertiser_id": advertiser_id,
            "note_type": note_type,
            "page": 1,
            "page_size": 100,
            "base_only": "1"
        }
        while True:
            resp = requests.post(url=url, headers=header, data=json.dumps(data)).json()
            if resp["data"].get("notes"):
                for i in resp["data"]["notes"]:
                    one_data = {
                        'desc': i.get("desc", None),
                        'read_rate': i.get("read_rate", None),
                        'note_id': i.get("note_id", None),
                        'image': i.get("image", None),
                        'author': i.get("author", None),
                        'has_shop_card': i.get("has_shop_card", None),
                        'title': i.get("title", None),
                        'interact_count': i.get("interact_count", 0),
                        'outside_shop_visit': i.get("outside_shop_visit", 0),
                        'create_time': datetime.datetime.fromtimestamp(int(i.get("create_time", 0)) // 1000).strftime(
                            '%Y-%m-%d %H:%M:%S'),
                        'cooperate_state_authorization': i.get("cooperate_state_authorization", 0),
                        'high_quality': high_quality_map.get(i.get("high_quality", "-")),
                        'has_shop': i.get("has_shop", None),
                        'note_type': i.get("note_type", None),
                        'status': status_map.get(i.get("status", "-")),
                        'note_content_type': note_content_map.get(i.get("note_content_type", "-")),
                        'cooperate_state': i.get("cooperate_state", None),
                        'author_image': i.get("author_image", None),
                        'crowd_creation_note': crowd_creation_note_map.get(i.get("crowd_creation_note", "-")),
                        'has_local_shop_goods': i.get("has_local_shop_goods", None),
                        'outside_shop_visit_rate': i.get("outside_shop_visit_rate", None),
                        'cooperate_component_type': cooperate_component_type_map.get(
                            i.get("cooperate_component_type", "-")),
                        'read_count': i.get("read_count", 0),
                        'order_count': i.get("order_count", 0),
                        'interact_rate': i.get("interact_rate", None),
                        'high_potential': high_quality_map.get(i.get("high_potential", "-")),
                        'has_shop_goods': i.get("has_shop_goods", None),
                        'shop_name': shop_name,
                        'advertiser_id': advertiser_id,
                        'advertiser_name': advertiser_name
                    }
                    more_data.append(one_data)

                if resp['data'] and len(resp['data']['notes']) == 100:
                    data['page'] += 1
                else:
                    break
            else:
                break

    print(f'{advertiser_name}:{advertiser_id}笔记数据已跑完')
    return more_data


query_sql = "SELECT access_token,shop_name,advertiser_id,advertiser_name FROM xhs_token_info where platform = '聚光'"
shop_data = []
for adv_info in gosql_v3.execute_query(query_sql):
    print(adv_info)
    access_token, shop_name, advertiser_id, advertiser_name = adv_info
    shop_data.extend(get_note_list(access_token=access_token, shop_name=shop_name, advertiser_name=advertiser_name,
                                   advertiser_id=advertiser_id))

first_sql = f'truncate table api_xhs_jg_note_data'
gosql_v3.api_to_sql(shop_data, 'api_xhs_jg_note_data', need_clean='yes', first_execute_sql=first_sql)
requests.get(
    get_credentials("bi_refresh_urls", "ds_j1e4a0b8bedd642d2b3faa0f", required=True))
# print(shop_data[0:1])
