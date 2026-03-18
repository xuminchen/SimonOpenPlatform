import json

import requests
import Tools
import gosql_v3
import datetime


def get_video_list(account_id):
    # 获取视频素材维度报表
    start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    interface = 'videos/get'
    parameters = {
        "account_id": account_id,
        "filtering": json.dumps([]),
        "page_size": 100,
        "page": 1
    }
    fields = ['video_id', 'width', 'height', 'video_frames', 'video_fps', 'video_codec', 'video_bit_rate', 'audio_codec', 'audio_bit_rate', 'file_size', 'type', 'signature', 'system_status', 'description', 'preview_url', 'key_frame_image_url', 'created_time', 'last_modified_time', 'video_profile_name', 'audio_sample_rate', 'max_keyframe_interval', 'min_keyframe_interval', 'sample_aspect_ratio', 'audio_profile_name', 'scan_type', 'image_duration_millisecond', 'audio_duration_millisecond', 'source_type', 'product_catalog_id', 'product_outer_id', 'source_reference_id', 'owner_account_id', 'status', 'source_material_id', 'new_source_type', 'aigc_type', 'first_publication_status', 'quality_status', 'cover_id', 'similarity_status']

    for data_item in Tools.get_api_data(interface, parameters, fields):
        if data_item["product_outer_id"]:
            print(data_item)


if __name__ == '__main__':
    # 主体： 深圳精准健康食物科技有限公司第一分公司
    Tools.get_business_unit_account()

    for i in Tools.organization_account_relation_get():
        account = i["account_id"]
        get_video_list(account)
