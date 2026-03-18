import requests

import Tools
import gosql_v3
import datetime
from subprojects._shared.core.api_credentials import get_credentials


def get_video_material(access_token, account_id):
    interface = 'videos/get'
    parameters = {
        "account_id": account_id,
        "page": 1,
        "page_size": 10
    }
    field_list = ['video_id', 'width', 'height', 'video_frames', 'video_fps', 'video_codec', 'video_bit_rate',
                  'audio_codec', 'audio_bit_rate', 'file_size', 'type', 'signature', 'system_status', 'description',
                  'preview_url', 'key_frame_image_url', 'created_time', 'last_modified_time', 'video_profile_name',
                  'audio_sample_rate', 'max_keyframe_interval', 'min_keyframe_interval', 'sample_aspect_ratio',
                  'audio_profile_name', 'scan_type', 'image_duration_millisecond', 'audio_duration_millisecond',
                  'source_type', 'product_catalog_id', 'product_outer_id', 'source_reference_id', 'owner_account_id',
                  'status', 'source_material_id', 'new_source_type', 'aigc_type', 'first_publication_status',
                  'quality_status']
    video_data = Tools.get_api_data(interface=interface, parameters=parameters, access_token=access_token,
                                    fields=field_list)
    gosql_v3.api_to_sql(json_data=video_data, sql_name="api_tencent_ad_video_material")


if __name__ == '__main__':
    # 主体： 深圳精准健康食物科技有限公司第一分公司
    token = Tools.ACCESS_TOKEN
    Tools.get_business_unit_account(access_token=token)
    for i in Tools.organization_account_relation_get(access_token=token):
        account = i["account_id"]
        get_video_material(access_token=token, account_id=account)

    requests.get(get_credentials("bi_refresh_urls", "ds_sf3dad44d23d6467fbf25e04", required=True))