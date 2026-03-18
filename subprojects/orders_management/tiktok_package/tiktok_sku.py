from tiktok_package.tiktok_auth import Tiktok
from utils import *
from decimal import Decimal
import datetime
import math
import json
import time
import argparse


class TiktokSku(Tiktok):
    def __init__(self):
        super().__init__()

    def get_sku_info(self,shop_id,sku_id = None):
        # method = "product.detail"
        method = "product.listV2"
        param = {
            # "product_id":sku_id,
            "store_id": int(shop_id),
            "status":0,
            "size": 100,
            "page": 1
        }
        if(sku_id):
            param['product_id'] = sku_id
        token = self.get_token(shop_id)
        resp = self.fetch_resp(method,param,token)

        import time

        time.sleep(5)


        with open(f"./{shop_id}.json", "w") as f:
            f.write(json.dumps(resp))
        # print(resp)
        print("Done!")

if __name__ == '__main__':
    app = TiktokSku()
    # for shop_id in app.shop_id_mapping.keys():
    #     print(shop_id)
    #     order_list = app.get_order_search_list(start_time, end_time, shop_id)
    #     all_orders.extend(order_list)
    #
    # print(all_orders)

    shop_id = "20184489"
    sku_id = "3519238806731637221"

    app.get_sku_info(shop_id)

