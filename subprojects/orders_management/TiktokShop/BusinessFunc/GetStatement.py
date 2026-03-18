import datetime
import time
from BusinessFunc import gosql_v3, getToken

field_mapping = {
    'adjustment_amount': '调整金额',
    'currency': '货币',
    'fee_amount': '费用金额',
    'id': 'ID',
    'net_sales_amount': '净销售额',
    'payment_id': '支付ID',
    'payment_status': '支付状态',
    'revenue_amount': '收入金额',
    'settlement_amount': '结算金额',
    'shipping_cost_amount': '运费金额',
    'statement_time': '对账时间'
}


def get_statement(statement_time_start, statement_time_end):
    statement_time_ge = int(datetime.datetime.strptime(statement_time_start, "%Y-%m-%d").timestamp())
    statement_time_lt = int(datetime.datetime.strptime(statement_time_end, "%Y-%m-%d").timestamp())
    url = "https://open-api.tiktokglobalshop.com/finance/202309/statements"

    headers = {
        'content-type': 'application/json',
        'x-tts-access-token': getToken.access_token
    }

    params = {
        "app_key": getToken.app_key,
        "shop_cipher": "TTP_UHd08wAAAABKNQAuH4gfTjRY0btkD_bR",
        "shop_id": "7495896608152062300",
        "page_size": 100,
        "sort_field": "statement_time",
        "statement_time_ge": statement_time_ge,
        "statement_time_lt": statement_time_lt
    }
    statement = []
    while True:
        time.sleep(1)
        response_json = getToken.request_signed_json(
            method="get",
            url=url,
            params=params,
            headers=headers,
            body=None,
            event_name="tiktok_shop_get_statement",
        )
        data_len = len(response_json.get('data', {}).get('statements', []))

        for statement_data in response_json['data']['statements']:
            statement_info = {
                'adjustment_amount': statement_data.get('adjustment_amount'),
                'currency': statement_data.get('currency'),
                'fee_amount': statement_data.get('fee_amount'),
                'id': statement_data.get('id'),
                'net_sales_amount': statement_data.get('net_sales_amount'),
                'payment_id': statement_data.get('payment_id'),
                'payment_status': statement_data.get('payment_status'),
                'revenue_amount': statement_data.get('revenue_amount'),
                'settlement_amount': statement_data.get('settlement_amount'),
                'shipping_cost_amount': statement_data.get('shipping_cost_amount'),
                'statement_time': datetime.datetime.fromtimestamp(
                    statement_data.get('statement_time', 0)
                ).strftime('%Y-%m-%d %H:%M:%S')
            }
            statement.append(statement_info)

        if data_len < 100:
            break
        else:
            next_page_token = response_json['data'].get('next_page_token')
            params['page_token'] = next_page_token

    gosql_v3.api_to_sql(statement, "api_tiktok_oversea_finance_statement")


get_statement("2025-01-01", "2025-04-22")
