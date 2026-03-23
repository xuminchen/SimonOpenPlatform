from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest import mock
import unittest

from webapp.services.connection_connectors import RedReportConnector


_RED_REPORT_TIMEZONE = timezone(timedelta(hours=8))


class RedReportConnectorTest(unittest.TestCase):
    def test_pull_data_calls_payload_style_client_methods(self) -> None:
        captured: dict[str, object] = {}

        class FakeClient:
            def __init__(self, *, access_token: str, **_: object) -> None:
                captured["access_token"] = access_token

            def offline_campaign_report(self, payload: dict[str, object]) -> dict[str, object]:
                captured["payload"] = payload
                return {
                    "code": 0,
                    "success": True,
                    "data": {
                        "data_list": [{"campaign_id": "c1", "time": "2026-03-22"}],
                        "total_count": 1,
                        "page_num": 1,
                        "page_size": 1,
                    },
                }

        connector = RedReportConnector(platform_code="red_juguang", streams=[])
        credential = {"access_token": "token", "advertiser_id": 1001}

        with mock.patch("webapp.services.connection_connectors.RedJuGuangApiClient", FakeClient):
            result = connector.pull_data("offline_campaign", credential, state={"page_size": 1, "limit": 1})

        self.assertEqual(captured["access_token"], "token")
        self.assertIsInstance(captured["payload"], dict)
        self.assertEqual(result["records"][0]["campaign_id"], "c1")
        self.assertEqual(result["records"][0]["advertiser_id"], 1001)

    def test_pull_data_calls_keyword_only_client_methods_with_kwargs_and_clamps_dates(self) -> None:
        captured: dict[str, object] = {}
        expected_max_day = (datetime.now(_RED_REPORT_TIMEZONE) - timedelta(days=1)).strftime("%Y-%m-%d")

        class FakeClient:
            def __init__(self, *, access_token: str, **_: object) -> None:
                captured["access_token"] = access_token

            def offline_note_report(
                self,
                *,
                advertiser_id: int,
                start_date: str,
                end_date: str,
                page_num: int,
                page_size: int,
                auto_paginate: bool,
            ) -> dict[str, object]:
                captured["kwargs"] = {
                    "advertiser_id": advertiser_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "page_num": page_num,
                    "page_size": page_size,
                    "auto_paginate": auto_paginate,
                }
                return {
                    "code": 0,
                    "success": True,
                    "data": {
                        "data_list": [{"note_id": "n1", "time": end_date}],
                        "total_count": 1,
                        "page_num": 1,
                        "page_size": 1,
                    },
                }

        connector = RedReportConnector(platform_code="red_juguang", streams=[])
        credential = {"access_token": "token", "advertiser_id": 2002}
        state = {
            "start_time": "2099-12-31",
            "end_time": "2099-12-31T23:59:59",
            "page_size": 1,
            "limit": 1,
        }

        with mock.patch("webapp.services.connection_connectors.RedJuGuangApiClient", FakeClient):
            result = connector.pull_data("offline_note", credential, state=state)

        kwargs = captured["kwargs"]
        self.assertIsInstance(kwargs, dict)
        self.assertEqual(kwargs["advertiser_id"], 2002)
        self.assertEqual(kwargs["start_date"], expected_max_day)
        self.assertEqual(kwargs["end_date"], expected_max_day)
        self.assertEqual(result["next_state"]["start_date"], expected_max_day)
        self.assertEqual(result["next_state"]["end_date"], expected_max_day)
        self.assertEqual(result["records"][0]["note_id"], "n1")
        self.assertEqual(result["records"][0]["advertiser_id"], 2002)


if __name__ == "__main__":
    unittest.main()
