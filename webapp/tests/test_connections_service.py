from __future__ import annotations

import unittest

from webapp.services.connections import _connector_failure_message


class ConnectionsServiceTest(unittest.TestCase):
    def test_connector_failure_message_returns_empty_when_any_request_succeeds(self) -> None:
        result = {
            "raw_responses_by_advertiser": [
                {"advertiser_id": 1, "ok": False, "message": "结束时间不为空，不能为今天，且格式需为yyyy-mm-dd"},
                {"advertiser_id": 2, "ok": True, "message": "成功"},
            ]
        }

        self.assertEqual(_connector_failure_message(result), "")

    def test_connector_failure_message_returns_first_upstream_error_when_all_fail(self) -> None:
        result = {
            "raw_responses_by_advertiser": [
                {"advertiser_id": 1, "ok": False, "message": "结束时间不为空，不能为今天，且格式需为yyyy-mm-dd"},
                {"advertiser_id": 2, "ok": False, "message": "结束时间不为空，不能为今天，且格式需为yyyy-mm-dd"},
            ]
        }

        self.assertEqual(
            _connector_failure_message(result),
            "结束时间不为空，不能为今天，且格式需为yyyy-mm-dd",
        )


if __name__ == "__main__":
    unittest.main()
