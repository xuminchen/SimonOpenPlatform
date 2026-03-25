from __future__ import annotations

import json
from types import SimpleNamespace
from unittest import mock
import unittest

from fastapi import HTTPException

from webapp.routers import accounts as accounts_router
from webapp.security import decrypt_text, encrypt_text
from webapp.services import token_refresh


class _FakeDBForMarkFailed:
    def __init__(self) -> None:
        self.add_calls = 0
        self.commit_calls = 0
        self.refresh_calls = 0

    def add(self, _: object) -> None:
        self.add_calls += 1

    def commit(self) -> None:
        self.commit_calls += 1

    def refresh(self, _: object) -> None:
        self.refresh_calls += 1


class _FakeQuery:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def order_by(self, _: object) -> "_FakeQuery":
        return self

    def all(self) -> list[object]:
        return self._rows


class _FakeDBForScheduler:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows
        self.rollback_calls = 0

    def query(self, _: object) -> _FakeQuery:
        return _FakeQuery(self._rows)

    def rollback(self) -> None:
        self.rollback_calls += 1


class TokenRefreshFailureStatusTest(unittest.TestCase):
    def test_mark_account_token_refresh_failed_persists_status(self) -> None:
        db = _FakeDBForMarkFailed()
        account = SimpleNamespace(
            platform="oceanengine",
            name="demo-account",
            config_encrypted=encrypt_text(
                json.dumps(
                    {
                        "app_id": "app-demo",
                        "secret_key": "secret-demo",
                        "token": {
                            "access_token": "old-token",
                            "refresh_token": "old-refresh",
                            "token_status": "ready",
                        },
                    },
                    ensure_ascii=False,
                )
            ),
        )

        with mock.patch("webapp.services.token_refresh.sync_account_to_credentials_file") as mocked_sync:
            changed = token_refresh._mark_account_token_refresh_failed(db, account, "refresh timeout")

        self.assertTrue(changed)
        self.assertEqual(db.add_calls, 1)
        self.assertEqual(db.commit_calls, 1)
        self.assertEqual(db.refresh_calls, 1)

        stored_config = json.loads(decrypt_text(account.config_encrypted))
        token_block = stored_config.get("token")
        self.assertIsInstance(token_block, dict)
        self.assertEqual(token_block.get("token_status"), "refresh_failed")
        self.assertTrue(str(token_block.get("last_refresh_at", "")).strip())
        self.assertEqual(token_block.get("last_error"), "refresh timeout")

        self.assertEqual(mocked_sync.call_count, 1)
        sync_kwargs = mocked_sync.call_args.kwargs
        self.assertEqual(sync_kwargs["platform"], "oceanengine")
        self.assertEqual(sync_kwargs["account_name"], "demo-account")

    def test_refresh_managed_tokens_once_marks_failed_accounts(self) -> None:
        account = SimpleNamespace(id=101, platform="oceanengine")
        db = _FakeDBForScheduler([account])

        with mock.patch(
            "webapp.services.token_refresh.refresh_account_token_if_needed",
            side_effect=RuntimeError("network boom"),
        ), mock.patch(
            "webapp.services.token_refresh._mark_account_token_refresh_failed",
            return_value=True,
        ) as mocked_mark:
            summary = token_refresh.refresh_managed_tokens_once(db)

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["refreshed"], 0)
        self.assertEqual(summary["skipped"], 0)
        self.assertEqual(db.rollback_calls, 1)
        mocked_mark.assert_called_once()

    def test_manual_refresh_failure_marks_credential_source_status(self) -> None:
        entry = SimpleNamespace(
            platform="oceanengine",
            name="demo-app",
            app_id="app-001",
            config={
                "app_id": "app-001",
                "secret_key": "secret-001",
                "token": {
                    "refresh_token": "r-001",
                    "token_status": "ready",
                },
            },
        )

        with mock.patch(
            "webapp.routers.accounts.find_credential_entry_by_app_id",
            return_value=entry,
        ), mock.patch(
            "webapp.routers.accounts.bootstrap_tokens_for_config",
            side_effect=RuntimeError("forced failure"),
        ), mock.patch("webapp.routers.accounts.sync_account_to_credentials_file") as mocked_sync:
            with self.assertRaises(HTTPException) as ctx:
                accounts_router._refresh_credential_source_token_by_app_id("app-001")

        self.assertEqual(ctx.exception.status_code, 502)
        self.assertIn("更新失败", str(ctx.exception.detail))
        self.assertEqual(mocked_sync.call_count, 1)

        sync_kwargs = mocked_sync.call_args.kwargs
        self.assertEqual(sync_kwargs["platform"], "oceanengine")
        self.assertEqual(sync_kwargs["account_name"], "demo-app")
        written = sync_kwargs["config"]
        self.assertEqual(written["app_id"], "app-001")
        self.assertEqual(written["token"]["token_status"], "refresh_failed")
        self.assertTrue(str(written["token"].get("last_refresh_at", "")).strip())


if __name__ == "__main__":
    unittest.main()
