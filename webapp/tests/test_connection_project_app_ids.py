from __future__ import annotations

import json
from types import SimpleNamespace
from datetime import datetime, timezone
import unittest

from webapp.services.connections import update_project_app_ids


class _FakeDB:
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

    def get(self, *_: object) -> None:
        return None


class ConnectionProjectAppIdsTest(unittest.TestCase):
    def test_update_project_app_ids_updates_project_payload(self) -> None:
        db = _FakeDB()
        project = SimpleNamespace(
            id=1,
            name="demo",
            platform_code="red_juguang",
            credential_id=None,
            app_id="app_old",
            app_ids_json='["app_old"]',
            schedule_cron="0 * * * *",
            destination="ClickHouse_DW",
            status=1,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )

        view = update_project_app_ids(db, project=project, app_ids=[" app_new ", "app_new", "", "app_b"])

        self.assertEqual(project.app_id, "app_new")
        self.assertEqual(json.loads(project.app_ids_json), ["app_new", "app_b"])
        self.assertEqual(view.app_id, "app_new")
        self.assertEqual(view.app_ids, ["app_new", "app_b"])
        self.assertEqual(db.add_calls, 1)
        self.assertEqual(db.commit_calls, 1)
        self.assertEqual(db.refresh_calls, 1)

    def test_update_project_app_ids_rejects_empty_payload(self) -> None:
        db = _FakeDB()
        project = SimpleNamespace(
            id=2,
            name="demo",
            platform_code="red_juguang",
            credential_id=None,
            app_id="app_old",
            app_ids_json='["app_old"]',
            schedule_cron="0 * * * *",
            destination="ClickHouse_DW",
            status=1,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )

        with self.assertRaises(ValueError):
            update_project_app_ids(db, project=project, app_ids=["", " "])


if __name__ == "__main__":
    unittest.main()
