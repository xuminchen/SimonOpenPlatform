from __future__ import annotations

import os
import tempfile
import unittest

from webapp.error_messages import PLATFORM_REQUIRED
from webapp.services.platform_configs import (
    create_platform_config,
    delete_platform_config,
    update_platform_config,
)


class PlatformConfigsServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self._old_config = os.environ.get("WONDERLAB_PLATFORM_CONFIG_FILE")
        self._tmp_dir = tempfile.TemporaryDirectory()
        os.environ["WONDERLAB_PLATFORM_CONFIG_FILE"] = f"{self._tmp_dir.name}/platform_configs.json"

    def tearDown(self) -> None:
        if self._old_config is None:
            os.environ.pop("WONDERLAB_PLATFORM_CONFIG_FILE", None)
        else:
            os.environ["WONDERLAB_PLATFORM_CONFIG_FILE"] = self._old_config
        self._tmp_dir.cleanup()

    def test_create_requires_platform(self) -> None:
        with self.assertRaisesRegex(ValueError, PLATFORM_REQUIRED):
            create_platform_config(platform="  ", label="x")

    def test_update_requires_platform(self) -> None:
        with self.assertRaisesRegex(ValueError, PLATFORM_REQUIRED):
            update_platform_config(platform="", label="x")

    def test_delete_requires_platform(self) -> None:
        with self.assertRaisesRegex(ValueError, PLATFORM_REQUIRED):
            delete_platform_config(platform=" ")


if __name__ == "__main__":
    unittest.main()
