from __future__ import annotations

import unittest

from webapp.schemas import DestinationFileListResponse


class DestinationSchemaContractTest(unittest.TestCase):
    def test_destination_file_list_response_excludes_absolute_path(self) -> None:
        payload = {
            "profile_id": 1,
            "profile_name": "local_profile",
            "relative_path": "destinations/local_profile",
            "absolute_path": "/tmp/should-not-leak",
            "files": [],
        }
        model = DestinationFileListResponse(**payload)

        serialized = model.model_dump()
        self.assertNotIn("absolute_path", serialized)
        self.assertEqual(serialized["relative_path"], "destinations/local_profile")


if __name__ == "__main__":
    unittest.main()
