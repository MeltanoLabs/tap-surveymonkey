"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os

from singer_sdk.testing import get_tap_test_class

from tap_surveymonkey.tap import TapSurveyMonkey

SAMPLE_CONFIG = {
    "access_token": os.environ.get("TAP_SURVEYMONKEY_ACCESS_TOKEN", "test-token"),
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ",
    ),
}


# Run standard built-in tap tests from the SDK:
TestTapSurveyMonkey = get_tap_test_class(
    tap_class=TapSurveyMonkey,
    config=SAMPLE_CONFIG,
)
