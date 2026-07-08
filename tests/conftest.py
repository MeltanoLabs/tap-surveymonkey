"""Pytest configuration.

The standard SDK tap tests exercise a full sync, which would otherwise make
live calls to the SurveyMonkey API. The ``_mock_surveymonkey_api`` fixture stubs
every endpoint the streams hit so the suite runs offline, with no credentials
and no dependency on a (expiring) real access token.
"""

from __future__ import annotations

import re
import sys
from typing import TYPE_CHECKING

import pytest
import responses

if TYPE_CHECKING:
    from collections.abc import Iterator

# A single survey drives every child stream (details/collectors/responses).
SURVEY_ID = "123456789"

_SURVEYS_PAGE = {
    "data": [
        {
            "id": SURVEY_ID,
            "title": "Customer Satisfaction",
            "nickname": "CSAT",
            "href": f"https://api.surveymonkey.com/v3/surveys/{SURVEY_ID}",
            "category": "customer_feedback",
            "language": "en",
            "question_count": 5,
            "page_count": 1,
            "response_count": 1,
            "folder_id": "0",
            "date_created": "2020-01-01T00:00:00+00:00",
            "date_modified": "2020-01-02T00:00:00+00:00",
        },
    ],
    "links": {},
}

_SURVEY_DETAILS = {
    "id": SURVEY_ID,
    "title": "Customer Satisfaction",
    "nickname": "CSAT",
    "category": "customer_feedback",
    "language": "en",
    "question_count": 5,
    "page_count": 1,
    "response_count": 1,
    "date_created": "2020-01-01T00:00:00+00:00",
    "date_modified": "2020-01-02T00:00:00+00:00",
    "href": f"https://api.surveymonkey.com/v3/surveys/{SURVEY_ID}/details",
    "pages": [
        {
            "id": "1",
            "title": "Page 1",
            "description": "",
            "position": 1,
            "question_count": 5,
            "href": f"https://api.surveymonkey.com/v3/surveys/{SURVEY_ID}/pages/1",
            "questions": [],
        },
    ],
}

_COLLECTORS_PAGE = {
    "data": [
        {
            "id": "987654321",
            "name": "Web Link",
            "href": "https://api.surveymonkey.com/v3/collectors/987654321",
        },
    ],
    "links": {},
}

_RESPONSES_PAGE = {
    "data": [
        {
            "id": "555555555",
            "collector_id": "987654321",
            "recipient_id": "",
            "total_time": 42,
            "href": "https://api.surveymonkey.com/v3/responses/555555555",
            "ip_address": "203.0.113.1",
            "response_status": "completed",
            "analyze_url": "",
            "custom_variables": {},
            "date_created": "2020-01-02T00:00:00+00:00",
            "date_modified": "2020-01-02T00:00:00+00:00",
            "pages": [],
        },
    ],
    "links": {},
}

# Match against any configured region host (US/EU/CA), ignoring query strings.
_HOST = r"https://api\.(eu\.)?surveymonkey\.(com|ca)/v3"


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest."""
    if sys.version_info < (3, 11):
        config.addinivalue_line(
            "filterwarnings",
            "once:Python 3.10 will reach its end of life on 2026-10:FutureWarning",
        )


@pytest.fixture(autouse=True, scope="session")
def _mock_surveymonkey_api() -> Iterator[None]:
    """Stub the SurveyMonkey v3 endpoints so tests run without live creds.

    Session-scoped so the mock is active before the SDK's record-capturing
    fixtures (which sync during setup) run.
    """
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.get(re.compile(rf"{_HOST}/surveys(\?.*)?$"), json=_SURVEYS_PAGE)
        rsps.get(
            re.compile(rf"{_HOST}/surveys/[^/]+/details(\?.*)?$"),
            json=_SURVEY_DETAILS,
        )
        rsps.get(
            re.compile(rf"{_HOST}/surveys/[^/]+/collectors(\?.*)?$"),
            json=_COLLECTORS_PAGE,
        )
        rsps.get(
            re.compile(rf"{_HOST}/surveys/[^/]+/responses/bulk(\?.*)?$"),
            json=_RESPONSES_PAGE,
        )
        yield
