"""SurveyMonkey tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_surveymonkey import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapSurveyMonkey(Tap):
    """Singer tap for SurveyMonkey."""

    name = "tap-surveymonkey"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            secret=True,
            title="Access Token",
            description=(
                "A SurveyMonkey OAuth access token. Used directly as a bearer "
                "token. When the connection is managed by the platform OAuth "
                "app, this is supplied via `oauth_credentials` instead."
            ),
        ),
        th.Property(
            "oauth_credentials",
            th.ObjectType(
                th.Property(
                    "access_token",
                    th.StringType,
                    secret=True,
                    description="The OAuth access token.",
                ),
                th.Property(
                    "refresh_token",
                    th.StringType,
                    secret=True,
                    description="The OAuth refresh token, if the token expires.",
                ),
                th.Property(
                    "refresh_proxy_url",
                    th.StringType,
                    description=(
                        "URL of an OAuth proxy that refreshes the token "
                        "server-side using stored client credentials."
                    ),
                ),
                th.Property(
                    "refresh_proxy_url_auth",
                    th.StringType,
                    secret=True,
                    description="Authorization header value for the refresh proxy.",
                ),
                th.Property(
                    "client_id",
                    th.StringType,
                    secret=True,
                    description="OAuth app client ID (for direct refresh).",
                ),
                th.Property(
                    "client_secret",
                    th.StringType,
                    secret=True,
                    description="OAuth app client secret (for direct refresh).",
                ),
                additional_properties=True,
            ),
            description=(
                "OAuth credentials injected by the platform OAuth app. Prefer a "
                "refresh_proxy_url + refresh_token so tokens can be refreshed."
            ),
        ),
        th.Property(
            "client_id",
            th.StringType,
            secret=True,
            title="Client ID",
            description="SurveyMonkey OAuth app client ID (for direct refresh).",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            secret=True,
            title="Client Secret",
            description="SurveyMonkey OAuth app client secret (for direct refresh).",
        ),
        th.Property(
            "api_url",
            th.StringType,
            title="API URL",
            default="https://api.surveymonkey.com/v3",
            description=(
                "Base URL of the regional SurveyMonkey API. Accounts have a "
                "data-residency region determined at sign-up: US "
                "`https://api.surveymonkey.com/v3` (default), EU "
                "`https://api.eu.surveymonkey.com/v3`, or CA "
                "`https://api.surveymonkey.ca/v3`. Using the wrong region's "
                "host will cause authentication or lookup failures."
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            title="Start Date",
            description=(
                "The earliest record date to sync, in ISO-8601 format "
                "(e.g. 2026-01-01T00:00:00Z). Used as the incremental bookmark "
                "for the `responses` stream."
            ),
        ),
        th.Property(
            "user_agent",
            th.StringType,
            title="User Agent",
            description="Optional User-Agent header to send with API requests.",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.SurveyMonkeyStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.SurveysStream(self),
            streams.SurveyDetailsStream(self),
            streams.CollectorsStream(self),
            streams.RecipientsStream(self),
            streams.ResponsesStream(self),
        ]


if __name__ == "__main__":
    TapSurveyMonkey.cli()
