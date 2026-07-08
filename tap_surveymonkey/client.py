"""REST client handling, including SurveyMonkeyStream base class."""

from __future__ import annotations

import datetime
import decimal
import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl

from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.streams import RESTStream

from tap_surveymonkey.auth import (
    TOKEN_ENDPOINT,
    SurveyMonkeyAuthenticator,
    SurveyMonkeyProxyAuthenticator,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Auth, Context

# SurveyMonkey serves regionally-hosted APIs. The token exchange returns an
# `access_url` naming the region an account lives in; the US host is the
# default and is used when `api_url` is not configured.
#   US: https://api.surveymonkey.com/v3
#   EU: https://api.eu.surveymonkey.com/v3
#   CA: https://api.surveymonkey.ca/v3
API_URL = "https://api.surveymonkey.com/v3"


class SurveyMonkeyPaginator(BaseHATEOASPaginator):
    """Paginator that follows SurveyMonkey ``links.next`` URLs."""

    @override
    def get_next_url(self, response: requests.Response) -> str | None:
        """Return the ``links.next`` URL from a response, if present."""
        return response.json().get("links", {}).get("next")


class SurveyMonkeyStream(RESTStream):
    """SurveyMonkey stream class."""

    records_jsonpath = "$.data[*]"

    @override
    @property
    def url_base(self) -> str:
        """Return the SurveyMonkey API v3 base URL.

        Defaults to the US host. Accounts hosted in the EU or CA data
        residency regions must set ``api_url`` to the matching regional host,
        or requests will fail against the wrong region.
        """
        return self.config.get("api_url") or API_URL

    @override
    @cached_property
    def authenticator(self) -> Auth:
        """Return an authenticator, selected from the configured credentials.

        SurveyMonkey tokens are long-lived, so a static bearer token is the
        common case. The ``oauth_credentials`` branches add token refresh for
        the rarer case where a token is issued with an expiry.
        """
        # 1. Static bearer token supplied directly.
        if access_token := self.config.get("access_token"):
            return BearerTokenAuthenticator(token=access_token)

        oauth_creds = self.config.get("oauth_credentials") or {}

        # 2. Platform proxy refresh (client credentials held server-side).
        if oauth_creds.get("refresh_proxy_url"):
            authenticator = SurveyMonkeyProxyAuthenticator(
                auth_endpoint=oauth_creds["refresh_proxy_url"],
                refresh_token=oauth_creds.get("refresh_token"),
                proxy_auth_header=oauth_creds.get("refresh_proxy_url_auth"),
            )
            if token := oauth_creds.get("access_token"):
                # Treat the supplied long-lived token as valid so we don't
                # refresh needlessly on the first request.
                authenticator.access_token = token
                authenticator.last_refreshed = datetime.datetime.now(
                    tz=datetime.timezone.utc,
                )
                authenticator.expires_in = None
            return authenticator

        # 3. Pre-supplied OAuth access token with no refresh mechanism.
        if token := oauth_creds.get("access_token"):
            return BearerTokenAuthenticator(token=token)

        # 4. Direct client-credential refresh.
        return SurveyMonkeyAuthenticator(
            auth_endpoint=TOKEN_ENDPOINT,
            client_id=oauth_creds.get("client_id") or self.config.get("client_id"),
            client_secret=oauth_creds.get("client_secret")
            or self.config.get("client_secret"),
            refresh_token=oauth_creds.get("refresh_token"),
        )

    @property
    @override
    def http_headers(self) -> dict:
        """Return HTTP headers to send with each request."""
        headers: dict[str, str] = {}
        if user_agent := self.config.get("user_agent"):
            headers["User-Agent"] = user_agent
        return headers

    @override
    def get_new_paginator(self) -> SurveyMonkeyPaginator:
        """Return a paginator that follows ``links.next`` URLs."""
        return SurveyMonkeyPaginator()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL query params, carrying over the next-page cursor.

        SurveyMonkey list endpoints allow up to 1000 resources per page, so we
        request the maximum to minimise round-trips and rate-limit pressure.
        The ``/responses/bulk`` endpoint caps ``per_page`` at 100 and overrides
        this in :class:`ResponsesStream`.
        """
        params: dict[str, Any] = {"per_page": 1000}
        if next_page_token:
            params.update(parse_qsl(next_page_token.query))
        return params

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and yield result records."""
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )
