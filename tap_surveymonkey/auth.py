"""SurveyMonkey authentication.

SurveyMonkey issues long-lived OAuth access tokens and, for most account tiers,
does not hand back a refresh token. The primary auth path is therefore a static
bearer token (see ``SurveyMonkeyStream.authenticator``). These authenticators
exist so that, if a token is ever issued with an expiry, it can be refreshed:

* ``SurveyMonkeyProxyAuthenticator`` — refreshes via a Matatika OAuth proxy that
  holds the client credentials server-side (the platform-managed path).
* ``SurveyMonkeyAuthenticator`` — refreshes directly against SurveyMonkey's
  token endpoint using a client id/secret + refresh token.
"""

from __future__ import annotations

import datetime
import sys
from typing import Any

import requests
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

TOKEN_ENDPOINT = "https://api.surveymonkey.com/oauth/token"  # noqa: S105


class SurveyMonkeyAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Direct OAuth2 authenticator (client id + secret + refresh token)."""

    def __init__(self, *args: Any, refresh_token: str | None, **kwargs: Any) -> None:
        """Initialize the authenticator.

        Args:
            args: Positional arguments passed to the parent.
            refresh_token: The OAuth refresh token.
            kwargs: Keyword arguments passed to the parent.
        """
        super().__init__(*args, **kwargs)
        self.refresh_token = refresh_token

    @override
    @property
    def oauth_request_body(self) -> dict:
        """Return the OAuth request body for a direct token refresh."""
        return {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }


class SurveyMonkeyProxyAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator that refreshes via a proxy handling client creds server-side."""

    def __init__(
        self,
        *args: Any,
        refresh_token: str | None,
        proxy_auth_header: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the proxy authenticator.

        Args:
            args: Positional arguments passed to the parent.
            refresh_token: The OAuth refresh token.
            proxy_auth_header: Optional ``Authorization`` header value for the proxy.
            kwargs: Keyword arguments passed to the parent.
        """
        super().__init__(*args, **kwargs)
        self.refresh_token = refresh_token
        self._proxy_auth_header = proxy_auth_header

    @override
    @property
    def oauth_request_body(self) -> dict:
        """Return the OAuth request body posted to the proxy."""
        return {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }

    @override
    def update_access_token(self) -> None:
        """Request a new access token from the OAuth proxy URL.

        The proxy expects a JSON body (unlike the standard form-encoded refresh),
        so this overrides the default implementation.

        Raises:
            RuntimeError: When the token refresh request fails.
        """
        self.logger.info("Requesting new access token via OAuth proxy")
        request_time = datetime.datetime.now(tz=datetime.timezone.utc)
        headers = {}
        if self._proxy_auth_header:
            headers["Authorization"] = self._proxy_auth_header

        response = requests.post(
            self.auth_endpoint,
            headers=headers,
            json=self.oauth_request_body,
            timeout=60,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as ex:
            text = ex.response.text if ex.response is not None else "Error"
            status = ex.response.status_code if ex.response is not None else "Unknown"
            msg = f"Failed to update access token via proxy (status={status}): {text}"
            raise RuntimeError(msg) from ex

        token_json = response.json()
        self.access_token = token_json["access_token"]
        expiration = token_json.get("expires_in")
        self.expires_in = int(expiration) if expiration else None
        self.last_refreshed = request_time
