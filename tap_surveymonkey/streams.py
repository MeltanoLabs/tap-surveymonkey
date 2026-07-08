"""Stream type classes for tap-surveymonkey.

Streams follow the SurveyMonkey API v3 (https://api.surveymonkey.com/v3/docs).
``surveys`` is the parent stream; ``survey_details``, ``collectors`` and
``responses`` are children that iterate per survey.
"""

from __future__ import annotations

import sys
import typing as t

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_surveymonkey.client import SurveyMonkeyStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class SurveysStream(SurveyMonkeyStream):
    """List of surveys in the account.

    https://api.surveymonkey.com/v3/docs#api-endpoints-get-surveys
    """

    name = "surveys"
    path = "/surveys"
    primary_keys = ("id",)
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Survey ID."),
        th.Property("title", th.StringType, description="Survey title."),
        th.Property("nickname", th.StringType, description="Survey nickname."),
        th.Property("href", th.StringType, description="API resource URL."),
        th.Property("category", th.StringType),
        th.Property("language", th.StringType),
        th.Property("question_count", th.IntegerType),
        th.Property("page_count", th.IntegerType),
        th.Property("response_count", th.IntegerType),
        th.Property("folder_id", th.StringType),
        th.Property("date_created", th.DateTimeType),
        th.Property("date_modified", th.DateTimeType),
        th.Property("preview", th.StringType),
        th.Property("analyze_url", th.StringType),
        th.Property("summary_url", th.StringType),
        th.Property("collect_url", th.StringType),
    ).to_dict()

    @override
    def get_child_context(
        self,
        record: dict,
        context: Context | None,
    ) -> dict:
        """Return a context dict for child streams, keyed by ``survey_id``."""
        return {"survey_id": record["id"]}


class SurveyDetailsStream(SurveyMonkeyStream):
    """Full survey definition, including pages and questions.

    https://api.surveymonkey.com/v3/docs#api-endpoints-get-surveys-id-details
    """

    name = "survey_details"
    path = "/surveys/{survey_id}/details"
    primary_keys = ("id",)
    replication_key = None
    parent_stream_type = SurveysStream
    # The details endpoint returns a single object, not a `data` array.
    records_jsonpath = "$"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Survey ID."),
        th.Property("title", th.StringType),
        th.Property("nickname", th.StringType),
        th.Property("category", th.StringType),
        th.Property("language", th.StringType),
        th.Property("question_count", th.IntegerType),
        th.Property("page_count", th.IntegerType),
        th.Property("response_count", th.IntegerType),
        th.Property("date_created", th.DateTimeType),
        th.Property("date_modified", th.DateTimeType),
        th.Property("href", th.StringType),
        th.Property(
            "pages",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("title", th.StringType),
                    th.Property("description", th.StringType),
                    th.Property("position", th.IntegerType),
                    th.Property("question_count", th.IntegerType),
                    th.Property("href", th.StringType),
                    th.Property(
                        "questions",
                        th.ArrayType(th.ObjectType(additional_properties=True)),
                    ),
                    additional_properties=True,
                ),
            ),
        ),
    ).to_dict()


class CollectorsStream(SurveyMonkeyStream):
    """Collectors (distribution channels) for each survey.

    https://api.surveymonkey.com/v3/docs#api-endpoints-get-surveys-id-collectors
    """

    name = "collectors"
    path = "/surveys/{survey_id}/collectors"
    primary_keys = ("id",)
    replication_key = None
    parent_stream_type = SurveysStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Collector ID."),
        th.Property("survey_id", th.StringType, description="Parent survey ID."),
        th.Property("name", th.StringType),
        th.Property("href", th.StringType),
    ).to_dict()

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Stamp the parent ``survey_id`` onto each collector record."""
        if context:
            row["survey_id"] = context.get("survey_id")
        return row

    @override
    def get_child_context(
        self,
        record: dict,
        context: Context | None,
    ) -> dict:
        """Return context for child streams, keyed by collector and survey."""
        return {
            "survey_id": record.get("survey_id"),
            "collector_id": record["id"],
        }


class RecipientsStream(SurveyMonkeyStream):
    """Recipients of each collector (email-invite distribution lists).

    Web-link collectors have no recipients and return an empty list.

    https://api.surveymonkey.com/v3/docs#api-endpoints-get-collectors-id-recipients
    """

    name = "recipients"
    path = "/collectors/{collector_id}/recipients"
    primary_keys = ("id",)
    replication_key = None
    parent_stream_type = CollectorsStream

    # These fields are only returned when named in the `include` query param.
    _INCLUDE_FIELDS = (
        "survey_response_status,mail_status,custom_fields,"
        "extra_fields,remove_link,survey_link"
    )

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Recipient ID."),
        th.Property("collector_id", th.StringType, description="Parent collector ID."),
        th.Property("survey_id", th.StringType, description="Parent survey ID."),
        th.Property("email", th.StringType, description="Recipient email address."),
        th.Property("phone_number", th.StringType, description="Recipient phone."),
        th.Property("href", th.StringType, description="API resource URL."),
        th.Property(
            "survey_response_status",
            th.StringType,
            description="How far the recipient got: "
            "not_responded, partially_responded, completely_responded.",
        ),
        th.Property(
            "mail_status",
            th.StringType,
            description="Invite delivery status: sent, not_sent or processing.",
        ),
        th.Property("custom_fields", th.ObjectType(additional_properties=True)),
        th.Property("extra_fields", th.ObjectType(additional_properties=True)),
        th.Property("remove_link", th.StringType, description="Unsubscribe link."),
        th.Property("survey_link", th.StringType, description="Survey link in invite."),
    ).to_dict()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Request the optional recipient fields via the ``include`` param."""
        params = super().get_url_params(context, next_page_token)
        params["include"] = self._INCLUDE_FIELDS
        return params

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Stamp the parent ``collector_id`` and ``survey_id`` onto each row."""
        if context:
            row["collector_id"] = context.get("collector_id")
            row["survey_id"] = context.get("survey_id")
        return row


class ResponsesStream(SurveyMonkeyStream):
    """Survey responses (bulk), including answers.

    https://api.surveymonkey.com/v3/docs#api-endpoints-get-surveys-id-responses-bulk
    """

    name = "responses"
    path = "/surveys/{survey_id}/responses/bulk"
    primary_keys = ("id",)
    replication_key = "date_modified"
    parent_stream_type = SurveysStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Response ID."),
        th.Property("survey_id", th.StringType, description="Parent survey ID."),
        th.Property("collector_id", th.StringType),
        th.Property("recipient_id", th.StringType),
        th.Property("total_time", th.IntegerType),
        th.Property("href", th.StringType),
        th.Property("ip_address", th.StringType),
        th.Property("response_status", th.StringType),
        th.Property("analyze_url", th.StringType),
        th.Property("custom_variables", th.ObjectType(additional_properties=True)),
        th.Property("date_created", th.DateTimeType),
        th.Property(
            "date_modified",
            th.DateTimeType,
            description="Last modification timestamp — replication key.",
        ),
        th.Property(
            "pages",
            th.ArrayType(th.ObjectType(additional_properties=True)),
            description="Answered pages and their question answers.",
        ),
    ).to_dict()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Add incremental + sort params for the bulk responses endpoint."""
        params = super().get_url_params(context, next_page_token)
        # The bulk responses endpoint caps per_page at 100 (unlike the 1000
        # allowed on other list endpoints), so pin it back down here.
        params["per_page"] = 100
        params["sort_by"] = "date_modified"
        params["sort_order"] = "ASC"
        start = self.get_starting_timestamp(context)
        if start:
            params["start_modified_at"] = start.strftime("%Y-%m-%dT%H:%M:%S")
        return params

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Stamp the parent ``survey_id`` onto each response record."""
        if context:
            row["survey_id"] = context.get("survey_id")
        return row
