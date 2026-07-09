"""Stream type classes for tap-surveymonkey.

Streams follow the SurveyMonkey API v3 (https://api.surveymonkey.com/v3/docs).
``surveys`` is the parent stream; ``survey_details``, ``collectors`` and
``responses`` are children that iterate per survey.

In addition to those nested streams, a set of **normalized** streams flatten
the nested ``/details`` and ``/responses/bulk`` payloads into a relational,
one-row-per-element grain (``survey_pages``, ``questions``,
``question_headings``, ``question_options``, ``sub_questions``,
``response_pages``, ``response_answers``). This mirrors the flattened source
tables downstream BI/dbt models expect (e.g. a Fivetran-style schema) without
requiring warehouse-specific ``UNNEST``/``FLATTEN`` SQL — the tap does the
unnesting. ``contacts`` and ``survey_categories`` are standalone list streams.

Note: SCD/history-mode (type-2 valid-from/valid-to versioning) is intentionally
NOT produced here — the tap emits current-state records; history is a
warehouse-side concern (e.g. dbt snapshots).
"""

from __future__ import annotations

import decimal
import hashlib
import re
import sys
import typing as t

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_surveymonkey.client import SurveyMonkeyStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context

# Pulls the survey id out of a ``/surveys/{id}/...`` request URL, so the
# response-derived streams can stamp it without a context round-trip.
_SURVEY_ID_IN_URL = re.compile(r"/surveys/([^/?]+)/")


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


# ---------------------------------------------------------------------------
# Normalized (flattened) streams
#
# These re-request an endpoint already consumed by a nested stream and flatten
# a specific level of the payload into a relational grain. They are children of
# ``surveys`` so they iterate per survey. Re-requesting ``/details`` is cheap
# (survey counts are low); the two response-derived streams re-request
# ``/responses/bulk`` (heavier — the header ``responses`` stream plus these two
# means the bulk endpoint is walked three times). The incremental
# ``start_modified_at`` filter bounds that volume; batching the three grains
# from a single fetch is a future optimization.
# ---------------------------------------------------------------------------


class _SurveyDetailsChildStream(SurveyMonkeyStream):
    """Base for normalized streams derived from ``/surveys/{id}/details``.

    The details endpoint returns one nested survey object. Subclasses walk the
    ``pages -> questions -> ...`` tree in :meth:`parse_response` and yield a
    flattened grain, reading ``survey_id`` from the payload's own ``id``.
    """

    parent_stream_type = SurveysStream
    path = "/surveys/{survey_id}/details"
    # Single object response, not a `data` array; each subclass walks it.
    records_jsonpath = "$"
    replication_key = None


class SurveyPagesStream(_SurveyDetailsChildStream):
    """One row per survey page (flattened from ``details.pages[]``)."""

    name = "survey_pages"
    primary_keys = ("survey_id", "id")

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Page ID."),
        th.Property("survey_id", th.StringType, description="Parent survey ID."),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("position", th.IntegerType),
        th.Property("question_count", th.IntegerType),
        th.Property("href", th.StringType),
    ).to_dict()

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield one record per page in the survey."""
        survey = response.json(parse_float=decimal.Decimal)
        survey_id = survey.get("id")
        for page in survey.get("pages") or []:
            yield {
                "id": page.get("id"),
                "survey_id": survey_id,
                "title": page.get("title"),
                "description": page.get("description"),
                "position": page.get("position"),
                "question_count": page.get("question_count"),
                "href": page.get("href"),
            }


class QuestionsStream(_SurveyDetailsChildStream):
    """One row per question (flattened from ``details.pages[].questions[]``)."""

    name = "questions"
    primary_keys = ("id",)

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Question ID."),
        th.Property("survey_id", th.StringType, description="Parent survey ID."),
        th.Property("page_id", th.StringType, description="Parent page ID."),
        th.Property("position", th.IntegerType),
        th.Property("visible", th.BooleanType),
        th.Property(
            "family",
            th.StringType,
            description="Question family, e.g. single_choice, matrix, open_ended.",
        ),
        th.Property("subtype", th.StringType, description="Question subtype."),
        th.Property("layout", th.ObjectType(additional_properties=True)),
        th.Property("sorting", th.ObjectType(additional_properties=True)),
        th.Property("required", th.ObjectType(additional_properties=True)),
        th.Property("validation", th.ObjectType(additional_properties=True)),
        th.Property("forced_ranking", th.BooleanType),
        th.Property("href", th.StringType),
    ).to_dict()

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield one record per question across all pages."""
        survey = response.json(parse_float=decimal.Decimal)
        survey_id = survey.get("id")
        for page in survey.get("pages") or []:
            page_id = page.get("id")
            for question in page.get("questions") or []:
                yield {
                    "id": question.get("id"),
                    "survey_id": survey_id,
                    "page_id": page_id,
                    "position": question.get("position"),
                    "visible": question.get("visible"),
                    "family": question.get("family"),
                    "subtype": question.get("subtype"),
                    "layout": question.get("layout"),
                    "sorting": question.get("sorting"),
                    "required": question.get("required"),
                    "validation": question.get("validation"),
                    "forced_ranking": question.get("forced_ranking"),
                    "href": question.get("href"),
                }


class QuestionHeadingsStream(_SurveyDetailsChildStream):
    """One row per question heading (``questions[].headings[]``).

    Headings carry no id of their own (a question usually has a single heading,
    but A/B random-assignment questions carry several), so the grain is keyed by
    ``question_id`` + ordinal ``heading_index``.
    """

    name = "question_headings"
    primary_keys = ("question_id", "heading_index")

    schema = th.PropertiesList(
        th.Property("question_id", th.StringType, description="Parent question ID."),
        th.Property("survey_id", th.StringType, description="Parent survey ID."),
        th.Property("page_id", th.StringType, description="Parent page ID."),
        th.Property(
            "heading_index",
            th.IntegerType,
            description="Ordinal position of the heading within the question.",
        ),
        th.Property("heading", th.StringType, description="Heading text (HTML)."),
        th.Property("description", th.StringType),
        th.Property("image", th.ObjectType(additional_properties=True)),
        th.Property(
            "random_assignment",
            th.ObjectType(additional_properties=True),
            description="A/B random-assignment config, when present.",
        ),
    ).to_dict()

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield one record per heading of every question."""
        survey = response.json(parse_float=decimal.Decimal)
        survey_id = survey.get("id")
        for page in survey.get("pages") or []:
            page_id = page.get("id")
            for question in page.get("questions") or []:
                question_id = question.get("id")
                for index, heading in enumerate(question.get("headings") or []):
                    yield {
                        "question_id": question_id,
                        "survey_id": survey_id,
                        "page_id": page_id,
                        "heading_index": index,
                        "heading": heading.get("heading"),
                        "description": heading.get("description"),
                        "image": heading.get("image"),
                        "random_assignment": heading.get("random_assignment"),
                    }


class QuestionOptionsStream(_SurveyDetailsChildStream):
    """One row per answer choice (``questions[].answers.choices[]``)."""

    name = "question_options"
    primary_keys = ("id",)

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Answer choice ID."),
        th.Property("question_id", th.StringType, description="Parent question ID."),
        th.Property("survey_id", th.StringType, description="Parent survey ID."),
        th.Property("page_id", th.StringType, description="Parent page ID."),
        th.Property("position", th.IntegerType),
        th.Property("visible", th.BooleanType),
        th.Property("text", th.StringType, description="Answer choice text."),
        th.Property("weight", th.IntegerType),
        th.Property("quiz_options", th.ObjectType(additional_properties=True)),
    ).to_dict()

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield one record per answer choice of every question."""
        survey = response.json(parse_float=decimal.Decimal)
        survey_id = survey.get("id")
        for page in survey.get("pages") or []:
            page_id = page.get("id")
            for question in page.get("questions") or []:
                question_id = question.get("id")
                answers = question.get("answers") or {}
                for choice in answers.get("choices") or []:
                    yield {
                        "id": choice.get("id"),
                        "question_id": question_id,
                        "survey_id": survey_id,
                        "page_id": page_id,
                        "position": choice.get("position"),
                        "visible": choice.get("visible"),
                        "text": choice.get("text"),
                        "weight": choice.get("weight"),
                        "quiz_options": choice.get("quiz_options"),
                    }


class SubQuestionsStream(_SurveyDetailsChildStream):
    """One row per matrix subquestion (``questions[].answers.rows[]``)."""

    name = "sub_questions"
    primary_keys = ("id",)

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Subquestion (row) ID."),
        th.Property("question_id", th.StringType, description="Parent question ID."),
        th.Property("survey_id", th.StringType, description="Parent survey ID."),
        th.Property("page_id", th.StringType, description="Parent page ID."),
        th.Property("position", th.IntegerType),
        th.Property("visible", th.BooleanType),
        th.Property("text", th.StringType, description="Subquestion (row) text."),
    ).to_dict()

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield one record per matrix row of every question."""
        survey = response.json(parse_float=decimal.Decimal)
        survey_id = survey.get("id")
        for page in survey.get("pages") or []:
            page_id = page.get("id")
            for question in page.get("questions") or []:
                question_id = question.get("id")
                answers = question.get("answers") or {}
                for row in answers.get("rows") or []:
                    yield {
                        "id": row.get("id"),
                        "question_id": question_id,
                        "survey_id": survey_id,
                        "page_id": page_id,
                        "position": row.get("position"),
                        "visible": row.get("visible"),
                        "text": row.get("text"),
                    }


class _ResponsesChildStream(SurveyMonkeyStream):
    """Base for normalized streams derived from ``/responses/bulk``.

    Mirrors :class:`ResponsesStream`'s incremental request params so the
    flattened grains only pull responses modified since the bookmark. The
    parent response's ``date_modified`` is carried onto each child row and used
    as the replication key. ``survey_id`` is read from the request URL.
    """

    parent_stream_type = SurveysStream
    path = "/surveys/{survey_id}/responses/bulk"
    records_jsonpath = "$.data[*]"
    replication_key = "date_modified"

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Add incremental + sort params, capping ``per_page`` at 100."""
        params = super().get_url_params(context, next_page_token)
        params["per_page"] = 100
        params["sort_by"] = "date_modified"
        params["sort_order"] = "ASC"
        start = self.get_starting_timestamp(context)
        if start:
            params["start_modified_at"] = start.strftime("%Y-%m-%dT%H:%M:%S")
        return params


class ResponsePagesStream(_ResponsesChildStream):
    """One row per answered response page (``responses[].pages[]``)."""

    name = "response_pages"
    primary_keys = ("response_id", "id")

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Page ID."),
        th.Property("response_id", th.StringType, description="Parent response ID."),
        th.Property("survey_id", th.StringType, description="Parent survey ID."),
        th.Property(
            "date_modified",
            th.DateTimeType,
            description="Parent response modification timestamp (replication key).",
        ),
    ).to_dict()

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield one record per page of every response."""
        match = _SURVEY_ID_IN_URL.search(response.url)
        survey_id = match.group(1) if match else None
        payload = response.json(parse_float=decimal.Decimal)
        for resp in payload.get("data") or []:
            response_id = resp.get("id")
            date_modified = resp.get("date_modified")
            for page in resp.get("pages") or []:
                yield {
                    "id": page.get("id"),
                    "response_id": response_id,
                    "survey_id": survey_id,
                    "date_modified": date_modified,
                }


class ResponseAnswersStream(_ResponsesChildStream):
    """One row per answer (``responses[].pages[].questions[].answers[]``).

    Individual answers carry no natural key, so ``id`` is a deterministic
    surrogate hashed from the identifying fields — stable across runs, so
    targets can upsert on it.
    """

    name = "response_answers"
    primary_keys = ("id",)

    _schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Surrogate answer key."),
        th.Property("response_id", th.StringType, description="Parent response ID."),
        th.Property("survey_id", th.StringType, description="Parent survey ID."),
        th.Property("page_id", th.StringType, description="Parent page ID."),
        th.Property("question_id", th.StringType, description="Parent question ID."),
        th.Property("choice_id", th.StringType),
        th.Property("row_id", th.StringType),
        th.Property("col_id", th.StringType),
        th.Property("other_id", th.StringType),
        th.Property("text", th.StringType, description="Free-text answer value."),
        th.Property("tag_data", th.ArrayType(th.ObjectType(additional_properties=True))),
        th.Property(
            "date_modified",
            th.DateTimeType,
            description="Parent response modification timestamp (replication key).",
        ),
    ).to_dict()
    # Answer objects vary by question family; keep unexpected keys rather than
    # silently dropping answer variants (e.g. simple_text, download_url).
    _schema["additionalProperties"] = True
    schema = _schema

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield one record per answer across every response/page/question."""
        match = _SURVEY_ID_IN_URL.search(response.url)
        survey_id = match.group(1) if match else None
        payload = response.json(parse_float=decimal.Decimal)
        for resp in payload.get("data") or []:
            response_id = resp.get("id")
            date_modified = resp.get("date_modified")
            for page in resp.get("pages") or []:
                page_id = page.get("id")
                for question in page.get("questions") or []:
                    question_id = question.get("id")
                    for answer in question.get("answers") or []:
                        row = {
                            "response_id": response_id,
                            "survey_id": survey_id,
                            "page_id": page_id,
                            "question_id": question_id,
                            "choice_id": answer.get("choice_id"),
                            "row_id": answer.get("row_id"),
                            "col_id": answer.get("col_id"),
                            "other_id": answer.get("other_id"),
                            "text": answer.get("text"),
                            "tag_data": answer.get("tag_data"),
                            "date_modified": date_modified,
                        }
                        row["id"] = self._surrogate_key(row)
                        yield row

    @staticmethod
    def _surrogate_key(row: dict) -> str:
        """Return a deterministic surrogate key for an answer row."""
        parts = (
            row["response_id"],
            row["page_id"],
            row["question_id"],
            row["choice_id"],
            row["row_id"],
            row["col_id"],
            row["other_id"],
            row["text"],
        )
        digest = "|".join("" if p is None else str(p) for p in parts)
        return hashlib.sha1(digest.encode()).hexdigest()  # noqa: S324


class ContactsStream(SurveyMonkeyStream):
    """Address-book contacts.

    Requires the ``contacts_read`` scope.
    https://api.surveymonkey.com/v3/docs#api-endpoints-get-contacts
    """

    name = "contacts"
    path = "/contacts"
    primary_keys = ("id",)
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Contact ID."),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("email", th.StringType),
        th.Property("phone_number", th.StringType),
        th.Property("status", th.StringType),
        th.Property("custom_fields", th.ObjectType(additional_properties=True)),
        th.Property("href", th.StringType),
    ).to_dict()


class SurveyCategoriesStream(SurveyMonkeyStream):
    """Survey categories available to the account.

    https://api.surveymonkey.com/v3/docs#api-endpoints-get-survey_categories
    """

    name = "survey_categories"
    path = "/survey_categories"
    primary_keys = ("id",)
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Category ID."),
        th.Property("name", th.StringType, description="Category display name."),
    ).to_dict()
