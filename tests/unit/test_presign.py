"""Presign handler: deny-by-default authz, TTL handling, and a URL that really resolves."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import cast

import pytest

from agent_reports.common.keys import report_key
from agent_reports.common.report import AgentTotals, render_report_csv
from agent_reports.common.settings import Settings
from agent_reports.common.storage import Zones
from agent_reports.lambda_handlers.presign import (
    PRIVILEGED_ROLES,
    authorize,
    caller_identity,
    handler,
)

REPORT_DATE = "2026-09-20"
AGENT = "AGT-000001"
DETAIL = {
    "agent_id": AGENT,
    "agent_name": "Rohan Bose",
    "region": "East",
    "branch": "Kolkata",
    "policy_id": "POL-0000000001",
    "product": "Group Health",
    "policy_start": "2026-02-01",
    "policy_end": "2027-02-01",
    "sum_insured": "1000000.00",
    "premium": "12000.50",
    "commission": "720.03",
    "claim_count": 1,
    "claim_amount": "3000.00",
    "settled_amount": "2500.00",
}
TOTALS = AgentTotals(
    agent_id=AGENT,
    policy_count=1,
    premium=Decimal("12000.50"),
    commission=Decimal("720.03"),
    claim_count=1,
    claim_amount=Decimal("3000.00"),
    settled_amount=Decimal("2500.00"),
    loss_ratio=Decimal("0.2500"),
)


@pytest.fixture
def report_object(zones: Zones) -> str:
    key = report_key(REPORT_DATE, AGENT)
    zones.reports.put_bytes(key, render_report_csv([DETAIL], TOTALS).encode("utf-8"))
    return key


def api_event(
    *,
    agent_id: str = AGENT,
    date: str = REPORT_DATE,
    caller: str | None = AGENT,
    role: str = "",
    via_claims: bool = True,
) -> dict[str, object]:
    event: dict[str, object] = {"queryStringParameters": {"agent_id": agent_id, "date": date}}
    if caller is None:
        return event
    if via_claims:
        event["requestContext"] = {"authorizer": {"claims": {"sub": caller, "custom:role": role}}}
    else:
        event["headers"] = {"X-Caller-Agent-Id": caller, "X-Caller-Role": role}
    return event


def body(response: dict[str, object]) -> dict[str, object]:
    return cast(dict[str, object], json.loads(str(response["body"])))


class TestAuthorization:
    def test_an_agent_may_read_their_own_report(self) -> None:
        authorize(AGENT, "", AGENT)

    @pytest.mark.parametrize("role", sorted(PRIVILEGED_ROLES))
    def test_privileged_roles_may_read_any_report(self, role: str) -> None:
        authorize("AGT-000999", role, AGENT)

    def test_another_agent_is_denied(self) -> None:
        from agent_reports.common.errors import AuthzDeniedError

        with pytest.raises(AuthzDeniedError) as excinfo:
            authorize("AGT-000002", "", AGENT)
        assert excinfo.value.context["caller_id"] == "AGT-000002"

    def test_unknown_role_is_denied(self) -> None:
        from agent_reports.common.errors import AuthzDeniedError

        with pytest.raises(AuthzDeniedError):
            authorize("AGT-000999", "intern", AGENT)

    def test_missing_identity_is_denied(self) -> None:
        from agent_reports.common.errors import AuthzDeniedError

        with pytest.raises(AuthzDeniedError, match="no caller identity"):
            authorize("", "reports-admin", AGENT)


class TestCallerIdentity:
    def test_claims_are_preferred(self) -> None:
        event = api_event(caller="AGT-000007", role="ops")
        assert caller_identity(event) == ("AGT-000007", "ops")

    def test_headers_are_the_fallback(self) -> None:
        event = api_event(caller="AGT-000007", role="finance", via_claims=False)
        assert caller_identity(event) == ("AGT-000007", "finance")

    def test_no_identity(self) -> None:
        assert caller_identity({}) == ("", "")


class TestHandler:
    def test_self_service_returns_a_working_url(
        self, handler_env: Settings, zones: Zones, report_object: str
    ) -> None:
        response = handler(api_event())
        assert response["statusCode"] == 200
        payload = body(response)
        assert payload["agent_id"] == AGENT
        assert payload["report_date"] == REPORT_DATE
        assert payload["report_key"] == report_object
        assert payload["expires_in"] == handler_env.presign_ttl_seconds == 900
        assert "X-Amz-Expires=900" in str(payload["url"])
        assert "X-Amz-Signature=" in str(payload["url"])
        assert str(payload["expires_at"]).endswith("Z")

        from agent_reports.testing import fetch_url

        status, content = fetch_url(str(payload["url"]))
        assert status == 200
        assert content == zones.reports.get_bytes(report_object)

    def test_download_is_an_attachment(
        self, handler_env: Settings, zones: Zones, report_object: str
    ) -> None:
        payload = body(handler(api_event()))
        assert "response-content-disposition" in str(payload["url"]).lower()

    def test_admin_role_can_presign_for_anyone(
        self, handler_env: Settings, zones: Zones, report_object: str
    ) -> None:
        response = handler(api_event(caller="AGT-000999", role="reports-admin"))
        assert response["statusCode"] == 200

    def test_another_agent_gets_403(
        self, handler_env: Settings, zones: Zones, report_object: str
    ) -> None:
        response = handler(api_event(caller="AGT-000002"))
        assert response["statusCode"] == 403
        assert body(response)["error"] == "forbidden"

    def test_anonymous_gets_401(
        self, handler_env: Settings, zones: Zones, report_object: str
    ) -> None:
        response = handler(api_event(caller=None))
        assert response["statusCode"] == 401
        assert body(response)["error"] == "unauthenticated"

    def test_missing_report_gets_404(self, handler_env: Settings, zones: Zones) -> None:
        """An agent asking for their own report before aggregation ran gets a 404, not a 403."""
        response = handler(api_event(agent_id="AGT-000042", caller="AGT-000042"))
        assert response["statusCode"] == 404
        assert body(response)["error"] == "not_found"

    @pytest.mark.parametrize(
        "agent_id, date",
        [
            ("AGT-1", REPORT_DATE),
            ("../../etc/passwd", REPORT_DATE),
            (AGENT, "20-09-2026"),
            ("", REPORT_DATE),
        ],
    )
    def test_malformed_input_gets_400(
        self, handler_env: Settings, zones: Zones, agent_id: str, date: str
    ) -> None:
        response = handler(api_event(agent_id=agent_id, date=date))
        assert response["statusCode"] == 400
        assert body(response)["error"] == "invalid_request"

    def test_authorization_is_checked_before_existence(
        self, handler_env: Settings, zones: Zones
    ) -> None:
        """An unauthorised caller must not be able to probe which reports exist."""
        response = handler(api_event(agent_id="AGT-000042", caller="AGT-000002"))
        assert response["statusCode"] == 403

    def test_response_is_not_cached(
        self, handler_env: Settings, zones: Zones, report_object: str
    ) -> None:
        response = handler(api_event())
        assert response["headers"]["Cache-Control"] == "no-store"
        assert response["headers"]["Content-Type"] == "application/json"
