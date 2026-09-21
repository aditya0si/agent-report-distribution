"""Email rendering: personalisation, HTML safety, and the two-body contract."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from agent_reports.common.report import AgentTotals
from agent_reports.lambda_handlers.email_templates import EmailContent, load_template, render_email

TOTALS = AgentTotals(
    agent_id="AGT-000001",
    policy_count=3,
    premium=Decimal("20000.75"),
    commission=Decimal("2800.10"),
    claim_count=4,
    claim_amount=Decimal("5700.50"),
    settled_amount=Decimal("3500.00"),
    loss_ratio=Decimal("0.2850"),
)
EXPIRES_AT = datetime(2026, 9, 20, 12, 30, tzinfo=UTC)
URL = "https://s3.amazonaws.com/agent-reports-out/reports/report.csv?X-Amz-Signature=abc123&X-Amz-Expires=900"


def render(**overrides: object) -> EmailContent:
    kwargs: dict[str, object] = {
        "agent_id": "AGT-000001",
        "agent_name": "Isha Patel",
        "region": "North",
        "branch": "Delhi",
        "report_date": "2026-09-20",
        "recipient": "agt-000001@example.com",
        "download_url": URL,
        "expires_at": EXPIRES_AT,
        "expires_in_seconds": 900,
        "sender": "reports@example.com",
        "totals": TOTALS,
        "now": datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
    }
    kwargs.update(overrides)
    return render_email(**kwargs)  # type: ignore[arg-type]


class TestRendering:
    def test_both_bodies_are_produced(self) -> None:
        content = render()
        assert content.text_body.strip()
        assert content.html_body.lstrip().startswith("<!DOCTYPE html>")

    def test_subject_carries_the_policy_count(self) -> None:
        assert render().subject == "Agent report 2026-09-20 - AGT-000001 (3 policies)"

    def test_plain_body_has_every_number(self) -> None:
        body = render().text_body
        assert "Isha Patel" in body
        assert "20,000.75" not in body  # Indian formatting is not applied; raw decimal is used
        assert "20000.75" in body
        assert "2800.10" in body
        assert "5700.50" in body
        assert "3500.00" in body
        assert "28.50%" in body
        assert "2026-09-20" in body
        assert URL in body

    def test_html_body_has_every_number_and_the_link(self) -> None:
        html = render().html_body
        assert "20000.75" in html
        assert "28.50%" in html
        # The URL is HTML-escaped inside the href attribute, so compare the escaped form.
        assert URL.replace("&", "&amp;") in html
        assert 'href="' in html

    def test_agent_name_is_html_escaped(self) -> None:
        content = render(agent_name='<script>alert("x")</script> & co')
        assert "<script>" not in content.html_body
        assert "&lt;script&gt;" in content.html_body
        assert "&amp; co" in content.html_body
        # The plain-text body keeps the original characters.
        assert '<script>alert("x")</script> & co' in content.text_body

    def test_missing_agent_name_falls_back_to_the_id(self) -> None:
        assert "AGT-000001" in render(agent_name="").text_body

    def test_missing_region_and_branch_render_as_na(self) -> None:
        body = render(region="", branch="").text_body
        assert "n/a / n/a" in body

    def test_expiry_is_rendered_in_hours(self) -> None:
        body = render(expires_in_seconds=3600).text_body
        assert "1 hour" in body
        assert "1 hours" not in body
        assert "2026-09-20 12:30" in body

    def test_multiple_hours_are_pluralised(self) -> None:
        assert "3 hours" in render(expires_in_seconds=10_800).text_body

    def test_totals_are_optional(self) -> None:
        content = render(totals=None)
        assert "see report" in content.text_body
        assert content.subject == "Agent report 2026-09-20 - AGT-000001"

    def test_naive_expiry_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            render(expires_at=datetime(2026, 9, 20, 12, 30))


class TestTemplates:
    def test_templates_are_package_data(self) -> None:
        assert "Daily report" in load_template("html")
        assert "Your daily agent report" in load_template("text")

    def test_unknown_template_kind(self) -> None:
        with pytest.raises(ValueError, match="unknown template kind"):
            load_template("pdf")

    def test_unsubstituted_placeholder_fails_loudly(self) -> None:
        from string import Template

        with pytest.raises(KeyError):
            Template("Hello $not_provided").substitute({"other": "x"})
