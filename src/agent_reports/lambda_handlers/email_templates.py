"""Email rendering for the dispatcher: one plain-text body, one HTML body, per agent.

Templates are package data (``agent_reports/templates/*``) loaded through
:mod:`importlib.resources`, so the same code works from a source checkout and from the Lambda zip.

Two deliberate choices:

* ``string.Template`` (``$name``) rather than ``str.format`` - the HTML body is full of CSS braces.
* ``substitute`` (not ``safe_substitute``) plus HTML escaping of every dynamic value: a missing
  placeholder raises instead of shipping a broken email, and an agent name containing markup cannot
  inject into the HTML body.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from html import escape
from importlib import resources
from string import Template

from ..common.report import AgentTotals

__all__ = [
    "EmailContent",
    "TEMPLATE_FILES",
    "load_template",
    "render_email",
]

TEMPLATE_FILES = {"text": "report_email.txt", "html": "report_email.html"}


@dataclass(frozen=True)
class EmailContent:
    """What gets handed to SES."""

    subject: str
    text_body: str
    html_body: str


def load_template(kind: str) -> str:
    """Read a template from package data (works from a checkout and from the Lambda zip)."""
    if kind not in TEMPLATE_FILES:
        raise ValueError(f"unknown template kind {kind!r}")
    return (
        resources.files("agent_reports")
        .joinpath("templates", TEMPLATE_FILES[kind])
        .read_text(encoding="utf-8")
    )


def render_email(
    *,
    agent_id: str,
    agent_name: str,
    region: str,
    branch: str,
    report_date: str,
    recipient: str,
    download_url: str,
    expires_at: datetime,
    expires_in_seconds: int,
    sender: str,
    totals: AgentTotals | None,
    now: datetime | None = None,
) -> EmailContent:
    """Render both bodies for one agent."""
    moment = now or datetime.now(tz=UTC)
    if expires_at.tzinfo is None:
        raise ValueError("expires_at must be timezone-aware")

    hours = max(1, round(expires_in_seconds / 3600))
    context: dict[str, str] = {
        "agent_id": agent_id,
        "agent_name": agent_name or agent_id,
        "region": region or "n/a",
        "branch": branch or "n/a",
        "report_date": report_date,
        "recipient": recipient,
        "download_url": download_url,
        "expires_at": expires_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M"),
        "expires_in_hours": f"{hours} hour{'s' if hours != 1 else ''}",
        "sender": sender,
    }
    if totals is not None:
        context.update(totals.as_email_context())
    else:
        context.update(
            {
                "policy_count": "see report",
                "premium": "see report",
                "commission": "see report",
                "claim_count": "see report",
                "claim_amount": "see report",
                "settled_amount": "see report",
                "loss_ratio": "see report",
            }
        )

    subject = f"Agent report {report_date} - {agent_id}"
    if totals is not None:
        subject = f"Agent report {report_date} - {agent_id} ({totals.policy_count} policies)"

    return EmailContent(
        subject=subject,
        text_body=Template(load_template("text")).substitute(_escaped(context, html=False)),
        html_body=Template(load_template("html")).substitute(_escaped(context, html=True)),
    )


def _escaped(context: dict[str, str], *, html: bool) -> dict[str, str]:
    if not html:
        return dict(context)
    return {key: escape(value, quote=True) for key, value in context.items()}
