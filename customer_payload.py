#!/usr/bin/env python3
"""One shape for a Salla customer, wherever it comes from (v2.11).

The Customer Queue payload (column H) is written by the Make capture scenario
as a 12-key JSON object. Until the capture builds it with Make's JSON module,
it pastes raw values into a text template, so a double quote, a backslash or
a line break in any field (an address, a last name) makes the whole payload
invalid JSON. This module gives the customer sync, the daily sweep and the
repair tools one set of helpers so they all agree on the shape:

  * salvage_template(text, cid, phone): recover the 12 fields from a payload
    the template broke, by cutting on the known key order. Refuses unless the
    salvaged id matches the row's customer id (and the phone, when given).
  * from_api(record): the same 12-key shape built from a Merchant API customer
    (`customers/{id}?fields[]=is_notifications_enabled`, or the list endpoint).
  * phone_of(payload): column C exactly as the capture writes it
    (mobile_code + mobile).
"""

import re

TEMPLATE_KEYS = ("id", "first_name", "last_name", "mobile", "mobile_code",
                 "email", "city", "gender", "lang", "birthday", "location",
                 "is_notifications_enabled")

API_FIELDS = "fields[]=is_notifications_enabled"

_LINE_BREAKS = re.compile(r"\s*[\r\n  ]+\s*")
_LITERALS = {"true": True, "false": False, "null": None}


def clean_text(value, sep=" "):
    """Single-line text for HubSpot: line breaks become `sep`, ends trimmed."""
    if value is None:
        return ""
    return _LINE_BREAKS.sub(sep, str(value)).strip()


def _digits(value):
    return re.sub(r"\D", "", str(value or ""))


def phone_of(payload):
    """Column C of the Customer Queue, as the capture builds it."""
    return f"{payload.get('mobile_code') or ''}{payload.get('mobile') or ''}"


def salvage_template(text, cid=None, phone=None):
    """Recover the 12 capture fields from a payload whose JSON the text
    template broke. Returns a dict, or None when the text is not the capture
    template or the result does not belong to this row.

    Every key must be present, in the capture's order. A value runs from its
    key to the next key, so quotes, backslashes and line breaks inside it are
    kept as typed. Values written without quotes (true, false, null, bare
    numbers) are accepted too."""
    if not text or not str(text).lstrip().startswith("{"):
        return None
    text = str(text)
    marks, pos = [], 0
    for key in TEMPLATE_KEYS:
        m = re.compile(r'"%s"\s*:\s*' % re.escape(key)).search(text, pos)
        if not m:
            return None
        marks.append((key, m.start(), m.end()))
        pos = m.end()
    closing = text.rstrip().rfind("}")
    if closing < marks[-1][2]:
        return None
    out = {}
    for i, (key, _, value_start) in enumerate(marks):
        end = marks[i + 1][1] if i + 1 < len(marks) else closing
        raw = text[value_start:end].rstrip()
        if i + 1 < len(marks):
            if not raw.endswith(","):
                return None
            raw = raw[:-1].rstrip()
        if len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"':
            value = raw[1:-1]
        elif raw in _LITERALS:
            value = _LITERALS[raw]
        elif re.fullmatch(r"-?\d+(\.\d+)?", raw or ""):
            value = raw
        else:
            return None
        out[key] = value
    if cid is not None and str(out.get("id") or "").strip() != str(cid).strip():
        return None
    if phone and out.get("mobile"):
        if not _digits(phone).endswith(_digits(out["mobile"])):
            return None
    for key in ("first_name", "last_name", "city", "gender", "lang", "email"):
        if isinstance(out.get(key), str):
            out[key] = clean_text(out[key])
    if isinstance(out.get("location"), str):
        out["location"] = clean_text(out["location"], sep=", ")
    return out


def from_api(record):
    """The 12-key capture shape from a Merchant API customer record."""
    record = record or {}
    bday = record.get("birthday")
    if isinstance(bday, dict):
        bday = bday.get("date") or ""
    flag = record.get("is_notifications_enabled")
    if isinstance(flag, bool):
        flag = "true" if flag else "false"
    elif flag is None:
        flag = ""
    return {
        "id": str(record.get("id") or ""),
        "first_name": clean_text(record.get("first_name")),
        "last_name": clean_text(record.get("last_name")),
        "mobile": str(record.get("mobile") or ""),
        "mobile_code": str(record.get("mobile_code") or ""),
        "email": clean_text(record.get("email")),
        "city": clean_text(record.get("city")),
        "gender": clean_text(record.get("gender")),
        "lang": clean_text(record.get("lang")),
        "birthday": str(bday or ""),
        "location": clean_text(record.get("location"), sep=", "),
        "is_notifications_enabled": flag,
    }
