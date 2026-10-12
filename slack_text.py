#!/usr/bin/env python3
"""Plain-English Slack messages with links (v2.13).

Every alert the engines send reads the same way: a headline that is a whole
sentence on its own (what most people see on a phone), then a thread reply
in three short parts:

  *What happened*   one or two sentences, no jargon
  *What to do*      numbered steps, or "Nothing" when the system fixes it
  *Links*           the HubSpot record, the Salla page, the sheet row

Links go straight to the record: HubSpot record pages, the Salla admin page
the order payload carries, and Google Sheets links down to the exact tab and
row. Nothing here posts anything; it only builds text.
"""

import os
import re
import time

PORTAL = "148009568"            # the ClaraHair HubSpot portal (pre-approved to show)
VALIDATION_SHEET = "1VdvXPDvlI6O1qq6jIFMhIVU7PvVIr5_JMlr374NXRKg"   # Bundle & Product Validation
OBJ = {"order": "0-123", "contact": "0-1", "product": "0-7", "line_item": "0-8",
       "bundle_template": "2-203847434", "warranty": "2-252148104"}


def portal():
    return (os.environ.get("HUBSPOT_PORTAL_ID") or PORTAL).strip()


def esc(text):
    """Slack mrkdwn escapes: &, <, > (and the | that would end a link label)."""
    return (str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace("|", "/"))


def link(url, label):
    return f"<{url}|{esc(label)}>" if url else esc(label)


def hubspot(kind, record_id, label=None):
    if not record_id:
        return ""
    url = f"https://app.hubspot.com/contacts/{portal()}/record/{OBJ[kind]}/{record_id}"
    return link(url, label or f"Open in HubSpot")


def hubspot_search(term, label=None):
    return link(f"https://app.hubspot.com/search/{portal()}/search?term={term}", label or f"Search HubSpot for {term}")


def sheet_url(sheet_id, gid=None, row=None):
    if not sheet_id:
        return ""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    if gid is not None:
        url += f"#gid={gid}"
        if row:
            url += f"&range=A{row}"
    return url


def sheet(sheet_id, label, gid=None, row=None):
    return link(sheet_url(sheet_id, gid, row), label)


def salla_order(order, label=None):
    """The admin link Salla puts in the order payload, when there is one."""
    url = ((order or {}).get("urls") or {}).get("admin") or ""
    ref = (order or {}).get("reference_id") or (order or {}).get("id") or ""
    return link(url, label or f"Order #{ref} in Salla") if url else esc(label or f"Order #{ref}")


def message(what, todo=None, links=None, note=None):
    """The thread reply every alert uses."""
    parts = ["*What happened*", what.strip()]
    if todo:
        parts += ["", "*What to do*"]
        if isinstance(todo, str):
            parts.append(todo.strip())
        else:
            parts += [f"{i}. {step}" for i, step in enumerate(todo, 1)]
    links = [x for x in (links or []) if x]
    if links:
        parts += ["", "*Links*", "  •  ".join(links)]
    if note:
        parts += ["", f"_{note.strip()}_"]
    return "\n".join(parts)


def plural(n, one, many=None):
    return f"{n} {one if n == 1 else (many or one + 's')}"


# ---------------------------------------------------------------- sheet rows

class SheetIndex:
    """Tab ids and row numbers in a Google Sheet, read through GoogleIO and
    cached for a few minutes, so a message can link to the exact row."""

    def __init__(self, gio, ttl_s=600):
        self.gio, self.ttl = gio, ttl_s
        self._tabs, self._cols = {}, {}

    def tabs(self, sheet_id):
        hit = self._tabs.get(sheet_id)
        if hit and time.time() - hit[0] < self.ttl:
            return hit[1]
        meta = self.gio._gexec(self.gio.sheets.get(spreadsheetId=sheet_id,
                                                   fields="sheets.properties(sheetId,title)"),
                               "sheet tabs", self.gio.sheets_rl)
        tabs = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets") or []}
        self._tabs[sheet_id] = (time.time(), tabs)
        return tabs

    def column(self, sheet_id, tab, header):
        """{value: row} for the column whose header row names `header`.
        Raises LookupError when no header names it: a renamed column is a
        lookup that failed, not a column with no rows. [v2.14.1]"""
        key = (sheet_id, tab, header)
        hit = self._cols.get(key)
        if hit and time.time() - hit[0] < self.ttl:
            if hit[1] is None:
                raise LookupError(f"no '{header}' column on the {tab} tab")
            return hit[1]
        head = self.gio._gexec(self.gio.sheets.values().get(spreadsheetId=sheet_id, range=f"'{tab}'!1:1"),
                               "sheet header", self.gio.sheets_rl).get("values") or [[]]
        names = [str(h).strip() for h in head[0]]
        if header not in names:
            self._cols[key] = (time.time(), None)    # cached, so a renamed column is not re-read per call
            raise LookupError(f"no '{header}' column on the {tab} tab")
        col = _col_letter(names.index(header))
        vals = self.gio._gexec(self.gio.sheets.values().get(spreadsheetId=sheet_id, range=f"'{tab}'!{col}:{col}",
                                                            valueRenderOption="UNFORMATTED_VALUE"),
                               "sheet column", self.gio.sheets_rl).get("values") or []
        out = {}
        for i, v in enumerate(vals, 1):
            if i > 1 and v and str(v[0]).strip():
                out.setdefault(re.sub(r"\.0$", "", str(v[0]).strip()), i)
        self._cols[key] = (time.time(), out)
        return out

    def row_link(self, sheet_id, tab, header, value, label):
        """(link, found): a link to the row whose `header` column equals
        value, or to the tab when the value is not there; never raises.
        found is True or False when the column was read, and None when the
        lookup itself failed (Sheets error, renamed tab or column), so "no
        row" is never claimed on a read that did not happen. None is falsy:
        callers that only test found keep working. [v2.14.1]"""
        try:
            gid = self.tabs(sheet_id).get(tab)
        except Exception:
            gid = None
        try:
            row = self.column(sheet_id, tab, header).get(str(value)) if value else None
        except Exception:
            return sheet(sheet_id, label, gid), None
        return sheet(sheet_id, label, gid, row), bool(row)


def _col_letter(i):
    s = ""
    i += 1
    while i:
        i, r = divmod(i - 1, 26)
        s = chr(65 + r) + s
    return s
