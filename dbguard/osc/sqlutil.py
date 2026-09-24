"""Identifier quoting, ALTER clause parsing and the INSTANT eligibility rule."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

SHADOW_PREFIX = "__osc_new_"
OLD_PREFIX = "__osc_old_"
TRIGGER_PREFIX = "__osc_"
MAX_TABLE_NAME = 64 - len(SHADOW_PREFIX)  # every derived name must fit in 64 chars


def q(name: str) -> str:
    """Quote one identifier with backticks."""
    return "`" + name.replace("`", "``") + "`"


def qt(db: str, table: str) -> str:
    return f"{q(db)}.{q(table)}"


def shadow_name(table: str) -> str:
    return SHADOW_PREFIX + table


def old_name(table: str) -> str:
    return OLD_PREFIX + table


def trigger_names(table: str) -> dict[str, str]:
    return {ev: f"{TRIGGER_PREFIX}{ev.lower()[:3]}_{table}" for ev in ("INSERT", "UPDATE", "DELETE")}


def split_top_level(text: str, sep: str = ",") -> list[str]:
    """Split on `sep` outside parentheses and quotes. `a INT, KEY k (a, b)` gives two parts."""
    parts, buf, depth, quote = [], [], 0, None
    i = 0
    while i < len(text):
        ch = text[i]
        if quote:
            buf.append(ch)
            if ch == "\\" and quote != "`" and i + 1 < len(text):
                buf.append(text[i + 1])
                i += 2
                continue
            if ch == quote:
                if i + 1 < len(text) and text[i + 1] == quote:  # doubled quote
                    buf.append(text[i + 1])
                    i += 2
                    continue
                quote = None
        elif ch in "'\"`":
            quote = ch
            buf.append(ch)
        elif ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth -= 1
            buf.append(ch)
        elif ch == sep and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return [p for p in parts if p]


def _strip_quoted(text: str) -> str:
    """Blank out quoted strings and parenthesised groups so keyword tests see only the top
    level of a clause (a DEFAULT 'after x' must not look like AFTER)."""
    out, depth, quote = [], 0, None
    for ch in text:
        if quote:
            if ch == quote:
                quote = None
            out.append(" ")
        elif ch in "'\"":
            quote = ch
            out.append(" ")
        elif ch == "(":
            depth += 1
            out.append(" ")
        elif ch == ")":
            depth -= 1
            out.append(" ")
        else:
            out.append(" " if depth else ch)
    return "".join(out)


def _ident(tok: str) -> str:
    tok = tok.strip()
    if tok.startswith("`") and tok.endswith("`"):
        return tok[1:-1].replace("``", "`")
    return tok


_IDENT = r"(`(?:[^`]|``)+`|[A-Za-z0-9_$]+)"


@dataclass
class Clause:
    text: str
    kind: str                 # add_column, add_index, drop_column, drop_pk, drop_index,
                              # modify, change, rename_column, rename_table, other
    column: str | None = None
    positioned: bool = False  # ADD COLUMN ... AFTER x / FIRST
    unique: bool = False


@dataclass
class AlterSpec:
    text: str
    clauses: list[Clause] = field(default_factory=list)

    def kinds(self) -> set[str]:
        return {c.kind for c in self.clauses}


def parse_alter(text: str) -> AlterSpec:
    """Classify each top-level clause of the ALTER text given to `--alter` (the part after
    `ALTER TABLE t`). Anything not recognised is `other`, which the caller treats as
    "not INSTANT, copy it"."""
    text = text.strip().rstrip(";").strip()
    if re.match(r"(?i)^alter\s+table\s", text):
        raise ValueError("--alter takes the clauses only, without ALTER TABLE <name>")
    spec = AlterSpec(text=text)
    for raw in split_top_level(text):
        top = _strip_quoted(raw)
        u = top.upper()
        words = u.split()
        c = Clause(text=raw, kind="other")
        if re.match(r"^DROP\s+PRIMARY\s+KEY\b", u):
            c.kind = "drop_pk"
        elif re.match(r"^DROP\s+(INDEX|KEY|FOREIGN\s+KEY|CHECK|CONSTRAINT)\b", u):
            c.kind = "drop_index"
        elif re.match(r"^DROP\b", u):
            m = re.match(r"(?i)^DROP\s+(?:COLUMN\s+)?" + _IDENT, raw.strip())
            c.kind, c.column = "drop_column", _ident(m.group(1)) if m else None
        elif re.match(r"^ADD\s+(CONSTRAINT\b.*)?(PRIMARY\s+KEY)\b", u):
            c.kind = "add_pk"
        elif re.match(r"^ADD\s+(CONSTRAINT\s+\S+\s+)?(UNIQUE|INDEX|KEY|FULLTEXT|SPATIAL|"
                      r"FOREIGN|CHECK)\b", u):
            c.kind = "add_index"
            c.unique = "UNIQUE" in words[:4]
            if "FULLTEXT" in words[:3]:
                c.kind = "add_fulltext"
        elif re.match(r"^ADD\b", u):
            if re.match(r"^ADD\s+(COLUMN\s+)?\(", raw.strip().upper()):
                c.kind = "other"  # ADD COLUMN (a INT, b INT) list form, treat as copy
            else:
                m = re.match(r"(?i)^ADD\s+(?:COLUMN\s+)?" + _IDENT, raw.strip())
                c.kind, c.column = "add_column", _ident(m.group(1)) if m else None
                c.positioned = bool(re.search(r"\b(AFTER|FIRST)\b", u))
                if re.search(r"\bAS\b", u):
                    c.kind = "add_generated"
        elif re.match(r"^RENAME\s+(COLUMN)\b", u):
            m = re.match(r"(?i)^RENAME\s+COLUMN\s+" + _IDENT, raw.strip())
            c.kind, c.column = "rename_column", _ident(m.group(1)) if m else None
        elif re.match(r"^RENAME\b", u):
            c.kind = "rename_table"
        elif re.match(r"^CHANGE\b", u):
            m = re.match(r"(?i)^CHANGE\s+(?:COLUMN\s+)?" + _IDENT + r"\s+" + _IDENT, raw.strip())
            c.kind = "change"
            if m:
                c.column = _ident(m.group(1))
                if _ident(m.group(2)).lower() != c.column.lower():
                    c.kind = "rename_column"
        elif re.match(r"^MODIFY\b", u):
            m = re.match(r"(?i)^MODIFY\s+(?:COLUMN\s+)?" + _IDENT, raw.strip())
            c.kind, c.column = "modify", _ident(m.group(1)) if m else None
        elif re.match(r"^(ALGORITHM|LOCK)\b", u):
            c.kind = "algorithm"
        spec.clauses.append(c)
    return spec


@dataclass
class InstantVerdict:
    ok: bool
    reason: str


def instant_verdict(spec: AlterSpec, *, has_fulltext: bool, row_format: str | None,
                    total_row_versions: int | None) -> InstantVerdict:
    """The conservative INSTANT rule (MySQL 8.4 manual, "Online DDL Operations" and the
    ALGORITHM=INSTANT limits): every clause is a plain ADD COLUMN at the end of the table,
    the table has no FULLTEXT index, is not ROW_FORMAT=COMPRESSED, and has used fewer than
    64 instant row versions. 8.0.29 and later also allow AFTER/FIRST and DROP COLUMN
    instantly, but this tool keeps to the case it has measured and lets MySQL refuse the rest.
    """
    if not spec.clauses:
        return InstantVerdict(False, "empty ALTER")
    kinds = spec.kinds()
    if kinds != {"add_column"}:
        return InstantVerdict(False, "not only ADD COLUMN (" + ", ".join(sorted(kinds)) + ")")
    if any(c.positioned for c in spec.clauses):
        return InstantVerdict(False, "ADD COLUMN uses AFTER or FIRST")
    if has_fulltext:
        return InstantVerdict(False, "table has a FULLTEXT index")
    if (row_format or "").upper() == "COMPRESSED":
        return InstantVerdict(False, "table is ROW_FORMAT=COMPRESSED")
    if total_row_versions is not None and total_row_versions >= 64:
        return InstantVerdict(False, f"table already has {total_row_versions} row versions "
                                     "(INSTANT allows 64, rebuild first)")
    return InstantVerdict(True, "only ADD COLUMN at the end, no FULLTEXT index")
