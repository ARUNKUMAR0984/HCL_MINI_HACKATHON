import re
import logging

logger = logging.getLogger(__name__)

# Words that must not appear as SQL *keywords* (not inside identifiers)
_FORBIDDEN = {"drop", "delete", "update", "insert", "alter", "truncate", "create", "replace"}


def validate_sql(sql: str) -> bool:
    """
    Raise ValueError for any disallowed SQL construct.

    Uses word-boundary regex so a column named 'updated_at' or
    'inserted_by' does NOT trigger a false positive.
    """
    if ";" in sql:
        raise ValueError("Multiple statements are not allowed.")

    sql_lower = sql.lower()

    for keyword in _FORBIDDEN:
        # \b = word boundary — won't match 'updated_at', only standalone 'update'
        if re.search(rf"\b{keyword}\b", sql_lower):
            raise ValueError(f"Forbidden SQL keyword detected: '{keyword}'")

    # Block comment-based injection
    if "--" in sql or "/*" in sql:
        raise ValueError("SQL comments are not allowed.")

    logger.debug("SQL passed validation: %s", sql)
    return True
