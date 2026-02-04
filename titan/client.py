import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, Generator, Iterable, Optional, TypeVar, Union

import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError

logger = logging.getLogger("titan")

UNSUPPORTED_FEATURE = 2
SYNTAX_ERROR = 1003
OBJECT_ALREADY_EXISTS_ERR = 2002
DOES_NOT_EXIST_ERR = 2003
INVALID_IDENTIFIER = 2004
OBJECT_DOES_NOT_EXIST_ERR = 2043
ACCESS_CONTROL_ERR = 3001
ALREADY_EXISTS_ERR = 3041  # Not sure this is correct
INVALID_GRANT_ERR = 3042
FEATURE_NOT_ENABLED_ERR = 3078  # Unsure if this is just Replication Groups or not

connection_params = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
}

_EXECUTION_CACHE = {}


def reset_cache():
    global _EXECUTION_CACHE
    _EXECUTION_CACHE = {}


def execute(
    conn_or_cursor: Union[SnowflakeConnection, SnowflakeCursor],
    sql: str,
    cacheable: bool = False,
    empty_response_codes: Optional[list[int]] = None,
) -> list:
    if isinstance(sql, str):
        sql_text = sql
    else:
        raise Exception(f"Unknown sql type: {type(sql)}, {sql}")

    if isinstance(conn_or_cursor, SnowflakeConnection):
        session = conn_or_cursor
        cur = session.cursor(snowflake.connector.DictCursor)
    elif isinstance(conn_or_cursor, SnowflakeCursor):
        session = conn_or_cursor.connection
        cur = conn_or_cursor
        cur._use_dict_result = True
    else:
        # Undocumented snowpark-specific type snowflake.connector.connection.StoredProcConnection
        # raise Exception(f"Unknown connection type: {type(conn_or_cursor)}, {conn_or_cursor}")
        session = conn_or_cursor
        cur = session.cursor(snowflake.connector.DictCursor)

    if sql.startswith("USE ROLE"):
        desired_role = sql.split(" ", 2)[-1]
        # TODO: determine if this works at all for quoted roles
        if desired_role == session.role:
            return [[]]

    session_header = f"[{session.user}:{session.role}] > {sql_text}"

    if cacheable and session.role in _EXECUTION_CACHE and sql_text in _EXECUTION_CACHE[session.role]:
        result = _EXECUTION_CACHE[session.role][sql_text]
        # logger.warning(f"{session_header}    \033[94m({len(result)} rows, cached)\033[0m")
        return result

    start = time.time()
    try:
        cur.execute(sql_text)
        result = cur.fetchall()
        runtime = time.time() - start
        logger.warning(f"{session_header}    \033[94m({len(result)} rows, {runtime:.2f}s)\033[0m")
        if cacheable:
            if session.role not in _EXECUTION_CACHE:
                _EXECUTION_CACHE[session.role] = {}
            _EXECUTION_CACHE[session.role][sql_text] = result
        return result
    except ProgrammingError as err:
        if empty_response_codes and err.errno in empty_response_codes:
            runtime = time.time() - start
            logger.warning(f"{session_header}    \033[94m(empty, {runtime:.2f}s)\033[0m")
            if cacheable:
                if session.role not in _EXECUTION_CACHE:
                    _EXECUTION_CACHE[session.role] = {}
                _EXECUTION_CACHE[session.role][sql_text] = []
            return []
        logger.error(f"{session_header}    \033[31m(err {err.errno}, {time.time() - start:.2f}s)\033[0m")
        raise ProgrammingError(f"{err} on {sql_text}", errno=err.errno) from err


def execute_in_parallel(
    conn_or_cursor: Union[SnowflakeConnection, SnowflakeCursor],
    sqls: Iterable[tuple[str, Any]],
    error_handler: Optional[Callable[[Exception, str], None]] = None,
    cacheable: bool = False,
    empty_response_codes: Optional[list[int]] = None,
    max_workers: int = 8,
) -> Generator[tuple[Any, list], None, None]:
    """
    Execute a function in parallel using ThreadPoolExecutor and yield results as they complete.

    Args:
        conn_or_cursor: SnowflakeConnection or SnowflakeCursor to use for execution
        sqls: An iterable of SQL statements to execute, optionally with associated items
        error_handler: Optional function to handle errors, takes (Exception, sql) as arguments (
        cacheable: Whether to cache results (default: False)
        empty_response_codes: List of error codes that should return empty results (default: None)
        max_workers: Maximum number of worker threads (default: 8)

    Yields:
        Tuples of (original_sql, result) as they complete

    Example:
        >>> sql_statements = [
        ...     "SELECT COUNT(*) FROM table1",
        ...     "SELECT COUNT(*) FROM table2",
        ...     "SELECT COUNT(*) FROM table3",
        ... ]
        >>> for sql, result in execute_in_parallel(connection, sql_statements):
        ...     print(f"SQL: {sql} produced result: {result}")
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a mapping of futures to their original items
        future_to_item = {
            executor.submit(execute, conn_or_cursor, sql, cacheable, empty_response_codes): (sql, item)
            for sql, item in sqls
        }

        # Yield results as they complete
        for future in as_completed(future_to_item):
            sql, item = future_to_item[future]
            try:
                result = future.result()
                yield item, result
            except Exception as e:
                if error_handler:
                    error_handler(e, sql)
                else:
                    logger.error(f"Error processing SQL {sql}: {e}")
                    raise e
