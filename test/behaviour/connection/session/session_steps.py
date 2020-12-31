from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

from behave import *

from grakn.rpc.session import SessionType


def open_sessions_for_databases(context, names: list, session_type=SessionType.DATA):
    for name in names:
        context.sessions.append(context.client.session(name, session_type))


@step("connection open schema session for database: {database_name}")
def step_impl(context, database_name):
    open_sessions_for_databases(context, [database_name], SessionType.SCHEMA)


@step("connection open data session for database: {database_name}")
@step("connection open session for database: {database_name}")
def step_impl(context, database_name: str):
    open_sessions_for_databases(context, [database_name], SessionType.DATA)


@step("connection open schema session for database")
@step("connection open schema session for databases")
@step("connection open schema sessions for database")
@step("connection open schema sessions for databases")
def step_impl(context):
    names = [context.table.headings[0]] + list(map(lambda row: row[0], context.table.rows))
    open_sessions_for_databases(context, names, SessionType.SCHEMA)


@step("connection open data session for database")
@step("connection open data session for databases")
@step("connection open data sessions for database")
@step("connection open data sessions for databases")
@step("connection open session for database")
@step("connection open session for databases")
@step("connection open sessions for database")
@step("connection open sessions for databases")
def step_impl(context):
    names = [context.table.headings[0]] + list(map(lambda row: row[0], context.table.rows))
    open_sessions_for_databases(context, names, SessionType.DATA)


@step("connection open data sessions in parallel for databases")
@step("connection open sessions in parallel for databases")
def step_impl(context):
    names = [context.table.headings[0]] + list(map(lambda row: row[0], context.table.rows))
    assert context.THREAD_POOL_SIZE >= len(names)
    with ThreadPoolExecutor(max_workers=context.THREAD_POOL_SIZE) as executor:
        for name in names:
            context.sessions_parallel.append(executor.submit(partial(context.client.session, name, SessionType.DATA)))
