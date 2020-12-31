from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from typing import List

from behave import *

from grakn.rpc.session import SessionType
from tests.behaviour.config.parameters import parse_bool, parse_list


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


@step("connection close all sessions")
def step_impl(context):
    for session in context.sessions:
        session.close()
    context.sessions = []


@step("session is null: {is_null}")
@step("sessions are null: {is_null}")
def step_impl(context, is_null):
    is_null = parse_bool(is_null)
    for session in context.sessions:
        assert (session is None) == is_null


@step("session is open: {is_open}")
@step("sessions are open: {is_open}")
def step_impl(context, is_open):
    is_open = parse_bool(is_open)
    for session in context.sessions:
        assert session.is_open() == is_open


@step("sessions in parallel are null: {is_null}")
def step_impl(context, is_null):
    is_null = parse_bool(is_null)
    for future_session in context.sessions_parallel:
        assert (future_session.result() is None) == is_null


@step("sessions in parallel are open: {is_open}")
def step_impl(context, is_open):
    is_open = parse_bool(is_open)
    for future_session in context.sessions_parallel:
        assert future_session.result().is_open() == is_open


def sessions_have_databases(context, names: List[str]):
    assert len(names) == len(context.sessions)
    session_iter = iter(context.sessions)
    for name in names:
        assert name == next(session_iter).database()


@step("session has database: {database_name}")
@step("sessions have database: {database_name}")
def step_impl(context, database_name: str):
    sessions_have_databases(context, list(database_name))


@step("sessions in parallel have databases")
def step_impl(context):
    database_names = parse_list(context.table)
    assert len(database_names) == len(context.sessions_parallel)
    future_session_iter = iter(context.sessions_parallel)
    for name in database_names:
        assert name == next(future_session_iter).result().database()
