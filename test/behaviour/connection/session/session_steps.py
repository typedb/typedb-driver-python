from behave import *

from grakn.rpc.session import SessionType


@step("connection open data session for database: {database_name}")
@step("connection open session for database: {database_name}")
def step_impl(context, database_name: str):
    context.sessions.append(context.client.session(database_name, SessionType.DATA))
