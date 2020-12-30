from behave import *

from grakn.rpc.session import SessionType
from grakn.rpc.transaction import TransactionType


@step("connection does not have any database")
def step_impl(context):
    assert len(context.client.databases().all()) == 0


@step("connection create database: {database_name}")
def step_impl(context, database_name: str):
    context.client.databases().create(database_name)


@step("connection open data session for database: {database_name}")
@step("connection open session for database: {database_name}")
def step_impl(context, database_name: str):
    context.sessions.append(context.client.session(database_name, SessionType.DATA))


@step("session opens transaction of type: {transaction_type}")
def step_impl(context, transaction_type):  # TODO transaction_type should parse to type TransactionType by itself ideally
    transaction_type = TransactionType.READ if transaction_type == "read" else "write"
    for session in context.sessions:
        transactions = []
        transactions.append(session.transaction(transaction_type))
        context.sessions_to_transactions[session] = transactions


@step("session transaction is open: {is_open}")
def step_impl(context, is_open):  # TODO is_open should parse to boolean by itself
    is_open = (is_open == "true")
    for session in context.sessions:
        for transaction in context.sessions_to_transactions[session]:
            assert transaction.is_open() == is_open
