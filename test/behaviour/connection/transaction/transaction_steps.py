from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from typing import Callable

from behave import *

from grakn.common.exception import GraknClientException
from grakn.rpc.transaction import TransactionType, Transaction


def for_each_session_open_transaction_of_type(context, transaction_types: list):
    for session in context.sessions:
        transactions = []
        for transaction_type in transaction_types:
            transaction = session.transaction(transaction_type)
            transactions.append(transaction)
        context.sessions_to_transactions[session] = transactions


# TODO: this is implemented as open(s) in some clients - get rid of that, simplify them
@step("session opens transaction of type: {transaction_type}")
@step("for each session, open transaction of type: {transaction_type}")
def step_impl(context, transaction_type):  # TODO transaction_type should parse to type TransactionType by itself ideally
    transaction_type = TransactionType.READ if transaction_type == "read" else TransactionType.WRITE
    for_each_session_open_transaction_of_type(context, [transaction_type])


@step("for each session, open transaction of type")
@step("for each session, open transactions of type")
def step_impl(context):
    raw_values = [context.table.headings[0]] + list(map(lambda row: row[0], context.table.rows))
    transaction_types = list(map(lambda raw: TransactionType.READ if raw == "read" else TransactionType.WRITE, raw_values))
    for_each_session_open_transaction_of_type(context, transaction_types)


@step("for each session, open transaction of type; throws exception")
@step("for each session, open transaction(s) of type; throws exception")
def step_impl(context):
    for session in context.sessions:
        for raw_type in [context.table.headings[0]] + list(map(lambda row: row[0], context.table.rows)):
            transaction_type = TransactionType.READ if raw_type == "read" else TransactionType.WRITE
            try:
                session.transaction(transaction_type)
                assert False
            except GraknClientException:
                pass


def for_each_session_transactions_are(context, assertion: Callable[[Transaction], None]):
    for session in context.sessions:
        for transaction in context.sessions_to_transactions[session]:
            assertion(transaction)


def assert_transaction_null(transaction: Transaction, is_null: bool):
    assert (transaction is None) is is_null


@step("session transaction is null: {is_null}")
@step("for each session, transaction is null: {is_null}")
@step("for each session, transactions are null: {is_null}")
def step_impl(context, is_null):
    is_null = is_null == "true"
    for_each_session_transactions_are(context, lambda tx: assert_transaction_null(tx, is_null))


def assert_transaction_open(transaction: Transaction, is_open: bool):
    assert transaction.is_open() is is_open


@step("session transaction is open: {is_open}")
@step("for each session, transaction is open: {is_open}")
@step("for each session, transactions are open: {is_open}")
def step_impl(context, is_open):
    is_open = is_open == "true"
    for_each_session_transactions_are(context, lambda tx: assert_transaction_open(tx, is_open))


@step("session transaction commits")
@step("transaction commits")
def step_impl(context):
    context.sessions_to_transactions[context.sessions[0]][0].commit()


@step("session transaction commits; throws exception")
@step("transaction commits; throws exception")
def step_impl(context):
    try:
        context.sessions_to_transactions[context.sessions[0]][0].commit()
        assert False
    except GraknClientException:
        pass


@step("for each session, transaction commits")
@step("for each session, transactions commit")
def step_impl(context):
    for session in context.sessions:
        for transaction in context.sessions_to_transactions[session]:
            transaction.commit()


@step("for each session, transaction commits; throws exception")
@step("for each session, transactions commit; throws exception")
def step_impl(context):
    for session in context.sessions:
        for transaction in context.sessions_to_transactions[session]:
            try:
                transaction.commit()
                assert False
            except GraknClientException:
                pass


# TODO: close(s) in other implementations - simplify
@step("for each session, transaction closes")
def step_impl(context):
    for session in context.sessions:
        for transaction in context.sessions_to_transactions[session]:
            transaction.close()


def for_each_session_transaction_has_type(context, transaction_types: list):
    for session in context.sessions:
        transactions = context.sessions_to_transactions[session]
        assert len(transaction_types) == len(transactions)
        transactions_iterator = iter(transactions)
        for transaction_type in transaction_types:
            assert next(transactions_iterator).transaction_type() == transaction_type


# NOTE: behave ignores trailing colons in feature files
@step("for each session, transaction has type")
@step("for each session, transactions have type")
def step_impl(context):
    raw_values = [context.table.headings[0]] + list(map(lambda row: row[0], context.table.rows))
    transaction_types = list(map(lambda raw: TransactionType.READ if raw == "read" else TransactionType.WRITE, raw_values))
    for_each_session_transaction_has_type(context, transaction_types)


# TODO: this is overcomplicated in some clients (has/have, transaction(s))
@step("for each session, transaction has type: {transaction_type}")
@step("session transaction has type: {transaction_type}")
def step_impl(context, transaction_type):
    transaction_type = TransactionType.READ if transaction_type == "read" else TransactionType.WRITE
    for_each_session_transaction_has_type(context, [transaction_type])


##############################################
# sequential sessions, parallel transactions #
##############################################

# TODO: transaction(s) in other implementations - simplify
@step("for each session, open transactions in parallel of type")
def step_impl(context):
    raw_values = [context.table.headings[0]] + list(map(lambda row: row[0], context.table.rows))
    types = list(map(lambda raw: TransactionType.READ if raw == "read" else TransactionType.WRITE, raw_values))
    assert context.THREAD_POOL_SIZE >= len(types)
    with ThreadPoolExecutor(max_workers=context.THREAD_POOL_SIZE) as executor:
        for session in context.sessions:
            context.sessions_to_transactions_parallel[session] = []
            for type_ in types:
                context.sessions_to_transactions_parallel[session].append(executor.submit(partial(session.transaction, type_)))


def for_each_session_transactions_in_parallel_are(context, assertion: Callable[[Transaction], None]):
    for session in context.sessions:
        for future_transaction in context.sessions_to_transactions_parallel[session]:
            # TODO: Ideally we would concurrently await all Futures and join the assertions, but not sure this is easy
            assertion(future_transaction.result())


@step("for each session, transactions in parallel are null: {is_null}")
def step_impl(context, is_null):
    is_null = is_null == "true"
    for_each_session_transactions_in_parallel_are(context, lambda tx: assert_transaction_null(tx, is_null))


@step("for each session, transactions in parallel are open: {is_open}")
def step_impl(context, is_open):
    is_open = is_open == "true"
    for_each_session_transactions_in_parallel_are(context, lambda tx: assert_transaction_open(tx, is_open))


@step("for each session, transactions in parallel have type")
def step_impl(context):
    raw_values = [context.table.headings[0]] + list(map(lambda row: row[0], context.table.rows))
    types = list(map(lambda raw: TransactionType.READ if raw == "read" else TransactionType.WRITE, raw_values))
    for session in context.sessions:
        future_transactions = context.sessions_to_transactions_parallel[session]
        assert len(types) == len(future_transactions)
        transactions = []
        for future_tx in future_transactions:
            transactions.append(future_tx.result())
        print("types")
        print(types)
        print("transactions")
        print(list(map(lambda tx: tx.transaction_type(), transactions)))
        transactions_iter = iter(transactions)
        for type_ in types:
            assert next(transactions_iter).transaction_type() == type_


############################################
# parallel sessions, parallel transactions #
############################################

def for_each_session_in_parallel_transactions_in_parallel_are(context, assertion):
    for future_session in context.sessions_parallel:
        for future_transaction in context.sessions_parallel_to_transactions_parallel[future_session]:
            assertion(future_transaction.result())


@step("for each session in parallel, transactions in parallel are null: {is_null}")
def step_impl(context, is_null):
    is_null = is_null == "true"
    for_each_session_in_parallel_transactions_in_parallel_are(context, lambda tx: assert_transaction_null(tx, is_null))


@step("for each session in parallel, transactions in parallel are open: {is_open}")
def step_impl(context, is_open):
    is_open = is_open == "true"
    for_each_session_in_parallel_transactions_in_parallel_are(context, lambda tx: assert_transaction_open(tx, is_open))


######################################
# transaction behaviour with queries #
######################################

@step("for each transaction, define query; throws exception containing {expected_exception}")
def step_impl(context, expected_exception: str):
    for session in context.sessions:
        for transaction in context.sessions_to_transactions[session]:
            try:
                next(transaction.query().define(context.text), default=None)
                assert False
            except GraknClientException as e:
                assert expected_exception in str(e)
