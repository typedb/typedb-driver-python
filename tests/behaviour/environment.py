from behave import *

from grakn.client import GraknClient


def before_all(context):
    context.THREAD_POOL_SIZE = 32
    context.client = GraknClient()


def before_scenario(context, scenario):
    for database in context.client.databases().all():
        context.client.databases().delete(database)
    context.sessions = []
    context.sessions_to_transactions = {}
    context.sessions_parallel = []
    context.sessions_to_transactions_parallel = {}
    context.sessions_parallel_to_transactions_parallel = {}
    context.tx = lambda: context.sessions_to_transactions[context.sessions[0]][0]


def after_scenario(context, scenario):
    for session in context.sessions:
        session.close()
    for database in context.client.databases().all():
        context.client.databases().delete(database)


def after_all(context):
    context.client.close()
