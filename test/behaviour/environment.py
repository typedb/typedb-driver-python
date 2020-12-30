from behave import *

from grakn.client import GraknClient


def before_scenario(context, scenario):
    context.client = GraknClient()
    for database in context.client.databases().all():
        context.client.databases().delete(database)
    context.sessions = []
    context.sessions_to_transactions = {}


def after_scenario(context, scenario):
    for session in context.sessions:
        session.close()
    context.client.close()
