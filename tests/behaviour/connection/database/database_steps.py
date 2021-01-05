from typing import List

from behave import *

from tests.behaviour.config.parameters import parse_list


def create_databases(context, names: List[str]):
    for name in names:
        context.client.databases().create(name)


@step("connection create database: {database_name}")
def step_impl(context, database_name: str):
    create_databases(context, [database_name])


# TODO: connection create database(s) in other implementations, simplify
@step("connection create databases")
def step_impl(context):
    names = parse_list(context.table)
    create_databases(context, names)
