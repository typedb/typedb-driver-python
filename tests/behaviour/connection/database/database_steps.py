from behave import *


@step("connection create database: {database_name}")
def step_impl(context, database_name: str):
    context.client.databases().create(database_name)
