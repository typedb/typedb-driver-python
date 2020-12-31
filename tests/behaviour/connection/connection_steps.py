from behave import *


@step("connection has been opened")
def step_impl(context):
    assert context.client and context.client.is_open()


@step("connection does not have any database")
def step_impl(context):
    assert len(context.client.databases().all()) == 0
