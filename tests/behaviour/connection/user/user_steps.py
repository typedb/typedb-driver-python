#
#   Copyright (C) 2021 Vaticle
#
#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing,
#   software distributed under the License is distributed on an
#   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#   KIND, either express or implied.  See the License for the
#   specific language governing permissions and limitations
#   under the License.
#
import os

from behave import step
from hamcrest import assert_that, has_item, not_

from tests.behaviour.context import Context
from typedb.client import *


def _get_client(context: Context):
    client = context.client
    assert isinstance(client, TypeDBClusterClient)
    return client


@step("users contains: {username}")
def step_impl(context: Context, username: str):
    assert_that([u.username() for u in _get_client(context).users().all()], has_item(username))


@step("users not contains: {username}")
def step_impl(context: Context, username: str):
    assert_that([u.username() for u in _get_client(context).users().all()], not_(has_item(username)))


@step("users create: {username}, {password}")
def step_impl(context: Context, username: str, password: str):
    _get_client(context).users().create(username, password)


@step("user password: {username}, {password}")
def step_impl(context: Context, username: str, password: str):
    _get_client(context).users().get(username).password(password)


@step("user connect: {username}, {password}")
def step_impl(context: Context, username: str, password: str):
    root_ca_path = os.environ["ROOT_CA"]
    credential = TypeDBCredential(username, password, root_ca_path)
    with TypeDB.cluster_client(addresses=["127.0.0.1:" + context.config.userdata["port"]], credential=credential) as client:
        client.databases().all()


@step("user delete: {username}")
def step_impl(context: Context, username: str):
    user = _get_client(context).users().get(username)
    user.delete()
