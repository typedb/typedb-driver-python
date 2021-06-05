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
from behave import step
from hamcrest import assert_that, has_item, not_

from tests.behaviour.context import Context
from typedb.api.connection.client import TypeDBClusterClient


@step("users contains: {name}")
def step_impl(context: Context, name: str):
    client = context.client
    assert isinstance(client, TypeDBClusterClient)
    assert_that([u.name() for u in client.users().all()], has_item(name))

@step("users not contains: {name}")
def step_impl(context: Context, name: str):
    client = context.client
    assert isinstance(client, TypeDBClusterClient)
    assert_that([u.name() for u in client.users().all()], not_(has_item(name)))

@step("users create: {name}, {password}")
def step_impl(context: Context, name: str, password: str):
    client = context.client
    assert isinstance(client, TypeDBClusterClient)
    client.users().create(name, password)

@step("user delete: {name}")
def step_impl(context: Context, name: str):
    client = context.client
    assert isinstance(client, TypeDBClusterClient)
    user = client.users().get(name)
    user.delete()


