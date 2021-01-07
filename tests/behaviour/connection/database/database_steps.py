#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
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
