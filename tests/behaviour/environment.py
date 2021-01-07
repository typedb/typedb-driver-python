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
