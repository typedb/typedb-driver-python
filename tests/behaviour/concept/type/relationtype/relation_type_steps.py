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
from hamcrest import *

from grakn.common.exception import GraknClientException
from grakn.concept.type.value_type import ValueType
from tests.behaviour.config.parameters import parse_value_type, parse_list, parse_bool
from tests.behaviour.context import Context


@step("relation({relation_label}) set relates role: {role_label}; throws exception")
def step_impl(context: Context, relation_label: str, role_label: str):
    try:
        context.tx().concepts().get_relation_type(relation_label).as_remote(context.tx()).set_relates(role_label)
        assert False
    except GraknClientException:
        pass


@step("relation({relation_label}) set relates role: {role_label}")
def step_impl(context: Context, relation_label: str, role_label: str):
    context.tx().concepts().get_relation_type(relation_label).as_remote(context.tx()).set_relates(role_label)


@step("relation({relation_label}) unset related role: {role_label}; throws exception")
def step_impl(context: Context, relation_label: str, role_label: str):
    try:
        context.tx().concepts().get_relation_type(relation_label).as_remote(context.tx()).unset_relates(role_label)
        assert False
    except GraknClientException:
        pass


@step("relation({relation_label}) unset related role: {role_label}")
def step_impl(context: Context, relation_label: str, role_label: str):
    context.tx().concepts().get_relation_type(relation_label).as_remote(context.tx()).unset_relates(role_label)


@step("relation({relation_label}) set relates role: {role_label} as {super_role}; throws exception")
def step_impl(context: Context, relation_label: str, role_label: str, super_role: str):
    try:
        context.tx().concepts().get_relation_type(relation_label).as_remote(context.tx()).set_relates(role_label, overridden_label=super_role)
        assert False
    except GraknClientException:
        pass


@step("relation({relation_label}) set relates role: {role_label} as {super_role}")
def step_impl(context: Context, relation_label: str, role_label: str, super_role: str):
    context.tx().concepts().get_relation_type(relation_label).as_remote(context.tx()).set_relates(role_label, overridden_label=super_role)


@step("relation({relation_label}) remove related role: {role_label}")
def step_impl(context: Context, relation_label: str, role_label: str):
    context.tx().concepts().get_relation_type(relation_label).as_remote(context.tx()).get_relates(role_label).as_remote(context.tx()).delete()


@step("relation({relation_label}) get role({role_label}) is null: {is_null}")
def step_impl(context: Context, relation_label: str, role_label: str, is_null):
    is_null = parse_bool(is_null)
    assert_that(context.tx().concepts().get_relation_type(relation_label).as_remote(context.tx()).get_relates(role_label) is None, is_(is_null))
