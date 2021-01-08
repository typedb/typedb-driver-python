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

from tests.behaviour.config.parameters import parse_value_type
from tests.behaviour.context import Context


@step("put attribute type: {type_label}, with value type: {value_type}")
def step_impl(context: Context, type_label: str, value_type: str):
    context.tx().concepts().put_attribute_type(type_label, parse_value_type(value_type))


@step("attribute({type_label}) get value type: {value_type}")
def step_impl(context: Context, type_label: str, value_type: str):
    assert_that(context.tx().concepts().get_attribute_type(type_label).get_value_type(), is_(parse_value_type(value_type)))


@step("attribute({type_label}) get supertype value type: {value_type}")
def step_impl(context: Context, type_label: str, value_type: str):
    supertype = context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).get_supertype()
    assert_that(supertype.get_value_type(), is_(parse_value_type(value_type)))
