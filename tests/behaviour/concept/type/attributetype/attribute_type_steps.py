#
# Copyright (C) 2022 Vaticle
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

from typedb.client import *
from tests.behaviour.config.parameters import parse_value_type, parse_list, parse_label
from tests.behaviour.context import Context


@step("put attribute type: {type_label}, with value type: {value_type}")
def step_impl(context: Context, type_label: str, value_type: str):
    context.tx().concepts().put_attribute_type(type_label, parse_value_type(value_type))


@step("attribute({type_label}) get value type: {value_type}")
def step_impl(context: Context, type_label: str, value_type: str):
    assert_that(context.tx().concepts().get_attribute_type(type_label).get_value_type(),
                is_(parse_value_type(value_type)))


@step("attribute({type_label}) get supertype value type: {value_type}")
def step_impl(context: Context, type_label: str, value_type: str):
    supertype = context.tx().concepts().get_attribute_type(type_label).as_remote(
        context.tx()).get_supertype().as_attribute_type()
    assert_that(supertype.get_value_type(), is_(parse_value_type(value_type)))


def attribute_type_as_value_type(context: Context, type_label: str, value_type: AttributeType.ValueType):
    attribute_type = context.tx().concepts().get_attribute_type(type_label)
    if value_type is AttributeType.ValueType.OBJECT:
        return attribute_type
    elif value_type is AttributeType.ValueType.BOOLEAN:
        return attribute_type.as_boolean()
    elif value_type is AttributeType.ValueType.LONG:
        return attribute_type.as_long()
    elif value_type is AttributeType.ValueType.DOUBLE:
        return attribute_type.as_double()
    elif value_type is AttributeType.ValueType.STRING:
        return attribute_type.as_string()
    elif value_type is AttributeType.ValueType.DATETIME:
        return attribute_type.as_datetime()
    else:
        raise ValueError("Unrecognised value type: " + str(value_type))


@step("attribute({type_label}) as({value_type}) get subtypes contain")
def step_impl(context: Context, type_label: str, value_type: str):
    sub_labels = [parse_label(s) for s in parse_list(context.table)]
    attribute_type = attribute_type_as_value_type(context, type_label, parse_value_type(value_type))
    actuals = list(map(lambda tt: tt.get_label(), attribute_type.as_remote(context.tx()).get_subtypes()))
    for sub_label in sub_labels:
        assert_that(sub_label, is_in(actuals))


@step("attribute({type_label}) as({value_type}) get subtypes do not contain")
def step_impl(context: Context, type_label: str, value_type: str):
    sub_labels = [parse_label(s) for s in parse_list(context.table)]
    attribute_type = attribute_type_as_value_type(context, type_label, parse_value_type(value_type))
    actuals = list(map(lambda tt: tt.get_label(), attribute_type.as_remote(context.tx()).get_subtypes()))
    for sub_label in sub_labels:
        assert_that(sub_label, not_(is_in(actuals)))


@step("attribute({type_label}) as({value_type}) set regex: {regex}")
def step_impl(context: Context, type_label: str, value_type, regex: str):
    value_type = parse_value_type(value_type)
    assert_that(value_type, is_(AttributeType.ValueType.STRING))
    attribute_type = attribute_type_as_value_type(context, type_label, value_type)
    attribute_type.as_remote(context.tx()).set_regex(regex)


@step("attribute({type_label}) as({value_type}) unset regex")
def step_impl(context: Context, type_label: str, value_type):
    value_type = parse_value_type(value_type)
    assert_that(value_type, is_(AttributeType.ValueType.STRING))
    attribute_type = attribute_type_as_value_type(context, type_label, value_type)
    attribute_type.as_remote(context.tx()).set_regex(None)


@step("attribute({type_label}) as({value_type}) get regex: {regex}")
def step_impl(context: Context, type_label: str, value_type, regex: str):
    value_type = parse_value_type(value_type)
    assert_that(value_type, is_(AttributeType.ValueType.STRING))
    attribute_type = attribute_type_as_value_type(context, type_label, value_type)
    assert_that(attribute_type.as_remote(context.tx()).get_regex(), is_(regex))


@step("attribute({type_label}) as({value_type}) does not have any regex")
def step_impl(context: Context, type_label: str, value_type):
    value_type = parse_value_type(value_type)
    assert_that(value_type, is_(AttributeType.ValueType.STRING))
    attribute_type = attribute_type_as_value_type(context, type_label, value_type)
    assert_that(attribute_type.as_remote(context.tx()).get_regex(), is_(None))


@step("attribute({type_label}) get key owners contain")
def step_impl(context: Context, type_label: str):
    owner_labels = [parse_label(s) for s in parse_list(context.table)]
    attribute_type = context.tx().concepts().get_attribute_type(type_label)
    actuals = list(map(lambda tt: tt.get_label(), attribute_type.as_remote(context.tx()).get_owners(only_key=True)))
    for owner_label in owner_labels:
        assert_that(actuals, has_item(owner_label))


@step("attribute({type_label}) get key owners do not contain")
def step_impl(context: Context, type_label: str):
    owner_labels = [parse_label(s) for s in parse_list(context.table)]
    attribute_type = context.tx().concepts().get_attribute_type(type_label)
    actuals = list(map(lambda tt: tt.get_label(), attribute_type.as_remote(context.tx()).get_owners(only_key=True)))
    for owner_label in owner_labels:
        assert_that(actuals, not_(has_item(owner_label)))


@step("attribute({type_label}) get key owners explicit contain")
def step_impl(context: Context, type_label: str):
    owner_labels = [parse_label(s) for s in parse_list(context.table)]
    attribute_type = context.tx().concepts().get_attribute_type(type_label)
    actuals = list(
        map(lambda tt: tt.get_label(), attribute_type.as_remote(context.tx()).get_owners_explicit(only_key=True)))
    for owner_label in owner_labels:
        assert_that(actuals, has_item(owner_label))


@step("attribute({type_label}) get key owners explicit do not contain")
def step_impl(context: Context, type_label: str):
    owner_labels = [parse_label(s) for s in parse_list(context.table)]
    attribute_type = context.tx().concepts().get_attribute_type(type_label)
    actuals = list(
        map(lambda tt: tt.get_label(), attribute_type.as_remote(context.tx()).get_owners_explicit(only_key=True)))
    for owner_label in owner_labels:
        assert_that(actuals, not_(has_item(owner_label)))


@step("attribute({type_label}) get attribute owners contain")
def step_impl(context: Context, type_label: str):
    owner_labels = [parse_label(s) for s in parse_list(context.table)]
    attribute_type = context.tx().concepts().get_attribute_type(type_label)
    actuals = list(map(lambda tt: tt.get_label(), attribute_type.as_remote(context.tx()).get_owners(only_key=False)))
    for owner_label in owner_labels:
        assert_that(actuals, has_item(owner_label))


@step("attribute({type_label}) get attribute owners do not contain")
def step_impl(context: Context, type_label: str):
    owner_labels = [parse_label(s) for s in parse_list(context.table)]
    attribute_type = context.tx().concepts().get_attribute_type(type_label)
    actuals = list(map(lambda tt: tt.get_label(), attribute_type.as_remote(context.tx()).get_owners(only_key=False)))
    for owner_label in owner_labels:
        assert_that(actuals, not_(has_item(owner_label)))


@step("attribute({type_label}) get attribute owners explicit contain")
def step_impl(context: Context, type_label: str):
    owner_labels = [parse_label(s) for s in parse_list(context.table)]
    attribute_type = context.tx().concepts().get_attribute_type(type_label)
    actuals = list(map(lambda tt: tt.get_label(), attribute_type.as_remote(context.tx()).get_owners_explicit(only_key=False)))
    for owner_label in owner_labels:
        assert_that(actuals, has_item(owner_label))


@step("attribute({type_label}) get attribute owners explicit do not contain")
def step_impl(context: Context, type_label: str):
    owner_labels = [parse_label(s) for s in parse_list(context.table)]
    attribute_type = context.tx().concepts().get_attribute_type(type_label)
    actuals = list(map(lambda tt: tt.get_label(), attribute_type.as_remote(context.tx()).get_owners_explicit(only_key=False)))
    for owner_label in owner_labels:
        assert_that(actuals, not_(has_item(owner_label)))
