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
from grakn.concept.thing.thing import Thing
from tests.behaviour.config.parameters import parse_bool, parse_list
from tests.behaviour.context import Context


def get_thing_type(context: Context, root_label: str, type_label: str):
    if root_label == "entity":
        return context.tx().concepts().get_entity_type(type_label)
    elif root_label == "attribute":
        return context.tx().concepts().get_attribute_type(type_label)
    elif root_label == "relation":
        return context.tx().concepts().get_relation_type(type_label)
    else:
        raise ValueError("Unrecognised value")


@step("thing type root get supertypes contain")
def step_impl(context: Context):
    super_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), context.tx().concepts().get_root_thing_type().as_remote(context.tx()).get_supertypes()))
    for super_label in super_labels:
        assert_that(super_label, is_in(actuals))


@step("thing type root get supertypes do not contain")
def step_impl(context: Context):
    super_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), context.tx().concepts().get_root_thing_type().as_remote(context.tx()).get_supertypes()))
    for super_label in super_labels:
        assert_that(super_label, not_(is_in(actuals)))


@step("thing type root get subtypes contain")
def step_impl(context: Context):
    super_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), context.tx().concepts().get_root_thing_type().as_remote(context.tx()).get_subtypes()))
    for super_label in super_labels:
        assert_that(super_label, is_in(actuals))


@step("thing type root get subtypes do not contain")
def step_impl(context: Context):
    super_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), context.tx().concepts().get_root_thing_type().as_remote(context.tx()).get_subtypes()))
    for super_label in super_labels:
        assert_that(super_label, not_(is_in(actuals)))


@step("put {root_label} type: {type_label}")
def step_impl(context: Context, root_label: str, type_label: str):
    if root_label == "entity":
        context.tx().concepts().put_entity_type(type_label)
    elif root_label == "relation":
        context.tx().concepts().put_relation_type(type_label)
    else:
        raise ValueError("Unrecognised value")


@step("delete {root_label} type: {type_label}; throws exception")
def step_impl(context: Context, root_label: str, type_label: str):
    try:
        get_thing_type(context, root_label, type_label).as_remote(context.tx()).delete()
        assert False
    except GraknClientException:
        pass


@step("delete {root_label} type: {type_label}")
def step_impl(context: Context, root_label: str, type_label: str):
    get_thing_type(context, root_label, type_label).as_remote(context.tx()).delete()


@step("{root_label}({type_label}) is null: {is_null}")
def step_impl(context: Context, root_label: str, type_label: str, is_null):
    is_null = parse_bool(is_null)
    assert_that(get_thing_type(context, root_label, type_label) is None, is_(is_null))


@step("{root_label}({type_label}) set label: {new_label}")
def step_impl(context: Context, root_label: str, type_label: str, new_label: str):
    get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_label(new_label)


@step("{root_label}({type_label}) get label: {get_label}")
def step_impl(context: Context, root_label: str, type_label: str, get_label: str):
    assert_that(get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_label(), is_(get_label))


@step("{root_label}({type_label}) set abstract: {is_abstract}")
def step_impl(context: Context, root_label: str, type_label: str, is_abstract):
    is_abstract = parse_bool(is_abstract)
    thing_type = get_thing_type(context, root_label, type_label)
    if is_abstract:
        thing_type.as_remote(context.tx()).set_abstract()
    else:
        thing_type.as_remote(context.tx()).unset_abstract()


@step("{root_label}({type_label}) is abstract: {is_abstract}")
def step_impl(context: Context, root_label: str, type_label: str, is_abstract):
    is_abstract = parse_bool(is_abstract)
    assert_that(get_thing_type(context, root_label, type_label).as_remote(context.tx()).is_abstract(), is_(is_abstract))


@step("{root_label}({type_label}) set supertype: {super_label}; throws exception")
def step_impl(context: Context, root_label: str, type_label: str, super_label: str):
    if root_label == "entity":
        entity_supertype = context.tx().concepts().get_entity_type(super_label)
        try:
            context.tx().concepts().get_entity_type(type_label).as_remote(context.tx()).set_supertype(entity_supertype)
            assert False
        except GraknClientException:
            pass
    elif root_label == "attribute":
        attribute_supertype = context.tx().concepts().get_attribute_type(super_label)
        try:
            context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).set_supertype(attribute_supertype)
            assert False
        except GraknClientException:
            pass
    elif root_label == "relation":
        relation_supertype = context.tx().concepts().get_relation_type(super_label)
        try:
            context.tx().concepts().get_relation_type(type_label).as_remote(context.tx()).set_supertype(relation_supertype)
            assert False
        except GraknClientException:
            pass
    else:
        raise ValueError("Unrecognised value")


@step("{root_label}({type_label}) set supertype: {super_label}")
def step_impl(context: Context, root_label: str, type_label: str, super_label: str):
    if root_label == "entity":
        entity_supertype = context.tx().concepts().get_entity_type(super_label)
        context.tx().concepts().get_entity_type(type_label).as_remote(context.tx()).set_supertype(entity_supertype)
    elif root_label == "attribute":
        attribute_supertype = context.tx().concepts().get_attribute_type(super_label)
        context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).set_supertype(attribute_supertype)
    elif root_label == "relation":
        relation_supertype = context.tx().concepts().get_relation_type(super_label)
        context.tx().concepts().get_relation_type(type_label).as_remote(context.tx()).set_supertype(relation_supertype)
    else:
        raise ValueError("Unrecognised value")


@step("{root_label}({type_label}) get supertype: {super_label}")
def step_impl(context: Context, root_label: str, type_label: str, super_label: str):
    supertype = get_thing_type(context, root_label, super_label)
    assert_that(get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_supertype(), is_(supertype))


@step("{root_label}({type_label}) get supertypes contain")
def step_impl(context: Context, root_label: str, type_label: str):
    super_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_supertypes()))
    for super_label in super_labels:
        assert_that(super_label, is_in(actuals))


@step("{root_label}({type_label}) get supertypes do not contain")
def step_impl(context: Context, root_label: str, type_label: str):
    super_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_supertypes()))
    for super_label in super_labels:
        assert_that(super_label, not_(is_in(actuals)))


@step("{root_label}({type_label}) get subtypes contain")
def step_impl(context: Context, root_label: str, type_label: str):
    sub_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_subtypes()))
    for sub_label in sub_labels:
        assert_that(sub_label, is_in(actuals))


@step("{root_label}({type_label}) get subtypes do not contain")
def step_impl(context: Context, root_label: str, type_label: str):
    sub_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_subtypes()))
    for sub_label in sub_labels:
        assert_that(sub_label, not_(is_in(actuals)))


@step("{root_label}({type_label}) set owns key type: {att_type_label} as {overridden_label}; throws exception")
def step_impl(context: Context, root_label: str, type_label: str, att_type_label: str, overridden_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    overridden_type = context.tx().concepts().get_attribute_type(overridden_label)
    try:
        get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, overridden_type, is_key=True)
        assert False
    except GraknClientException:
        pass


@step("{root_label}({type_label}) set owns key type: {att_type_label}; throws exception")
def step_impl(context: Context, root_label: str, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    try:
        get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, is_key=True)
        assert False
    except GraknClientException:
        pass


@step("{root_label}({type_label}) set owns key type: {att_type_label} as {overridden_label}")
def step_impl(context: Context, root_label: str, type_label: str, att_type_label: str, overridden_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    overridden_type = context.tx().concepts().get_attribute_type(overridden_label)
    get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, overridden_type, is_key=True)


@step("{root_label}({type_label}) set owns key type: {att_type_label}")
def step_impl(context: Context, root_label: str, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, is_key=True)


@step("{root_label}({type_label}) unset owns attribute type: {att_type_label}; throws exception")
@step("{root_label}({type_label}) unset owns key type: {att_type_label}; throws exception")
def step_impl(context: Context, root_label: str, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    try:
        get_thing_type(context, root_label, type_label).as_remote(context.tx()).unset_owns(attribute_type)
        assert False
    except GraknClientException:
        pass


@step("{root_label}({type_label}) unset owns attribute type: {att_type_label}")
@step("{root_label}({type_label}) unset owns key type: {att_type_label}")
def step_impl(context: Context, root_label: str, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    get_thing_type(context, root_label, type_label).as_remote(context.tx()).unset_owns(attribute_type)


@step("{root_label}({type_label}) get owns key types contain")
def step_impl(context: Context, root_label: str, type_label: str):
    attribute_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_owns(keys_only=True)))
    for attribute_label in attribute_labels:
        assert_that(attribute_label, is_in(actuals))


@step("{root_label}({type_label}) get owns key types do not contain")
def step_impl(context: Context, root_label: str, type_label: str):
    attribute_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_owns(keys_only=True)))
    for attribute_label in attribute_labels:
        assert_that(attribute_label, not_(is_in(actuals)))


@step("{root_label}({type_label}) set owns attribute type: {att_type_label} as {overridden_label}; throws exception")
def step_impl(context: Context, root_label: str, type_label: str, att_type_label: str, overridden_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    overridden_type = context.tx().concepts().get_attribute_type(overridden_label)
    try:
        get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, overridden_type)
        assert False
    except GraknClientException:
        pass


@step("{root_label}({type_label}) set owns attribute type: {att_type_label}; throws exception")
def step_impl(context: Context, root_label: str, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    try:
        get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_owns(attribute_type)
        assert False
    except GraknClientException:
        pass


@step("{root_label}({type_label}) set owns attribute type: {att_type_label} as {overridden_label}")
def step_impl(context: Context, root_label: str, type_label: str, att_type_label: str, overridden_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    overridden_type = context.tx().concepts().get_attribute_type(overridden_label)
    get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, overridden_type)


@step("{root_label}({type_label}) set owns attribute type: {att_type_label}")
def step_impl(context: Context, root_label: str, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_owns(attribute_type)


@step("{root_label}({type_label}) get owns attribute types contain")
def step_impl(context: Context, root_label: str, type_label: str):
    attribute_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_owns()))
    for attribute_label in attribute_labels:
        assert_that(attribute_label, is_in(actuals))


@step("{root_label}({type_label}) get owns attribute types do not contain")
def step_impl(context: Context, root_label: str, type_label: str):
    attribute_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_owns()))
    for attribute_label in attribute_labels:
        assert_that(attribute_label, not_(is_in(actuals)))


@step("{root_label}({type_label}) set plays role: {scope}:{role_label} as {overridden_scope}:{overridden_label}; throws exception")
def step_impl(context: Context, root_label: str, type_label: str, scope: str, role_label: str, overridden_scope: str, overridden_label: str):
    role_type = context.tx().concepts().get_relation_type(scope).as_remote(context.tx()).get_relates(role_label)
    overridden_type = context.tx().concepts().get_relation_type(overridden_scope).as_remote(context.tx()).get_relates(overridden_label)
    try:
        get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_plays(role_type, overridden_type)
        assert False
    except GraknClientException:
        pass


@step("{root_label}({type_label}) set plays role: {scope}:{role_label} as {overridden_scope}:{overridden_label}")
def step_impl(context: Context, root_label: str, type_label: str, scope: str, role_label: str, overridden_scope: str, overridden_label: str):
    role_type = context.tx().concepts().get_relation_type(scope).as_remote(context.tx()).get_relates(role_label)
    overridden_type = context.tx().concepts().get_relation_type(overridden_scope).as_remote(context.tx()).get_relates(overridden_label)
    get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_plays(role_type, overridden_type)


@step("{root_label}({type_label}) set plays role: {scope}:{role_label}; throws exception")
def step_impl(context: Context, root_label: str, type_label: str, scope: str, role_label: str):
    role_type = context.tx().concepts().get_relation_type(scope).as_remote(context.tx()).get_relates(role_label)
    try:
        get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_plays(role_type)
        assert False
    except GraknClientException:
        pass


@step("{root_label}({type_label}) set plays role: {scope}:{role_label}")
def step_impl(context: Context, root_label: str, type_label: str, scope: str, role_label: str):
    role_type = context.tx().concepts().get_relation_type(scope).as_remote(context.tx()).get_relates(role_label)
    get_thing_type(context, root_label, type_label).as_remote(context.tx()).set_plays(role_type)


@step("{root_label}({type_label}) unset plays role: {scope}:{role_label}; throws exception")
def step_impl(context: Context, root_label: str, type_label: str, scope: str, role_label: str):
    role_type = context.tx().concepts().get_relation_type(scope).as_remote(context.tx()).get_relates(role_label)
    try:
        get_thing_type(context, root_label, type_label).as_remote(context.tx()).unset_plays(role_type)
        assert False
    except GraknClientException:
        pass


@step("{root_label}({type_label}) unset plays role: {scope}:{role_label}")
def step_impl(context: Context, root_label: str, type_label: str, scope: str, role_label: str):
    role_type = context.tx().concepts().get_relation_type(scope).as_remote(context.tx()).get_relates(role_label)
    get_thing_type(context, root_label, type_label).as_remote(context.tx()).unset_plays(role_type)


@step("{root_label}({type_label}) get playing roles contain")
def step_impl(context: Context, root_label: str, type_label: str):
    role_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_plays()))
    for role_label in role_labels:
        assert_that(role_label, is_in(actuals))


@step("{root_label}({type_label}) get playing roles do not contain")
def step_impl(context: Context, root_label: str, type_label: str):
    role_labels = parse_list(context.table)
    actuals = list(map(lambda t: t.get_label(), get_thing_type(context, root_label, type_label).as_remote(context.tx()).get_plays()))
    for role_label in role_labels:
        assert_that(role_label, not_(is_in(actuals)))
