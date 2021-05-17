#
# Copyright (C) 2021 Vaticle
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

from typedb.common.exception import TypeDBClientException
from typedb.common.label import Label
from tests.behaviour.config.parameters import parse_bool, parse_list, RootLabel, parse_label
from tests.behaviour.context import Context


@step("put {root_label:RootLabel} type: {type_label}")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    if root_label == RootLabel.ENTITY:
        context.tx().concepts().put_entity_type(type_label)
    elif root_label == RootLabel.RELATION:
        context.tx().concepts().put_relation_type(type_label)
    else:
        raise ValueError("Unrecognised value")


@step("delete {root_label:RootLabel} type: {type_label}; throws exception")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    try:
        context.get_thing_type(root_label, type_label).as_remote(context.tx()).delete()
        assert False
    except TypeDBClientException:
        pass


@step("delete {root_label:RootLabel} type: {type_label}")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    context.get_thing_type(root_label, type_label).as_remote(context.tx()).delete()


@step("{root_label:RootLabel}({type_label}) is null: {is_null}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, is_null):
    is_null = parse_bool(is_null)
    assert_that(context.get_thing_type(root_label, type_label) is None, is_(is_null))


@step("{root_label:RootLabel}({type_label}) set label: {new_label}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, new_label: str):
    context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_label(new_label)


@step("{root_label:RootLabel}({type_label}) get label: {get_label}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, get_label: str):
    assert_that(context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_label().name(), is_(get_label))


@step("{root_label:RootLabel}({type_label}) set abstract: {is_abstract}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, is_abstract):
    is_abstract = parse_bool(is_abstract)
    thing_type = context.get_thing_type(root_label, type_label)
    if is_abstract:
        thing_type.as_remote(context.tx()).set_abstract()
    else:
        thing_type.as_remote(context.tx()).unset_abstract()


@step("{root_label:RootLabel}({type_label}) is abstract: {is_abstract}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, is_abstract):
    is_abstract = parse_bool(is_abstract)
    assert_that(context.get_thing_type(root_label, type_label).as_remote(context.tx()).is_abstract(), is_(is_abstract))


@step("{root_label:RootLabel}({type_label}) set supertype: {super_label}; throws exception")
def step_impl(context: Context, root_label: RootLabel, type_label: str, super_label: str):
    if root_label == RootLabel.ENTITY:
        entity_supertype = context.tx().concepts().get_entity_type(super_label)
        try:
            context.tx().concepts().get_entity_type(type_label).as_remote(context.tx()).set_supertype(entity_supertype)
            assert False
        except TypeDBClientException:
            pass
    elif root_label == RootLabel.ATTRIBUTE:
        attribute_supertype = context.tx().concepts().get_attribute_type(super_label)
        try:
            context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).set_supertype(attribute_supertype)
            assert False
        except TypeDBClientException:
            pass
    elif root_label == RootLabel.RELATION:
        relation_supertype = context.tx().concepts().get_relation_type(super_label)
        try:
            context.tx().concepts().get_relation_type(type_label).as_remote(context.tx()).set_supertype(relation_supertype)
            assert False
        except TypeDBClientException:
            pass
    else:
        raise ValueError("Unrecognised value")


@step("{root_label:RootLabel}({type_label}) set supertype: {super_label}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, super_label: str):
    if root_label == RootLabel.ENTITY:
        entity_supertype = context.tx().concepts().get_entity_type(super_label)
        context.tx().concepts().get_entity_type(type_label).as_remote(context.tx()).set_supertype(entity_supertype)
    elif root_label == RootLabel.ATTRIBUTE:
        attribute_supertype = context.tx().concepts().get_attribute_type(super_label)
        context.tx().concepts().get_attribute_type(type_label).as_remote(context.tx()).set_supertype(attribute_supertype)
    elif root_label == RootLabel.RELATION:
        relation_supertype = context.tx().concepts().get_relation_type(super_label)
        context.tx().concepts().get_relation_type(type_label).as_remote(context.tx()).set_supertype(relation_supertype)
    else:
        raise ValueError("Unrecognised value")


@step("{root_label:RootLabel}({type_label}) get supertype: {super_label}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, super_label: str):
    supertype = context.get_thing_type(root_label, super_label)
    assert_that(context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_supertype(), is_(supertype))


@step("{root_label:RootLabel}({type_label}) get supertypes contain")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    super_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_supertypes()]
    for super_label in super_labels:
        assert_that(actuals, has_item(super_label))


@step("{root_label:RootLabel}({type_label}) get supertypes do not contain")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    super_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_supertypes()]
    for super_label in super_labels:
        assert_that(actuals, not_(has_item(super_label)))


@step("{root_label:RootLabel}({type_label}) get subtypes contain")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    sub_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_subtypes()]
    for sub_label in sub_labels:
        assert_that(actuals, has_item(sub_label))


@step("{root_label:RootLabel}({type_label}) get subtypes do not contain")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    sub_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_subtypes()]
    for sub_label in sub_labels:
        assert_that(actuals, not_(has_item(sub_label)))


@step("{root_label:RootLabel}({type_label}) set owns key type: {att_type_label} as {overridden_label}; throws exception")
def step_impl(context: Context, root_label: RootLabel, type_label: str, att_type_label: str, overridden_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    overridden_type = context.tx().concepts().get_attribute_type(overridden_label)
    try:
        context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, overridden_type, is_key=True)
        assert False
    except TypeDBClientException:
        pass


@step("{root_label:RootLabel}({type_label}) set owns key type: {att_type_label}; throws exception")
def step_impl(context: Context, root_label: RootLabel, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    try:
        context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, is_key=True)
        assert False
    except TypeDBClientException:
        pass


@step("{root_label:RootLabel}({type_label}) set owns key type: {att_type_label} as {overridden_label}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, att_type_label: str, overridden_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    overridden_type = context.tx().concepts().get_attribute_type(overridden_label)
    context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, overridden_type, is_key=True)


@step("{root_label:RootLabel}({type_label}) set owns key type: {att_type_label}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, is_key=True)


@step("{root_label:RootLabel}({type_label}) unset owns attribute type: {att_type_label}; throws exception")
@step("{root_label:RootLabel}({type_label}) unset owns key type: {att_type_label}; throws exception")
def step_impl(context: Context, root_label: RootLabel, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    try:
        context.get_thing_type(root_label, type_label).as_remote(context.tx()).unset_owns(attribute_type)
        assert False
    except TypeDBClientException:
        pass


@step("{root_label:RootLabel}({type_label}) unset owns attribute type: {att_type_label}")
@step("{root_label:RootLabel}({type_label}) unset owns key type: {att_type_label}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    context.get_thing_type(root_label, type_label).as_remote(context.tx()).unset_owns(attribute_type)


@step("{root_label:RootLabel}({type_label}) get owns key types contain")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    attribute_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_owns(keys_only=True)]
    for attribute_label in attribute_labels:
        assert_that(actuals, has_item(attribute_label))


@step("{root_label:RootLabel}({type_label}) get owns key types do not contain")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    attribute_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_owns(keys_only=True)]
    for attribute_label in attribute_labels:
        assert_that(actuals, not_(has_item(attribute_label)))


@step("{root_label:RootLabel}({type_label}) set owns attribute type: {att_type_label} as {overridden_label}; throws exception")
def step_impl(context: Context, root_label: RootLabel, type_label: str, att_type_label: str, overridden_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    overridden_type = context.tx().concepts().get_attribute_type(overridden_label)
    try:
        context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, overridden_type)
        assert False
    except TypeDBClientException:
        pass


@step("{root_label:RootLabel}({type_label}) set owns attribute type: {att_type_label}; throws exception")
def step_impl(context: Context, root_label: RootLabel, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    try:
        context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_owns(attribute_type)
        assert False
    except TypeDBClientException:
        pass


@step("{root_label:RootLabel}({type_label}) set owns attribute type: {att_type_label} as {overridden_label}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, att_type_label: str, overridden_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    overridden_type = context.tx().concepts().get_attribute_type(overridden_label)
    context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_owns(attribute_type, overridden_type)


@step("{root_label:RootLabel}({type_label}) set owns attribute type: {att_type_label}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, att_type_label: str):
    attribute_type = context.tx().concepts().get_attribute_type(att_type_label)
    context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_owns(attribute_type)


@step("{root_label:RootLabel}({type_label}) get owns attribute types contain")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    attribute_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_owns()]
    for attribute_label in attribute_labels:
        assert_that(actuals, has_item(attribute_label))


@step("{root_label:RootLabel}({type_label}) get owns attribute types do not contain")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    attribute_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_owns()]
    for attribute_label in attribute_labels:
        assert_that(actuals, not_(has_item(attribute_label)))


@step("{root_label:RootLabel}({type_label}) set plays role: {role_label:ScopedLabel} as {overridden_label:ScopedLabel}; throws exception")
def step_impl(context: Context, root_label: RootLabel, type_label: str, role_label: Label, overridden_label: Label):
    role_type = context.tx().concepts().get_relation_type(role_label.scope()).as_remote(context.tx()).get_relates(role_label.name())
    overridden_type = context.tx().concepts().get_relation_type(overridden_label.scope()).as_remote(context.tx()).get_relates(overridden_label.name())
    try:
        context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_plays(role_type, overridden_type)
        assert False
    except TypeDBClientException:
        pass


@step("{root_label:RootLabel}({type_label}) set plays role: {role_label:ScopedLabel} as {overridden_label:ScopedLabel}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, role_label: Label, overridden_label: Label):
    role_type = context.tx().concepts().get_relation_type(role_label.scope()).as_remote(context.tx()).get_relates(role_label.name())
    overridden_type = context.tx().concepts().get_relation_type(overridden_label.scope()).as_remote(context.tx()).get_relates(overridden_label.name())
    context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_plays(role_type, overridden_type)


@step("{root_label:RootLabel}({type_label}) set plays role: {role_label:ScopedLabel}; throws exception")
def step_impl(context: Context, root_label: RootLabel, type_label: str, role_label: Label):
    role_type = context.tx().concepts().get_relation_type(role_label.scope()).as_remote(context.tx()).get_relates(role_label.name())
    try:
        context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_plays(role_type)
        assert False
    except TypeDBClientException:
        pass


@step("{root_label:RootLabel}({type_label}) set plays role: {role_label:ScopedLabel}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, role_label: Label):
    role_type = context.tx().concepts().get_relation_type(role_label.scope()).as_remote(context.tx()).get_relates(role_label.name())
    context.get_thing_type(root_label, type_label).as_remote(context.tx()).set_plays(role_type)


@step("{root_label:RootLabel}({type_label}) unset plays role: {role_label:ScopedLabel}; throws exception")
def step_impl(context: Context, root_label: RootLabel, type_label: str, role_label: Label):
    role_type = context.tx().concepts().get_relation_type(role_label.scope()).as_remote(context.tx()).get_relates(role_label.name())
    try:
        context.get_thing_type(root_label, type_label).as_remote(context.tx()).unset_plays(role_type)
        assert False
    except TypeDBClientException:
        pass


@step("{root_label:RootLabel}({type_label}) unset plays role: {role_label:ScopedLabel}")
def step_impl(context: Context, root_label: RootLabel, type_label: str, role_label: Label):
    role_type = context.tx().concepts().get_relation_type(role_label.scope()).as_remote(context.tx()).get_relates(role_label.name())
    context.get_thing_type(root_label, type_label).as_remote(context.tx()).unset_plays(role_type)


@step("{root_label:RootLabel}({type_label}) get playing roles contain")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    role_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_plays()]
    for role_label in role_labels:
        assert_that(role_label, is_in(actuals))


@step("{root_label:RootLabel}({type_label}) get playing roles do not contain")
def step_impl(context: Context, root_label: RootLabel, type_label: str):
    role_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.get_thing_type(root_label, type_label).as_remote(context.tx()).get_plays()]
    for role_label in role_labels:
        assert_that(role_label, not_(is_in(actuals)))


@step("thing type root get supertypes contain")
def step_impl(context: Context):
    super_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.tx().concepts().get_root_thing_type().as_remote(context.tx()).get_supertypes()]
    for super_label in super_labels:
        assert_that(super_label, is_in(actuals))


@step("thing type root get supertypes do not contain")
def step_impl(context: Context):
    super_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.tx().concepts().get_root_thing_type().as_remote(context.tx()).get_supertypes()]
    for super_label in super_labels:
        assert_that(super_label, not_(is_in(actuals)))


@step("thing type root get subtypes contain")
def step_impl(context: Context):
    sub_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.tx().concepts().get_root_thing_type().as_remote(context.tx()).get_subtypes()]
    for sub_label in sub_labels:
        assert_that(sub_label, is_in(actuals))


@step("thing type root get subtypes do not contain")
def step_impl(context: Context):
    sub_labels = [parse_label(s) for s in parse_list(context.table)]
    actuals = [t.get_label() for t in context.tx().concepts().get_root_thing_type().as_remote(context.tx()).get_subtypes()]
    for sub_label in sub_labels:
        assert_that(sub_label, not_(is_in(actuals)))
