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

from tests.behaviour.context import Context


# TODO
@step("the integrity is validated")
def step_impl(context: Context):
    pass


@step("graql define")
def step_impl(context: Context):
    context.tx().query().define(context.text)


@step("graql define; throws exception")
def step_impl(context: Context):
    try:
        next(context.tx().query().define(context.text))
        assert False
    except GraknClientException:
        pass


@step("graql define; throws exception containing \"{exception}\"")
def step_impl(context: Context, exception: str):
    try:
        next(context.tx().query().define(context.text))
        assert False
    except GraknClientException as e:
        assert_that(exception, is_in(str(e)))


@step("graql undefine")
def step_impl(context: Context):
    context.tx().query().undefine(context.text)


@step("graql undefine; throws exception")
def step_impl(context: Context):
    try:
        next(context.tx().query().undefine(context.text))
        assert False
    except GraknClientException:
        pass


@step("graql undefine; throws exception containing \"{exception}\"")
def step_impl(context: Context, exception: str):
    try:
        next(context.tx().query().undefine(context.text))
        assert False
    except GraknClientException as e:
        assert_that(exception, is_in(str(e)))


@step("graql insert")
def step_impl(context: Context):
    context.tx().query().insert(context.text)


@step("graql insert; throws exception")
def step_impl(context: Context):
    try:
        next(context.tx().query().insert(context.text))
        assert False
    except GraknClientException:
        pass


@step("graql insert; throws exception containing \"{exception}\"")
def step_impl(context: Context, exception: str):
    try:
        next(context.tx().query().insert(context.text))
        assert False
    except GraknClientException as e:
        assert_that(exception, is_in(str(e)))


@step("graql delete")
def step_impl(context: Context):
    context.tx().query().delete(context.text)


@step("graql delete; throws exception")
def step_impl(context: Context):
    try:
        next(context.tx().query().delete(context.text))
        assert False
    except GraknClientException:
        pass


@step("graql delete; throws exception containing \"{exception}\"")
def step_impl(context: Context, exception: str):
    try:
        next(context.tx().query().delete(context.text))
        assert False
    except GraknClientException as e:
        assert_that(exception, is_in(str(e)))
