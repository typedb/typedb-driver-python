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
import re
from collections import defaultdict
from typing import Dict, List, Tuple, Union, Optional

from behave import *
from hamcrest import *

from grakn.common.exception import GraknClientException
from grakn.concept.answer.concept_map import ConceptMap
from grakn.concept.answer.numeric import Numeric
from tests.behaviour.config.parameters import parse_bool, parse_int, parse_float, parse_datetime, parse_table
from tests.behaviour.context import Context, ConceptSubtype, AttributeSubtype


@step("the integrity is validated")
def step_impl(context: Context):
    # TODO
    pass


@step("graql define")
def step_impl(context: Context):
    context.tx().query().define(query=context.text)


@step("graql define; throws exception")
def step_impl(context: Context):
    assert_that(calling(next).with_args(context.tx().query().define(query=context.text)), raises(GraknClientException))


@step("graql define; throws exception containing \"{exception}\"")
def step_impl(context: Context, exception: str):
    assert_that(calling(next).with_args(context.tx().query().define(query=context.text)), raises(GraknClientException, exception))


@step("graql undefine")
def step_impl(context: Context):
    context.tx().query().undefine(query=context.text)


@step("graql undefine; throws exception")
def step_impl(context: Context):
    assert_that(calling(next).with_args(context.tx().query().undefine(query=context.text)), raises(GraknClientException))


@step("graql undefine; throws exception containing \"{exception}\"")
def step_impl(context: Context, exception: str):
    assert_that(calling(next).with_args(context.tx().query().undefine(query=context.text)), raises(GraknClientException, exception))


@step("graql insert")
def step_impl(context: Context):
    context.tx().query().insert(query=context.text)


@step("graql insert; throws exception")
def step_impl(context: Context):
    assert_that(calling(next).with_args(context.tx().query().insert(query=context.text)), raises(GraknClientException))


@step("graql insert; throws exception containing \"{exception}\"")
def step_impl(context: Context, exception: str):
    assert_that(calling(next).with_args(context.tx().query().insert(query=context.text)), raises(GraknClientException, exception))


@step("graql delete")
def step_impl(context: Context):
    context.tx().query().delete(query=context.text)


@step("graql delete; throws exception")
def step_impl(context: Context):
    assert_that(calling(next).with_args(context.tx().query().delete(query=context.text)), raises(GraknClientException))


@step("graql delete; throws exception containing \"{exception}\"")
def step_impl(context: Context, exception: str):
    assert_that(calling(next).with_args(context.tx().query().insert(query=context.text)), raises(GraknClientException, exception))


@step("graql update")
def step_impl(context: Context):
    context.tx().query().update(query=context.text)


@step("graql update; throws exception")
def step_impl(context: Context):
    assert_that(calling(next).with_args(context.tx().query().update(query=context.text)), raises(GraknClientException))


@step("graql update; throws exception containing \"{exception}\"")
def step_impl(context: Context, exception: str):
    assert_that(calling(next).with_args(context.tx().query().update(query=context.text)), raises(GraknClientException, exception))

@step("get answers of graql insert")
def step_impl(context: Context):
    context.clear_answers()
    context.answers = [answer for answer in context.tx().query().insert(query=context.text)]


@step("get answers of graql match")
def step_impl(context: Context):
    context.clear_answers()
    context.answers = [answer for answer in context.tx().query().match(query=context.text)]


@step("graql match; throws exception")
def step_impl(context: Context):
    assert_that(calling(next).with_args(context.tx().query().match(query=context.text)), raises(GraknClientException))


@step("get answer of graql match aggregate")
def step_impl(context: Context):
    context.clear_answers()
    context.numeric_answer = next(context.tx().query().match_aggregate(query=context.text))


@step("get answers of graql match group")
def step_impl(context: Context):
    context.clear_answers()
    context.answer_groups = [group for group in context.tx().query().match_group(query=context.text)]


@step("get answers of graql match group aggregate")
def step_impl(context: Context):
    context.clear_answers()
    context.numeric_answer_groups = [group for group in context.tx().query().match_group_aggregate(query=context.text)]


@step("answer size is: {expected_size:Int}")
def step_impl(context: Context, expected_size: int):
    assert_that(context.answers, has_length(expected_size), "Expected [%d] answers, but got [%d]" % (expected_size, len(context.answers)))


@step("rules contain: {rule_label:Str}")
def step_impl(context: Context, rule_label: str):
    return rule_label in [rule.get_label() for rule in context.tx().logic().get_rules()]

@step("rules do not contain: {rule_label:Str}")
def step_impl(context: Context, rule_label: str):
    return not (rule_label in [rule.get_label() for rule in context.tx().logic().get_rules()])


class ConceptMatcher:

    def matches(self, context: Context, concept: ConceptSubtype):
        return False


class TypeLabelMatcher(ConceptMatcher):

    def __init__(self, label: str):
        self.label = label

    def matches(self, context: Context, concept: ConceptSubtype):
        if concept.is_role_type():
            return self.label == concept.get_scoped_label()
        elif concept.is_type():
            return self.label == concept.get_label()
        else:
            raise TypeError("A Concept was matched by label, but it is not a Type.")


class AttributeMatcher(ConceptMatcher):

    def __init__(self, type_and_value: str):
        s = type_and_value.split(":")
        assert_that(s, has_length(2), "[%s] is not a valid attribute identifier. It should have format \"type_label:value\"." % type_and_value)
        self.type_label, self.value = s

    def check(self, attribute: AttributeSubtype):
        if attribute.is_boolean():
            return attribute.get_value() == parse_bool(self.value)
        elif attribute.is_long():
            return attribute.get_value() == parse_int(self.value)
        elif attribute.is_double():
            return attribute.get_value() == parse_float(self.value)
        elif attribute.is_string():
            return attribute.get_value() == self.value
        elif attribute.is_datetime():
            return attribute.get_value() == parse_datetime(self.value)
        else:
            raise ValueError("Unrecognised value type " + str(type(attribute)))


class AttributeValueMatcher(AttributeMatcher):

    def matches(self, context: Context, concept: ConceptSubtype):
        if not concept.is_attribute():
            return False

        attribute = concept

        if self.type_label != attribute.as_remote(context.tx()).get_type().get_label():
            return False

        return self.check(attribute)


class ThingKeyMatcher(AttributeMatcher):

    def matches(self, context: Context, concept: ConceptSubtype):
        if not concept.is_thing():
            return False

        keys = [key for key in concept.as_remote(context.tx()).get_has(only_key=True)]

        for key in keys:
            if key.as_remote(context.tx()).get_type().get_label() == self.type_label:
                return self.check(key)

        return False


def parse_concept_identifier(value: str):
    identifier_type, identifier_body = value.split(":", 1)
    if identifier_type == "label":
        return TypeLabelMatcher(label=identifier_body)
    elif identifier_type == "key":
        return ThingKeyMatcher(type_and_value=identifier_body)
    elif identifier_type == "value":
        return AttributeValueMatcher(type_and_value=identifier_body)
    else:
        raise ValueError("Failed to parse concept identifier: " + value)


def answer_concepts_match(context: Context, answer_identifier: List[Tuple[str, str]], answer: ConceptMap) -> bool:
    for var, concept_identifier in answer_identifier:
        matcher = parse_concept_identifier(concept_identifier)
        if not matcher.matches(context, answer.get(var)):
            return False

    return True


@step("uniquely identify answer concepts")
def step_impl(context: Context):
    answer_identifiers = parse_table(context.table)
    assert_that(context.answers, has_length(len(answer_identifiers)),
                "The number of answers [%d] should match the number of answer identifiers [%d]." % (len(context.answers), len(answer_identifiers)))

    result_set = [(ai, []) for ai in answer_identifiers]
    for answer in context.answers:
        for answer_identifier, matched_answers in result_set:
            if answer_concepts_match(context, answer_identifier, answer):
                matched_answers.append(answer)

    for answer_identifier, answers in result_set:
        assert_that(answers, has_length(1), "Each answer identifier should match precisely 1 answer, but [%d] answers "
                                            "matched the identifier [%s]." % (len(answers), answer_identifier))


@step("order of answer concepts is")
def step_impl(context: Context):
    answer_identifiers = parse_table(context.table)
    assert_that(context.answers, has_length(len(answer_identifiers)),
                "The number of answers [%d] should match the number of answer identifiers [%d]." % (len(context.answers), len(answer_identifiers)))
    for i in range(len(context.answers)):
        answer = context.answers[i]
        answer_identifier = answer_identifiers[i]
        assert_that(answer_concepts_match(context, answer_identifier, answer),
                    reason="The answer at index [%d] does not match the identifier [%s]." % (i, answer_identifier))


def get_numeric_value(numeric: Numeric):
    if numeric.is_int():
        return numeric.as_int()
    elif numeric.is_float():
        return numeric.as_float()
    else:
        return None


def assert_numeric_value(numeric: Numeric, expected_answer: Union[int, float], reason: Optional[str] = None):
    if numeric.is_int():
        assert_that(numeric.as_int(), is_(expected_answer), reason)
    elif numeric.is_float():
        assert_that(numeric.as_float(), is_(close_to(expected_answer, delta=0.001)), reason)
    else:
        assert False


@step("aggregate value is: {expected_answer:Float}")
def step_impl(context: Context, expected_answer: float):
    assert_that(context.numeric_answer is not None, reason="The last query executed was not an aggregate query.")
    assert_numeric_value(context.numeric_answer, expected_answer)


@step("aggregate answer is not a number")
def step_impl(context: Context):
    assert_that(context.numeric_answer.is_nan())


class AnswerIdentifierGroup:

    GROUP_COLUMN_NAME = "owner"

    def __init__(self, raw_answer_identifiers: List[List[Tuple[str, str]]]):
        self.owner_identifier = next(entry[1] for entry in raw_answer_identifiers[0] if entry[0] == self.GROUP_COLUMN_NAME)
        self.answer_identifiers = [[(var, concept_identifier) for (var, concept_identifier) in raw_answer_identifier if var != self.GROUP_COLUMN_NAME]
                                   for raw_answer_identifier in raw_answer_identifiers]


@step("answer groups are")
def step_impl(context: Context):
    raw_answer_identifiers = parse_table(context.table)
    grouped_answer_identifiers = defaultdict(list)
    for raw_answer_identifier in raw_answer_identifiers:
        owner = next(entry[1] for entry in raw_answer_identifier if entry[0] == AnswerIdentifierGroup.GROUP_COLUMN_NAME)
        grouped_answer_identifiers[owner].append(raw_answer_identifier)
    answer_identifier_groups = [AnswerIdentifierGroup(raw_identifiers) for raw_identifiers in grouped_answer_identifiers.values()]

    assert_that(context.answer_groups, has_length(len(answer_identifier_groups)),
                "Expected [%d] answer groups, but found [%d]." % (len(answer_identifier_groups), len(context.answer_groups)))

    for answer_identifier_group in answer_identifier_groups:
        identifier = parse_concept_identifier(answer_identifier_group.owner_identifier)
        answer_group = next((group for group in context.answer_groups if identifier.matches(context, group.owner())), None)
        assert_that(answer_group is not None,
                    reason="The group identifier [%s] does not match any of the answer group owners." % answer_identifier_group.owner_identifier)

        result_set = [(ai, []) for ai in answer_identifier_group.answer_identifiers]
        for answer in answer_group.concept_maps():
            for (answer_identifier, matched_answers) in result_set:
                if answer_concepts_match(context, answer_identifier, answer):
                    matched_answers.append(answer)

        for (answer_identifier, answers) in result_set:
            assert_that(answers, has_length(1), "Each answer identifier should match precisely 1 answer, but [%d] answers "
                                                "matched the identifier [%s]." % (len(answers), answer_identifier))


@step("group aggregate values are")
def step_impl(context: Context):
    raw_answer_identifiers = parse_table(context.table)
    expectations = {}
    for raw_answer_identifier in raw_answer_identifiers:
        owner = next(entry[1] for entry in raw_answer_identifier if entry[0] == AnswerIdentifierGroup.GROUP_COLUMN_NAME)
        expected_answer = parse_float(next(entry[1] for entry in raw_answer_identifier if entry[0] == "value"))
        expectations[owner] = expected_answer

    assert_that(context.numeric_answer_groups, has_length(len(expectations)),
                reason="Expected [%d] answer groups, but found [%d]." % (len(expectations), len(context.numeric_answer_groups)))

    for (owner_identifier, expected_answer) in expectations.items():
        identifier = parse_concept_identifier(owner_identifier)
        numeric_group = next((group for group in context.numeric_answer_groups if identifier.matches(context, group.owner())), None)
        assert_that(numeric_group is not None,
                    reason="The group identifier [%s] does not match any of the answer group owners." % owner_identifier)

        actual_answer = get_numeric_value(numeric_group.numeric())
        assert_numeric_value(numeric_group.numeric(), expected_answer,
                             reason="Expected answer [%f] for group [%s], but got [%f]" % (expected_answer, owner_identifier, actual_answer))


def variable_from_template_placeholder(placeholder: str):
    if placeholder.endswith(".iid"):
        return placeholder.replace(".iid", "").replace("answer.", "")
    else:
        raise ValueError("Cannot replace template not based on IID.")


def apply_query_template(template: str, answer: ConceptMap):
    query = ""
    matches = re.finditer(r"<(.+?)>", template)
    i = 0
    for match in matches:
        required_variable = variable_from_template_placeholder(match.group(1))
        query += template[i:match.span()[0]]
        if required_variable in answer.map().keys():
            concept = answer.get(required_variable)
            if not concept.is_thing():
                raise TypeError("Cannot apply IID templating to Types")
            query += concept.get_iid()
        else:
            raise ValueError("No IID available for template placeholder: [%s]" % match.group())
        i = match.span()[1]
    query += template[i:]
    return query


# TODO: This step seems needlessly complex for what it's actually used for
@step("each answer satisfies")
def step_impl(context: Context):
    for answer in context.answers:
        query = apply_query_template(template=context.text, answer=answer)
        assert_that(list(context.tx().query().match(query)), has_length(1))
