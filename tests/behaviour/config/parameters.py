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
from datetime import datetime
from enum import Enum
from typing import List, Dict, Tuple

import parse
from behave import register_type
from behave.model import Table

# TODO: We aren't consistently using typed parameters in step implementations - we should be.
from grakn.api.concept.type.attribute_type import AttributeType
from grakn.api.transaction import GraknTransaction
from grakn.common.label import Label


@parse.with_pattern(r"true|false")
def parse_bool(value: str) -> bool:
    return value == "true"


register_type(Bool=parse_bool)


def parse_int(text: str) -> int:
    return int(text)


register_type(Int=parse_int)


def parse_float(text: str) -> float:
    return float(text)


register_type(Float=parse_float)


@parse.with_pattern(r"\d\d\d\d-\d\d-\d\d(?: \d\d:\d\d:\d\d)?")
def parse_datetime(text: str) -> datetime:
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.strptime(text, "%Y-%m-%d")


register_type(DateTime=parse_datetime)


class RootLabel(Enum):
    ENTITY = 0,
    ATTRIBUTE = 1,
    RELATION = 2


@parse.with_pattern(r"entity|attribute|relation")
def parse_root_label(text: str) -> RootLabel:
    if text == "entity":
        return RootLabel.ENTITY
    elif text == "attribute":
        return RootLabel.ATTRIBUTE
    elif text == "relation":
        return RootLabel.RELATION
    else:
        raise ValueError("Unrecognised root label: " + text)


register_type(RootLabel=parse_root_label)


@parse.with_pattern(r"[a-zA-Z0-9-_]+:[a-zA-Z0-9-_]+")
def parse_label(text: str) -> Label:
    fragments = text.split(":")
    return Label.of(*fragments) if len(fragments) == 2 else Label.of(fragments[0])


register_type(ScopedLabel=parse_label)


@parse.with_pattern(r"\$([a-zA-Z0-9]+)")
def parse_var(text: str):
    return text


register_type(Var=parse_var)


@parse.with_pattern(r"long|double|string|boolean|datetime")
def parse_value_type(value: str) -> AttributeType.ValueType:
    mapping = {
        "long": AttributeType.ValueType.LONG,
        "double": AttributeType.ValueType.DOUBLE,
        "string": AttributeType.ValueType.STRING,
        "boolean": AttributeType.ValueType.BOOLEAN,
        "datetime": AttributeType.ValueType.DATETIME
    }
    return mapping[value]


register_type(ValueType=parse_value_type)


@parse.with_pattern("read|write")
def parse_transaction_type(value: str) -> GraknTransaction.Type:
    return GraknTransaction.Type.READ if value == "read" else GraknTransaction.Type.WRITE


register_type(TransactionType=parse_transaction_type)


def parse_list(table: Table) -> List[str]:
    return [table.headings[0]] + list(map(lambda row: row[0], table.rows))


def parse_dict(table: Table) -> Dict[str, str]:
    result = {table.headings[0]: table.headings[1]}
    for row in table.rows:
        result[row[0]] = row[1]
    return result


def parse_table(table: Table) -> List[List[Tuple[str, str]]]:
    """
    Extracts the rows of a Table as lists of Tuples, where each Tuple contains the column header and the cell value.

    For example, the table::

        | x         | type         |
        | key:ref:0 | label:person |
        | key:ref:2 | label:dog    |

    is converted to::

        [
            [('x', 'key:ref:0'), ('type', 'label:person')],
            [('x', 'key:ref:2'), ('type', 'label:dog')]
        ]
    """
    return [[(table.headings[idx], row[idx]) for idx in range(len(row))] for row in table.rows]
