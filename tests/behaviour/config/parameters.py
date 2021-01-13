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
from typing import List, Dict

import parse
from behave import register_type
from behave.model import Table

from grakn.concept.type.value_type import ValueType
from grakn.rpc.transaction import TransactionType

# TODO: We aren't consistently using typed parameters in step implementations - we should be.

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


@parse.with_pattern(r"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d")
def parse_datetime(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")


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


@parse.with_pattern(r"\$([a-zA-Z0-9]+)")
def parse_var(text: str):
    return text


register_type(Var=parse_var)


@parse.with_pattern(r"long|double|string|boolean|datetime")
def parse_value_type(value: str) -> ValueType:
    mapping = {
        "long": ValueType.LONG,
        "double": ValueType.DOUBLE,
        "string": ValueType.STRING,
        "boolean": ValueType.BOOLEAN,
        "datetime": ValueType.DATETIME
    }
    return mapping[value]


register_type(ValueType=parse_value_type)


@parse.with_pattern("read|write")
def parse_transaction_type(value: str) -> TransactionType:
    return TransactionType.READ if value == "read" else TransactionType.WRITE


register_type(TransactionType=parse_transaction_type)


def parse_list(table: Table) -> List[str]:
    return [table.headings[0]] + list(map(lambda row: row[0], table.rows))


def parse_dict(table: Table) -> Dict[str, str]:
    print(table.headings)
    print(table.rows)
    result = {table.headings[0]: table.headings[1]}
    for row in table.rows:
        result[row[0]] = row[1]
    return result
