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

from __future__ import annotations
from typing import TYPE_CHECKING

from typedb.api.answer.numeric import Numeric
from typedb.common.exception import TypeDBClientException, ILLEGAL_CAST
from typedb.typedb_client_python import numeric_is_long, numeric_is_double, numeric_is_nan, \
    numeric_get_long, numeric_get_double, numeric_to_string

if TYPE_CHECKING:
    from typedb.typedb_client_python import Numeric as NativeNumeric


class _Numeric(Numeric):

    def __init__(self, numeric: NativeNumeric):
        self._numeric = numeric

    def is_int(self) -> bool:
        return numeric_is_long(self._numeric)

    def is_float(self) -> bool:
        return numeric_is_double(self._numeric)

    def is_nan(self) -> bool:
        return numeric_is_nan(self._numeric)

    def as_int(self):
        if not self.is_int():
            raise TypeDBClientException.of(ILLEGAL_CAST, "int")
        return numeric_get_long(self._numeric)

    def as_float(self):
        if not self.is_float():
            raise TypeDBClientException.of(ILLEGAL_CAST, "float")
        return numeric_get_double(self._numeric)

    def __str__(self):
        return numeric_to_string(self._numeric)

    def __eq__(self, other):
        if not (other and isinstance(other, Numeric)):
            return False
        if self.is_nan() and other.is_nan():
            return True
        if self.is_int() and other.is_int() and self.as_int() == other.as_int():
            return True
        if self.is_float() and other.is_float() and self.as_float() == other.as_float():
            return True
        return False

    def __hash__(self):
        return 0 if self.is_nan() else hash(self.as_int()) if self.is_int() else hash(self.as_float())
