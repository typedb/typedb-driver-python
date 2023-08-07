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
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from typedb.api.concept.concept import Concept
from typedb.typedb_client_python import concept_to_string, concept_equals

if TYPE_CHECKING:
    from typedb.connection.transaction import _Transaction
    from typedb.typedb_client_python import Concept as NativeConcept, Transaction as NativeTransaction


class _Concept(Concept, ABC):

    def __init__(self, concept: NativeConcept):
        self._native_object = concept

    @staticmethod
    def native_transaction(transaction: _Transaction) -> NativeTransaction:
        return transaction.concepts.native_transaction()

    @property
    def native_object(self):
        return self._native_object

    def __str__(self):
        return concept_to_string(self.native_object)

    def __eq__(self, other):
        return other and isinstance(other, _Concept) and concept_equals(self.native_object, other.native_object)

    @abstractmethod
    def __hash__(self):
        pass
