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
from enum import Enum
from abc import ABC

from typedb.api.concept.concept import Concept
from typedb.concept.concept_manager import _ConceptManager
from typedb.connection.transaction import _TransactionImpl

from typedb.typedb_client_python import Concept as NativeConcept, Transaction as NativeTransaction, Transitive, Explicit


class Transitivity(Enum):
    Transitive = Transitive
    Explicit = Explicit


class _Concept(Concept, ABC):

    def __init__(self, concept: NativeConcept):
        self._concept = concept

    @staticmethod
    def native_transaction(transaction: _TransactionImpl) -> NativeTransaction:
        return transaction.concepts().native_transaction()

    def native_object(self):
        return self._concept

    # def is_remote(self):
    #     return False


# class _RemoteConcept(RemoteConcept, ABC):
#
#     def is_remote(self):
#         return True
