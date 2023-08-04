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

from typedb.api.connection.options import Options
from typedb.api.connection.transaction import Transaction
from typedb.common.exception import TypeDBClientException, TRANSACTION_CLOSED
from typedb.concept.concept_manager import _ConceptManager
from typedb.logic.logic_manager import _LogicManager
from typedb.query.query_manager import _QueryManager
from typedb.typedb_client_python import transaction_new, transaction_commit, transaction_rollback, transaction_is_open, transaction_on_close, transaction_force_close, TransactionCallbackDirector

if TYPE_CHECKING:
    from typedb.connection.session import _Session
    from typedb.api.connection.transaction import TransactionType


class _Transaction(Transaction):

    def __init__(self, session: _Session, transaction_type: TransactionType, options: Options = None):
        if not options:
            options = Options()
        self._transaction_type = transaction_type
        self._options = options
        self._native_object = transaction_new(session.native_object, transaction_type.value, options.native_object)
        self._concept_manager = _ConceptManager(self._native_object)
        self._query_manager = _QueryManager(self._native_object)
        self._logic_manager = _LogicManager(self._native_object)

    @property
    def transaction_type(self) -> TransactionType:
        return self._transaction_type

    @property
    def options(self) -> Options:
        return self._options

    @property
    def concepts(self) -> _ConceptManager:
        return self._concept_manager

    @property
    def logic(self) -> _LogicManager:
        return self._logic_manager

    @property
    def query(self) -> _QueryManager:
        return self._query_manager

    @property
    def native_object(self):
        return self._native_object

    def is_open(self) -> bool:
        if not self.native_object.thisown:
            return False
        return transaction_is_open(self.native_object)

    def on_close(self, function: callable):
        transaction_on_close(self.native_object, _Transaction.TransactionOnClose().callback(function))

    class TransactionOnClose(TransactionCallbackDirector):
        pass

    def commit(self):
        if not self.native_object.thisown:
            raise TypeDBClientException.of(TRANSACTION_CLOSED)
        transaction_commit(self.native_object)

    def rollback(self):
        if not self.native_object.thisown:
            raise TypeDBClientException.of(TRANSACTION_CLOSED)
        transaction_rollback(self.native_object)

    def close(self):
        if self.native_object.thisown:
            transaction_force_close(self.native_object)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is not None:
            return False
