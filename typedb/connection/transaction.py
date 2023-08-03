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
from typedb.api.connection.transaction import Transaction, TransactionType
from typedb.common.exception import TypeDBClientException, TRANSACTION_CLOSED
from typedb.concept.concept_manager import _ConceptManager
from typedb.logic.logic_manager import _LogicManager
from typedb.query.query_manager import _QueryManager

if TYPE_CHECKING:
    from typedb.connection.session import _Session

from typedb.typedb_client_python import transaction_new, transaction_commit, transaction_rollback, transaction_is_open, transaction_on_close, transaction_force_close, TransactionCallbackDirector


class _Transaction(Transaction):

    def __init__(self, session: _Session, transaction_type: TransactionType, options: Options = None):
        if not options:
            options = Options()
        # self._session = session
        self._transaction_type = transaction_type
        self._options = options
        self._transaction = transaction_new(session.native_object, transaction_type.value, options.native_object)
        self._concept_manager = _ConceptManager(self._transaction)
        self._query_manager = _QueryManager(self._transaction)
        self._logic_manager = _LogicManager(self._transaction)

    @property
    def transaction_type(self) -> TransactionType:
        return self._transaction_type

    @property
    def options(self) -> Options:
        return self._options

    def is_open(self) -> bool:
        if not self._transaction.thisown:
            return False
        return transaction_is_open(self._transaction)

    @property
    def concepts(self) -> _ConceptManager:
        return self._concept_manager

    @property
    def logic(self) -> _LogicManager:
        return self._logic_manager

    @property
    def query(self) -> _QueryManager:
        return self._query_manager

    def on_close(self, function: callable):
        transaction_on_close(self._transaction, _Transaction.TransactionOnClose().callback(function))

    class TransactionOnClose(TransactionCallbackDirector):
        pass

    def commit(self):
        if not self._transaction.thisown:
            raise TypeDBClientException.of(TRANSACTION_CLOSED)
        transaction_commit(self._transaction)

    def rollback(self):
        if not self._transaction.thisown:
            raise TypeDBClientException.of(TRANSACTION_CLOSED)
        transaction_rollback(self._transaction)

    def close(self):
        if self._transaction.thisown:
            transaction_force_close(self._transaction)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is not None:
            return False
