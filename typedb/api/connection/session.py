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
import enum
from abc import ABC, abstractmethod

import typedb_protocol.common.session_pb2 as session_proto

from typedb.api.connection.database import Database
from typedb.api.connection.options import TypeDBOptions
from typedb.api.connection.transaction import TypeDBTransaction, TransactionType


class SessionType(enum.Enum):
    DATA = 0
    SCHEMA = 1

    def is_data(self):
        return self is SessionType.DATA

    def is_schema(self):
        return self is SessionType.SCHEMA

    def proto(self):
        return session_proto.Session.Type.Value(self.name)


class TypeDBSession(ABC):

    @abstractmethod
    def is_open(self) -> bool:
        pass

    @abstractmethod
    def session_type(self) -> "SessionType":
        pass

    @abstractmethod
    def database(self) -> Database:
        pass

    @abstractmethod
    def options(self) -> TypeDBOptions:
        pass

    @abstractmethod
    def transaction(self, transaction_type: TransactionType, options: TypeDBOptions = None) -> TypeDBTransaction:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
