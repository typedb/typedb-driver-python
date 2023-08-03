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
from abc import ABC, abstractmethod
from typing import Optional

from typedb.api.connection.database import DatabaseManager
from typedb.api.connection.options import Options
from typedb.api.connection.session import Session, SessionType
from typedb.api.connection.user import UserManager, User


class Client(ABC):

    @abstractmethod
    def is_open(self) -> bool:
        pass

    @property
    @abstractmethod
    def databases(self) -> DatabaseManager:
        pass

    @abstractmethod
    def session(self, database: str, session_type: SessionType, options: Optional[Options] = None) -> Session:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @property
    @abstractmethod
    def users(self) -> UserManager:
        pass

    @abstractmethod
    def user(self) -> User:
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
