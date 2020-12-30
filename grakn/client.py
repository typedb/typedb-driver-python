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

import grpc

from grakn.options import GraknOptions
from grakn.rpc.database_manager import DatabaseManager as _DatabaseManager
from grakn.rpc.session import Session as _Session, SessionType

# Repackaging these symbols allows users to import everything they (most likely) need from "grakn.client"
from grakn.common.exception import GraknClientException  # noqa # pylint: disable=unused-import
from grakn.concept.type.attribute_type import ValueType  # noqa # pylint: disable=unused-import
from grakn.rpc.transaction import TransactionType  # noqa # pylint: disable=unused-import


class GraknClient(object):
    DEFAULT_URI = "localhost:1729"

    def __init__(self, address=DEFAULT_URI):
        self._channel = grpc.insecure_channel(address)
        self._databases = _DatabaseManager(self._channel)
        self._is_open = True

    def session(self, database: str, session_type: SessionType, options=GraknOptions()):
        return _Session(self, database, session_type, options)

    def databases(self):
        return self._databases

    def close(self):
        self._channel.close()
        self._is_open = False

    def is_open(self):
        return self._is_open

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is None:
            pass
        else:
            return False
