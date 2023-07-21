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

from grpc import Channel, insecure_channel

from typedb.common.exception import TypeDBClientException, CLIENT_NOT_OPEN
from typedb.connection.client import _TypeDBClientImpl
from typedb.connection.core.stub import _CoreStub
from typedb.connection.database_manager import _TypeDBDatabaseManagerImpl

from typedb.typedb_client_python import connection_open_plaintext


class _CoreClient(_TypeDBClientImpl):

    def __init__(self, address: str, parallelisation: int = 2):
        super(_CoreClient, self).__init__(address, parallelisation)
        self._channel, self._stub = self.new_channel_and_stub()
        self._connection = connection_open_plaintext(address)
        self._databases = _TypeDBDatabaseManagerImpl(self._connection)
        self._is_open = True

    def databases(self) -> _TypeDBDatabaseManagerImpl:
        if not self.is_open():
            raise TypeDBClientException.of(CLIENT_NOT_OPEN)
        return self._databases

    def channel(self) -> Channel:
        return self._channel

    def stub(self) -> _CoreStub:
        return self._stub

    def new_channel_and_stub(self) -> (Channel, _CoreStub):
        channel = insecure_channel(self._address)
        return channel, _CoreStub(channel)

    def close(self) -> None:
        super().close()
