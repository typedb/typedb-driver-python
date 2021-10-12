#
#   Copyright (C) 2021 Vaticle
#
#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing,
#   software distributed under the License is distributed on an
#   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#   KIND, either express or implied.  See the License for the
#   specific language governing permissions and limitations
#   under the License.
#
from typing import Callable

import grpc

from typedb.api.connection.credential import TypeDBCredential
from typedb.connection.client import _TypeDBClientImpl
from typedb.connection.cluster.stub import _ClusterServerStub
from typedb.connection.database_manager import _TypeDBDatabaseManagerImpl


class _ClusterServerClient(_TypeDBClientImpl):

    def __init__(self, address: str, credential: TypeDBCredential, parallelisation: int = 2):
        super(_ClusterServerClient, self).__init__(address, parallelisation)
        self._credential = credential
        self._channel, self._stub = self.new_channel_and_stub()
        self._databases = _TypeDBDatabaseManagerImpl(self.stub())

    def databases(self) -> _TypeDBDatabaseManagerImpl:
        return self._databases

    def channel(self) -> grpc.Channel:
        return self._channel

    def stub(self) -> _ClusterServerStub:
        return self._stub

    def new_channel_and_stub(self) -> (grpc.Channel, _ClusterServerStub):
        channel = self._new_channel()
        return channel, _ClusterServerStub(channel, self._credential)

    def _new_channel(self) -> grpc.Channel:
        if self._credential.tls_root_ca_path() is not None:
            with open(self._credential.tls_root_ca_path(), 'rb') as root_ca:
                channel_credentials = grpc.ssl_channel_credentials(root_ca.read())
        else:
            channel_credentials = grpc.ssl_channel_credentials()
        combined_credentials = grpc.composite_channel_credentials(
            channel_credentials,
            grpc.metadata_call_credentials(_CredentialAuth(credential=self._credential, token_fn=lambda: self._stub.token()))
        )
        return grpc.secure_channel(self._address, combined_credentials)

    def close(self) -> None:
        super().close()
        self._channel.close()


class _CredentialAuth(grpc.AuthMetadataPlugin):
    def __init__(self, credential, token_fn: Callable[[], str]):
        self._credential = credential
        self._token_fn = token_fn

    def __call__(self, context, callback):
        token = self._token_fn()
        if token is None:
            callback((('username', self._credential.username()), ('password', self._credential.password())), None)
        else:
            callback((('username', self._credential.username()), ('token', token)), None)
