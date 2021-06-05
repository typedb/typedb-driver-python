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
from grpc import Channel, insecure_channel

from typedb.common.rpc.stub import TypeDBStub
from typedb.connection.connection_factory import _TypeDBConnectionFactory
from typedb.connection.core.stub import _CoreStub


class _CoreConnectionFactory(_TypeDBConnectionFactory):

    def newChannel(self, address: str) -> Channel:
        return insecure_channel(address)

    def newTypeDBStub(self, channel: Channel) -> TypeDBStub:
        return _CoreStub.create(channel)
