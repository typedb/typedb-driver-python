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
from typing import TypeVar

import typedb_protocol.core.core_service_pb2_grpc as core_service_proto
from grpc import Channel

from typedb.common.rpc.stub import TypeDBStub

T = TypeVar('T')


class _CoreStub(TypeDBStub):

    def __init__(self, channel: Channel):
        super(_CoreStub, self).__init__()
        self._channel = channel
        self._stub = core_service_proto.TypeDBStub(channel)

    def channel(self) -> Channel:
        return self._channel

    def stub(self) -> TypeDBStub:
        return self._stub
