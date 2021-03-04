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

from grakn.common.exception import GraknClientException


class ServerAddress:

    def __init__(self, external_host: str, external_port: int, internal_host: str, internal_port: int):
        self._external_host = external_host
        self._external_port = external_port
        self._internal_host = internal_host
        self._internal_port = internal_port

    def external_host(self) -> str:
        return self._external_host

    def external_port(self) -> int:
        return self._external_port

    def internal_host(self) -> str:
        return self._internal_host

    def internal_port(self) -> int:
        return self._internal_port

    def internal(self) -> str:
        return "%s:%d" % (self._internal_host, self._internal_port)

    def external(self) -> str:
        return "%s:%d" % (self._external_host, self._external_port)

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._external_host == other.external_host() and self._external_port == other.external_port() and self._internal_host == other.internal_host() and self._internal_port == other.internal_port()

    def __hash__(self):
        return hash((self._external_host, self._external_port, self._internal_host, self._internal_port))

    def __str__(self):
        return "%s,%s" % (self.external(), self.internal())

    @staticmethod
    def parse(address: str) -> "ServerAddress":
        s = address.split(",")
        if len(s) == 1:
            s1 = address.split(":")
            return ServerAddress(external_host=s1[0], external_port=int(s1[1]), internal_host=s1[0], internal_port=int(s1[1]))
        elif len(s) == 2:
            client_url = s[0].split(":")
            server_url = s[1].split(":")
            if len(client_url) != 2 or len(server_url) != 2:
                raise GraknClientException("Failed to parse server address: " + address)
            return ServerAddress(external_host=client_url[0], external_port=int(client_url[1]), internal_host=server_url[0], internal_port=int(server_url[1]))
        else:
            raise GraknClientException("Failed to parse server address: " + address)
