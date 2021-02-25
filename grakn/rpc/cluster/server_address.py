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

    def __init__(self, client_host: str, client_port: int, server_host: str, server_port: int):
        self._client_host = client_host
        self._client_port = client_port
        self._server_host = server_host
        self._server_port = server_port

    def client_host(self) -> str:
        return self._client_host

    def client_port(self) -> int:
        return self._client_port

    def server_host(self) -> str:
        return self._server_host

    def server_port(self) -> int:
        return self._server_port

    def server(self) -> str:
        return "%s:%d" % (self._server_host, self._server_port)

    def client(self) -> str:
        return "%s:%d" % (self._client_host, self._client_port)

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._client_host == other.client_host() and self._client_port == other.client_port() and self._server_host == other.server_host() and self._server_port == other.server_port()

    def __hash__(self):
        return hash((self._client_host, self._client_port, self._server_host, self._server_port))

    def __str__(self):
        return "%s,%s" % (self.client(), self.server())

    @staticmethod
    def parse(address: str) -> "ServerAddress":
        s = address.split(",")
        if len(s) == 1:
            s1 = address.split(":")
            return ServerAddress(client_host=s1[0], client_port=int(s1[1]), server_host=s1[0], server_port=int(s1[1]))
        elif len(s) == 2:
            client_url = s[0].split(":")
            server_url = s[1].split(":")
            if len(client_url) != 2 or len(server_url) != 2:
                raise GraknClientException("Failed to parse server address: " + address)
            return ServerAddress(client_host=client_url[0], client_port=int(client_url[1]), server_host=server_url[0], server_port=int(server_url[1]))
        else:
            raise GraknClientException("Failed to parse server address: " + address)
