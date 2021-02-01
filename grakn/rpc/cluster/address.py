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


class Address:
    class Server:

        def __init__(self, host: str, client_port: int, server_port: int):
            self._host = host
            self._client_port = client_port
            self._server_port = server_port

        def host(self) -> str:
            return self._host

        def client_port(self) -> int:
            return self._client_port

        def server_port(self) -> int:
            return self._server_port

        def server(self) -> str:
            return "%s:%d" % (self._host, self._server_port)

        def client(self) -> str:
            return "%s:%d" % (self._host, self._client_port)

        def __eq__(self, other):
            if other is self:
                return True
            if not other or type(self) != type(other):
                return False
            return self._client_port == other.client_port() and self._server_port == other.server_port() and self._host == other.host()

        def __hash__(self):
            return hash((self._host, self._client_port, self._server_port))

        def __str__(self):
            return "%s:%d:%d" % (self._host, self._client_port, self._server_port)

        @staticmethod
        def parse(address: str) -> "Address.Server":
            s = address.split(":")
            return Address.Server(host=s[0], client_port=int(s[1]), server_port=int(s[2]))
