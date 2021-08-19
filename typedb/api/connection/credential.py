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

from os import path

from typedb.common.exception import TypeDBClientException, CLUSTER_INVALID_ROOT_CA_PATH


class TypeDBCredential:

    def __init__(self, username: str, password: str, tls_root_ca_path: str = None):
        self._username = username
        self._password = password

        if (tls_root_ca_path is not None and not path.exists(tls_root_ca_path)):
            raise TypeDBClientException.of(CLUSTER_INVALID_ROOT_CA_PATH, tls_root_ca_path)

        self._tls_root_ca_path = tls_root_ca_path


    def username(self):
        return self._username


    def password(self):
        return self._password

    def tls_root_ca_path(self):
        return self._tls_root_ca_path
