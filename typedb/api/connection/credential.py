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

from os import path
from typing import Optional

from typedb.common.exception import TypeDBClientException, CLUSTER_INVALID_ROOT_CA_PATH, CLUSTER_INCONSISTENT_CREDENTIAL
from typedb.typedb_client_python import credential_new, check_error


class TypeDBCredential:

    def __init__(self, username: str, password: str, *, tls_root_ca_path: Optional[str] = None,
                 tls_enabled: bool = True):
        if tls_root_ca_path is not None and not tls_enabled:
            raise TypeDBClientException.of(CLUSTER_INCONSISTENT_CREDENTIAL)
        self._native_object = credential_new(username, password, tls_root_ca_path, tls_enabled)
        if check_error():
            raise TypeDBClientException("Credential error.")
        if tls_root_ca_path is not None and not path.exists(tls_root_ca_path):
            raise TypeDBClientException.of(CLUSTER_INVALID_ROOT_CA_PATH, tls_root_ca_path)

    @property
    def native_object(self):
        return self._native_object
