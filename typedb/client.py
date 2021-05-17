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
from typing import Union, Iterable

from typedb.api.client import TypeDBClient, TypeDBClusterClient
from typedb.cluster.client import _ClusterClient
from typedb.core.client import _CoreClient

# Repackaging these symbols allows them to be imported from "typedb.client"
from typedb.api.options import TypeDBOptions  # noqa # pylint: disable=unused-import
from typedb.api.session import TypeDBSession, SessionType  # noqa # pylint: disable=unused-import
from typedb.api.transaction import TypeDBTransaction, TransactionType  # noqa # pylint: disable=unused-import


class TypeDB:
    DEFAULT_ADDRESS = "localhost:1729"

    @staticmethod
    def core_client(address: str, parallelisation: int = 2) -> TypeDBClient:
        return _CoreClient(address, parallelisation)

    @staticmethod
    def cluster_client(addresses: Union[Iterable[str], str], parallelisation: int = 2) -> TypeDBClusterClient:
        if isinstance(addresses, str):
            return _ClusterClient([addresses], parallelisation)
        else:
            return _ClusterClient(addresses, parallelisation)
