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

from typedb.connection.cluster.client import _ClusterClient
from typedb.connection.core.client import _CoreClient

# Repackaging these symbols allows them to be imported from "typedb.client"

from typedb.api.answer.concept_map import *  # noqa # pylint: disable=unused-import
from typedb.api.answer.concept_map_group import *  # noqa # pylint: disable=unused-import
from typedb.api.answer.numeric import *  # noqa # pylint: disable=unused-import
from typedb.api.answer.numeric_group import *  # noqa # pylint: disable=unused-import

from typedb.api.concept.thing.attribute import *  # noqa # pylint: disable=unused-import
from typedb.api.concept.thing.entity import *  # noqa # pylint: disable=unused-import
from typedb.api.concept.thing.relation import *  # noqa # pylint: disable=unused-import
from typedb.api.concept.thing.thing import *  # noqa # pylint: disable=unused-import

from typedb.api.concept.type.attribute_type import *  # noqa # pylint: disable=unused-import
from typedb.api.concept.type.entity_type import *  # noqa # pylint: disable=unused-import
from typedb.api.concept.type.relation_type import *  # noqa # pylint: disable=unused-import
from typedb.api.concept.type.role_type import *  # noqa # pylint: disable=unused-import
from typedb.api.concept.type.thing_type import *  # noqa # pylint: disable=unused-import
from typedb.api.concept.type.type import *  # noqa # pylint: disable=unused-import

from typedb.api.concept.concept import *  # noqa # pylint: disable=unused-import
from typedb.api.concept.concept_manager import *  # noqa # pylint: disable=unused-import

from typedb.api.connection.client import *
from typedb.api.connection.credential import *
from typedb.api.connection.database import *  # noqa # pylint: disable=unused-import
from typedb.api.connection.options import *  # noqa # pylint: disable=unused-import
from typedb.api.connection.session import *  # noqa # pylint: disable=unused-import
from typedb.api.connection.transaction import *  # noqa # pylint: disable=unused-import
from typedb.api.connection.user import *  # noqa # pylint: disable=unused-import

from typedb.api.logic.explanation import *  # noqa # pylint: disable=unused-import
from typedb.api.logic.logic_manager import *  # noqa # pylint: disable=unused-import
from typedb.api.logic.rule import *  # noqa # pylint: disable=unused-import

from typedb.api.query.query_manager import *  # noqa # pylint: disable=unused-import

from typedb.common.exception import *  # noqa # pylint: disable=unused-import
from typedb.common.label import *  # noqa # pylint: disable=unused-import


class TypeDB:
    DEFAULT_ADDRESS = "localhost:1729"

    @staticmethod
    def core_client(address: str, parallelisation: int = 2) -> TypeDBClient:
        return _CoreClient(address, parallelisation)

    @staticmethod
    def cluster_client(addresses: Union[Iterable[str], str], credential: TypeDBCredential,
                       parallelisation: int = 2) -> TypeDBClusterClient:
        if isinstance(addresses, str):
            return _ClusterClient([addresses], credential, parallelisation)
        else:
            return _ClusterClient(addresses, credential, parallelisation)
