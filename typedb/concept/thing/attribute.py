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

from __future__ import annotations
from typing import Optional, Iterator, Any, TYPE_CHECKING

from typedb.api.concept.thing.attribute import Attribute
from typedb.common.streamer import Streamer
from typedb.concept.thing.thing import _Thing
from typedb.concept.type.attribute_type import _AttributeType
from typedb.concept.value.value import _Value
from typedb.typedb_client_python import attribute_get_type, attribute_get_value, attribute_get_owners, \
    concept_iterator_next

if TYPE_CHECKING:
    from typedb.concept.type.thing_type import _ThingType
    from typedb.connection.transaction import _Transaction


class _Attribute(Attribute, _Thing):

    def get_type(self) -> _AttributeType:
        return _AttributeType(attribute_get_type(self.native_object))

    def get_value(self) -> _Value:
        return _Value(attribute_get_value(self.native_object))

    def get_owners(self, transaction: _Transaction, owner_type: Optional[_ThingType] = None) -> Iterator[Any]:
        return (_Thing.of(item) for item in Streamer(attribute_get_owners(self.native_transaction(transaction),
                                                                          self.native_object,
                                                                          owner_type.native_object if owner_type else None
                                                                          ), concept_iterator_next))
