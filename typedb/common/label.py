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
from typing import Optional


class Label:

    def __init__(self, scope: Optional[str], name: str):
        self._scope = scope
        self._name = name

    @staticmethod
    def of(*args: str) -> "Label":
        """
        Create a Label from a specified name, or scoped name.

        If a single string is provided, this is interpreted as the label name. If a pair of strings is provided,
        the first string is the scope and the second string is the name.

        **Examples**

        - ``Label.of("entity")``
        - ``Label.of("relation", "role")``
        """
        return Label(scope=args[0], name=args[1]) if len(args) == 2 else Label(scope=None, name=args[0])

    def scope(self) -> Optional[str]:
        return self._scope

    def name(self) -> str:
        return self._name

    def scoped_name(self) -> str:
        return "%s:%s" % (self._scope, self._name) if self._scope else self._name

    def __str__(self):
        return self.scoped_name()

    def __hash__(self):
        return hash((self._name, self._scope))

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._scope == other._scope and self._name == other._name
