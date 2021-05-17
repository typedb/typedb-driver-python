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
from threading import Lock


class AtomicBoolean:

    def __init__(self, initial_value: bool):
        self._value = initial_value
        self._lock = Lock()

    def get(self) -> bool:
        return self._value

    def set(self, value: bool) -> None:
        with self._lock:
            self._value = value

    def compare_and_set(self, expected_value: bool, new_value: bool) -> bool:
        """
        Atomically sets the value to ``newValue`` if the current value == ``expectedValue``.

        :param expected_value: the expected value
        :param new_value: the new value
        :return: True if successful, False if the actual value was not equal to the expected value.
        """
        with self._lock:
            result = self._value == expected_value
            self._value = new_value
            return result
