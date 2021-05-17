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
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Callable

T = TypeVar('T')
U = TypeVar('U')


class QueryFuture(Generic[T], ABC):

    @abstractmethod
    def get(self) -> T:
        pass

    def map(self, function: Callable[[T], U]) -> "QueryFuture[U]":
        return _MappedQueryFuture(self, function)


class _MappedQueryFuture(Generic[T, U], QueryFuture[U]):

    def __init__(self, query_future: QueryFuture[T], function: Callable[[T], U]):
        self._query_future = query_future
        self._function = function

    def get(self) -> U:
        return self._function(self._query_future.get())
