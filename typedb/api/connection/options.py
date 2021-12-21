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

import typedb_protocol.common.options_pb2 as options_proto


class TypeDBOptions:

    def __init__(self):
        self.infer: Optional[bool] = None
        self.trace_inference: Optional[bool] = None
        self.explain: Optional[bool] = None
        self.parallel: Optional[bool] = None
        self.prefetch_size: Optional[int] = None
        self.prefetch: Optional[bool] = None
        self.session_idle_timeout_millis: Optional[int] = None
        self.transaction_timeout_millis: Optional[int] = None
        self.schema_lock_acquire_timeout_millis: Optional[int] = None

    @staticmethod
    def core() -> "TypeDBOptions":
        return TypeDBOptions()

    @staticmethod
    def cluster() -> "TypeDBClusterOptions":
        return TypeDBClusterOptions()

    def is_cluster(self) -> bool:
        return False

    def get_infer(self) -> Optional[bool]:
        return self.infer

    def set_infer(self, infer: bool):
        self.infer = infer
        return self

    def get_trace_inference(self) -> Optional[bool]:
        return self.trace_inference

    def set_trace_inference(self, trace_inference: bool):
        self.trace_inference = trace_inference
        return self

    def get_explain(self) -> Optional[bool]:
        return self.explain

    def set_explain(self, explain: bool):
        self.explain = explain
        return self

    def get_parallel(self) -> Optional[bool]:
        return self.parallel

    def set_parallel(self, parallel: bool):
        self.parallel = parallel
        return self

    def get_prefetch_size(self) -> Optional[int]:
        return self.prefetch_size

    def set_prefetch_size(self, prefetch_size: int):
        self.prefetch_size = prefetch_size
        return self

    def get_prefetch(self) -> Optional[bool]:
        return self.prefetch

    def set_prefetch(self, prefetch: bool):
        self.prefetch = prefetch
        return self

    def get_session_idle_timeout_millis(self) -> Optional[int]:
        return self.session_idle_timeout_millis

    def set_session_idle_timeout_millis(self, session_idle_timeout_millis: int):
        self.session_idle_timeout_millis = session_idle_timeout_millis
        return self

    def get_transaction_timeout_millis(self) -> Optional[int]:
        return self.transaction_timeout_millis

    def set_transaction_timeout_millis(self, transaction_timeout_millis: int):
        self.transaction_timeout_millis = transaction_timeout_millis
        return self

    def get_schema_lock_acquire_timeout_millis(self) -> Optional[int]:
        return self.schema_lock_acquire_timeout_millis

    def set_schema_lock_acquire_timeout_millis(self, schema_lock_acquire_timeout_millis: int):
        self.schema_lock_acquire_timeout_millis = schema_lock_acquire_timeout_millis
        return self

    def proto(self) -> options_proto.Options:
        proto_options = options_proto.Options()

        if self.infer is not None:
            proto_options.infer = self.infer
        if self.trace_inference is not None:
            proto_options.trace_inference = self.trace_inference
        if self.explain is not None:
            proto_options.explain = self.explain
        if self.parallel is not None:
            proto_options.parallel = self.parallel
        if self.prefetch_size is not None:
            proto_options.prefetch_size = self.prefetch_size
        if self.prefetch is not None:
            proto_options.prefetch = self.prefetch
        if self.session_idle_timeout_millis is not None:
            proto_options.session_idle_timeout_millis = self.session_idle_timeout_millis
        if self.transaction_timeout_millis is not None:
            proto_options.transaction_timeout_millis = self.transaction_timeout_millis
        if self.schema_lock_acquire_timeout_millis is not None:
            proto_options.schema_lock_acquire_timeout_millis = self.schema_lock_acquire_timeout_millis

        return proto_options


class TypeDBClusterOptions(TypeDBOptions):

    def __init__(self):
        super().__init__()
        self.read_any_replica: Optional[bool] = None

    def is_cluster(self) -> bool:
        return True

    def get_read_any_replica(self) -> Optional[bool]:
        return self.read_any_replica

    def set_read_any_replica(self, read_any_replica: bool):
        self.read_any_replica = read_any_replica
        return self

    def proto(self) -> options_proto.Options:
        proto_options = super(TypeDBClusterOptions, self).proto()

        if self.read_any_replica is not None:
            proto_options.read_any_replica = self.read_any_replica

        return proto_options
