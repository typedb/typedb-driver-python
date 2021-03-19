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

import grakn_protocol.common.options_pb2 as options_proto


class GraknOptions:

    def __init__(self):
        self.infer: Optional[bool] = None
        self.trace_inference: Optional[bool] = None
        self.explain: Optional[bool] = None
        self.parallel: Optional[bool] = None
        self.batch_size: Optional[int] = None
        self.prefetch: Optional[bool] = None
        self.session_idle_timeout_millis: Optional[int] = None
        self.schema_lock_acquire_timeout_millis: Optional[int] = None

    @staticmethod
    def core() -> "GraknOptions":
        return GraknOptions()

    @staticmethod
    def cluster() -> "GraknClusterOptions":
        return GraknClusterOptions()

    def is_cluster(self) -> bool:
        return False

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
        if self.batch_size is not None:
            proto_options.batch_size = self.batch_size
        if self.prefetch is not None:
            proto_options.prefetch = self.prefetch
        if self.session_idle_timeout_millis is not None:
            proto_options.session_idle_timeout_millis = self.session_idle_timeout_millis
        if self.schema_lock_acquire_timeout_millis is not None:
            proto_options.schema_lock_acquire_timeout_millis = self.schema_lock_acquire_timeout_millis

        return proto_options


class GraknClusterOptions(GraknOptions):

    def __init__(self):
        super().__init__()
        self.read_any_replica: Optional[bool] = None

    def is_cluster(self) -> bool:
        return True

    def proto(self) -> options_proto.Options:
        proto_options = super(GraknClusterOptions, self).proto()

        if self.read_any_replica is not None:
            proto_options.read_any_replica = self.read_any_replica

        return proto_options
