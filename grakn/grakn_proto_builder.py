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
from typing import Type, Union

import grakn_protocol.protobuf.options_pb2 as options_proto

from grakn.options import GraknOptions, GraknClusterOptions


def options(opts: Union[GraknOptions, GraknClusterOptions]):
    proto_options = options_proto.Options()
    if opts.infer is not None:
        proto_options.infer = opts.infer
    if opts.explain is not None:
        proto_options.explain = opts.explain
    if opts.batch_size is not None:
        proto_options.batch_size = opts.batch_size
    if opts.is_cluster() and opts.read_any_replica is not None:
        proto_options.read_any_replica = opts.read_any_replica
    return proto_options
