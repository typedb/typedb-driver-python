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
from typing import Optional, Tuple, Union

from grpc import RpcError, Call, StatusCode

from grakn.common.error_message import ErrorMessage, UNABLE_TO_CONNECT, CLUSTER_REPLICA_NOT_PRIMARY


class GraknClientException(Exception):

    def __init__(self, message: Union[ErrorMessage, str], cause: Optional[BaseException], params: Tuple[str, ...]):
        if isinstance(message, str):
            self.message = message
        else:
            self.message = message.message(params)
            self.error_message = message

        self.__cause__ = cause
        super(GraknClientException, self).__init__(self.message)

    @staticmethod
    def from_rpc(rpc_error: Union[RpcError, Call]) -> "GraknClientException":
        if rpc_error.code() in [StatusCode.UNAVAILABLE, StatusCode.UNKNOWN] or "Received RST_STREAM" in str(rpc_error):
            return GraknClientException(message=UNABLE_TO_CONNECT, cause=rpc_error, params=())
        elif rpc_error.code() is StatusCode.INTERNAL and "[RPL01]" in str(rpc_error):
            return GraknClientException(message=CLUSTER_REPLICA_NOT_PRIMARY, cause=rpc_error, params=())
        else:
            return GraknClientException(message=rpc_error.details(), cause=rpc_error, params=())

    @staticmethod
    def of(error_message: ErrorMessage, *args):
        return GraknClientException(message=error_message, cause=None, params=args)
