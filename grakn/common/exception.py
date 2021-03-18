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

from grakn.common.error_message import ErrorMessage


class GraknClientException(Exception):

    # TODO
    def __init__(self,
                 message: Optional[str] = None,
                 cause: Optional[BaseException] = None,
                 error_message: Optional[ErrorMessage] = None,
                 *args):

        if message:
            self.message = message
        elif error_message:
            self.message = error_message.message(args)
        else:
            raise ValueError("GraknClientException")

        super(GraknClientException, self).__init__(self.message)
