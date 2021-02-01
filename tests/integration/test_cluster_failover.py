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

import unittest
from functools import partial
from multiprocessing.pool import ThreadPool
from unittest import TestCase

from grakn.client import GraknClient, SessionType, TransactionType
from grakn.rpc.session import Session


class TestClusterFailover(TestCase):

    def setUp(self):
        with GraknClient.cluster("localhost:11729") as client:
            if "grakn" not in client.databases().all():
                client.databases().create("grakn")

    def test_hello_world(self):
        with GraknClient.cluster("localhost:11729") as client:
            print("Hello World")


if __name__ == "__main__":
    unittest.main(verbosity=2)
