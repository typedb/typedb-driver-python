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
from grakn.client import GraknClient
from grakn.common.exception import GraknClientException
from grakn.rpc.session import Session
from grakn.rpc.transaction import Transaction
from test.integration.base import test_base, GraknServer


class TestConcept(test_base):
    @classmethod
    def setUpClass(cls):
        super(TestConcept, cls).setUpClass()
        global client
        client = GraknClient()

    @classmethod
    def tearDownClass(cls):
        super(TestConcept, cls).tearDownClass()
        client.close()

    def setUp(self):
        if "grakn" not in client.databases().all():
            client.databases().create("grakn")

    def test_get_supertypes(self):
        with client.session("grakn", Session.Type.SCHEMA) as session:
            with session.transaction(Transaction.Type.WRITE) as tx:
                lion = tx.concepts().put_entity_type("lion")
                for lion_supertype in lion.as_remote(tx).get_supertypes():
                    print(lion_supertype)

    def test_streaming_operation_on_closed_tx(self):
        with client.session("grakn", Session.Type.SCHEMA) as session:
            with session.transaction(Transaction.Type.WRITE) as tx:
                lion = tx.concepts().put_entity_type("lion")
                tx.close()
                try:
                    for _ in lion.as_remote(tx).get_supertypes():
                        self.fail()
                    self.fail()
                except GraknClientException:
                    pass

    def test_invalid_streaming_operation(self):
        with client.session("grakn", Session.Type.SCHEMA) as session:
            with session.transaction(Transaction.Type.WRITE) as tx:
                lion = tx.concepts().put_entity_type("lion")
                lion._label = "lizard"
                try:
                    for _ in lion.as_remote(tx).get_supertypes():
                        self.fail()
                    self.fail()
                except GraknClientException:
                    pass

if __name__ == "__main__":
    with GraknServer():
        unittest.main(verbosity=2)
