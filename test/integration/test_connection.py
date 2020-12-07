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
from grakn.rpc.Session import Session
from grakn.rpc.Transaction import Transaction
from test.integration.base import test_base, GraknServer

client: GraknClient


class ConnectionTest(test_base):
    @classmethod
    def setUpClass(cls):
        super(ConnectionTest, cls).setUpClass()
        global client
        client = GraknClient()

    @classmethod
    def tearDownClass(cls):
        super(ConnectionTest, cls).tearDownClass()
        global client
        client.close()

    def test_database(self):
        dbs = client.databases().all()
        if "grakn" in dbs:
            client.databases().delete("grakn")
        client.databases().create("grakn")
        dbs = client.databases().all()
        self.assertTrue("grakn" in dbs)

    def test_session(self):
        if "grakn" not in client.databases().all():
            client.databases().create("grakn")
        session = client.session("grakn", Session.Type.SCHEMA)
        session.close()

    def test_transaction(self):
        if "grakn" not in client.databases().all():
            client.databases().create("grakn")
        with client.session("grakn", Session.Type.SCHEMA) as session:
            with session.transaction(Transaction.Type.WRITE) as tx:
                pass

if __name__ == "__main__":
    with GraknServer():
        unittest.main(verbosity=2)
