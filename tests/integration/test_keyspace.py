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
from tests.integration.base import test_Base, GraknServer

client = None
session = None

class test_Keyspace(test_Base):
    @classmethod
    def setUpClass(cls):
        super(test_Keyspace, cls).setUpClass()
        global client, session
        client = GraknClient("localhost:48555")
        session = client.session("keyspacetest")

    @classmethod
    def tearDownClass(cls):
        super(test_Keyspace, cls).tearDownClass()
        global client, session
        session.close()
        client.close()

    def test_retrieve_delete(self):
       """ Test retrieving and deleting a specific keyspace """

       tx = session.transaction().write()
       tx.close()

       keyspaces = client.keyspaces().retrieve()
       self.assertGreater(len(keyspaces), 0)
       self.assertTrue('keyspacetest' in keyspaces)

       client.keyspaces().delete('keyspacetest')
       post_delete_keyspaces = client.keyspaces().retrieve()
       self.assertFalse('keyspacetest' in post_delete_keyspaces)


if __name__ == "__main__":
    with GraknServer():
        unittest.main(verbosity=2)
