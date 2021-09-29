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

from typedb.client import TypeDBSession, SessionType, TypeDB, TransactionType


class TestConcurrent(TestCase):

    def setUp(self):
        with TypeDB.core_client("127.0.0.1:1729") as client:
            if "typedb" not in client.databases().all():
                client.databases().create("typedb")

    def open_tx(self, session: TypeDBSession, *args):
        tx = session.transaction(TransactionType.WRITE)
        tx.close()
        self.txs_closed += 1
        print("Total txs closed: %d" % self.txs_closed)

    def test_open_many_transactions_in_parallel(self):
        self.txs_closed = 0
        with TypeDB.core_client("127.0.0.1:1729") as client:
            with client.session("typedb", SessionType.DATA) as session:
                pool = ThreadPool(8)
                results = [None for _ in range(10000)]
                pool.map(partial(self.open_tx, session), results)
                pool.close()
                pool.join()


if __name__ == "__main__":
    unittest.main(verbosity=2)
