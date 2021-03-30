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

from grakn.client import *


GRAKN = "grakn"
SCHEMA = SessionType.SCHEMA
DATA = SessionType.DATA
WRITE = TransactionType.WRITE
READ = TransactionType.READ


class TestConcurrent(TestCase):

    def setUp(self):
        with Grakn.core_client(Grakn.DEFAULT_ADDRESS) as client:
            if client.databases().contains(GRAKN):
                client.databases().get(GRAKN).delete()
            client.databases().create(GRAKN)

    def open_tx(self, session: GraknSession, *args):
        tx = session.transaction(WRITE)
        tx.close()
        self.txs_closed += 1
        print("Total txs closed: %d" % self.txs_closed)

    def test_open_many_transactions_in_parallel(self):
        self.txs_closed = 0
        with Grakn.core_client(Grakn.DEFAULT_ADDRESS) as client, client.session(GRAKN, DATA) as session:
            pool = ThreadPool(8)
            results = [None for _ in range(10)]
            pool.map(partial(self.open_tx, session), results)
            pool.close()
            pool.join()

    def test_explanations(self):
        with Grakn.core_client(Grakn.DEFAULT_ADDRESS) as client:
            with client.session(GRAKN, SCHEMA) as session, session.transaction(WRITE) as tx:
                schema = """
                define
                person sub entity, owns name, plays friendship:friend, plays marriage:husband, plays marriage:wife;
                name sub attribute, value string;
                friendship sub relation, relates friend;
                marriage sub relation, relates husband, relates wife;
                rule marriage-is-friendship: when {
                    $x isa person; $y isa person; (husband: $x, wife: $y) isa marriage;
                } then {
                    (friend: $x, friend: $y) isa friendship;
                };
                rule everyone-is-friends: when {
                    $x isa person; $y isa person; not { $x is $y; };
                } then {
                    (friend: $x, friend: $y) isa friendship;
                };
                """
                tx.query().define(schema)
                tx.commit()

            with client.session(GRAKN, DATA) as session, session.transaction(WRITE) as tx:
                data = """
                insert
                $x isa person, has name "Zack";
                $y isa person, has name "Yasmin";
                (husband: $x, wife: $y) isa marriage;
                """
                tx.query().insert(data)
                tx.commit()

            opts = GraknOptions.core()
            opts.explain = True
            with client.session(GRAKN, DATA) as session, session.transaction(READ, opts) as tx:
                answers = list(tx.query().match("match (friend: $p1, friend: $p2) isa friendship; $p1 has name $na;"))
                assert len(answers[0].explainables().relations()) == 1
                assert len(answers[1].explainables().relations()) == 1
                explanations = list(tx.query().explain(next(iter(answers[0].explainables().relations().values()))))
                assert len(explanations) == 3
                explanations2 = list(tx.query().explain(next(iter(answers[1].explainables().relations().values()))))
                assert len(explanations2) == 3
                print([str(e) for e in explanations])


if __name__ == "__main__":
    unittest.main(verbosity=2)
