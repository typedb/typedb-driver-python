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

from unittest import TestCase
from grakn.client import GraknClient


class PythonApplicationTest(TestCase):
    """ Very basic tests to ensure no error occur when performing simple operations with the test grakn-client distribution"""

    def test_define_schema(self):
        client = GraknClient("localhost:48555")
        session = client.session("define_schema")
        with session.transaction().write() as tx:
            tx.query("define person sub entity, has name; name sub attribute, value string;")
            tx.commit()
        session.close()
        client.close()

    def test_match_query(self):
        client = GraknClient("localhost:48555")
        session = client.session("define_schema")
        with session.transaction().read() as tx:
            tx.query("match $s sub thing; get;")
        session.close()
        client.close()


    def test_insert_query(self):
        client = GraknClient("localhost:48555")
        session = client.session("define_schema")
        with session.transaction().write() as tx:
            tx.query("define person sub entity, has name; name sub attribute, value string;")
            tx.commit()
        with session.transaction().write() as tx:
            tx.query("insert $x isa person, has name \"john\";")
            tx.commit()
        session.close()
        client.close()

