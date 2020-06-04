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

