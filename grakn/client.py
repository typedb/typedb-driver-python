import grpc
import sched
import time

from grakn.options import GraknOptions
from grakn.rpc.database_manager import DatabaseManager
from grakn.rpc.session import Session


class GraknClient(object):
    DEFAULT_URI = "localhost:1729"

    def __init__(self, address=DEFAULT_URI):
        self._channel = grpc.insecure_channel(address)
        self._databases = DatabaseManager(self._channel)
        self._scheduler = sched.scheduler(time.time, time.sleep)

    def session(self, database, session_type, options=GraknOptions()):
        return Session(self, database, session_type, options)

    def databases(self):
        return self._databases

    def close(self):
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is None:
            pass
        else:
            return False
