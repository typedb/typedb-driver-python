from grakn.rpc.database_manager import DatabaseManager
from grakn.rpc.session import Session
import grpc
import enum


class GraknClient(object):
    DEFAULT_URI = "localhost:1729"

    def __init__(self, address=DEFAULT_URI):
        self._channel = grpc.insecure_channel(address)
        self._databases = DatabaseManager(self._channel)

    def session(self, database, type, options=None):
        return Session(self._channel, database, type, options)

    def databases(self): return self._databases

    def close(self): self._channel.close()

    def __enter__(self): return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if tb is None: pass
        else: return False
