import enum
import grpc
import six
import time
import uuid

from six.moves import queue

from graknprotocol.protobuf.grakn_pb2_grpc import GraknStub
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.common.exception import GraknClientException
from grakn.concept.concept_manager import ConceptManager
from grakn.options import GraknOptions
from grakn.proto_builder import GraknProtoBuilder
from grakn.query.query_manager import QueryManager
from grakn.rpc.stream import Stream


class Transaction(object):

    def __init__(self, channel, session_id, transaction_type, options=GraknOptions()):
        self._transaction_type = transaction_type
        self._concept_manager = ConceptManager(self)
        self._query_manager = QueryManager()
        self._response_queues = {}

        self._grpc_stub = GraknStub(channel)
        self._request_iterator = RequestIterator()
        self._response_iterator = self._grpc_stub.transaction(self._request_iterator)
        self._transaction_was_closed = False

        open_req = transaction_proto.Transaction.Open.Req()
        open_req.session_id = session_id
        open_req.type = Transaction._transaction_type_proto(transaction_type)
        open_req.options.CopyFrom(GraknProtoBuilder.options(options))
        req = transaction_proto.Transaction.Req()
        req.open_req.CopyFrom(open_req)

        start_time = time.time() * 1000.0
        res = self._execute(req)
        end_time = time.time() * 1000.0
        self._network_latency_millis = end_time - start_time - res.open_res.processing_time_millis

    def transaction_type(self):
        return self._transaction_type

    def is_open(self):
        return not self._transaction_was_closed

    def concepts(self):
        return self._concept_manager

    def query(self):
        return self._query_manager

    def commit(self):
        req = transaction_proto.Transaction.Req()
        commit_req = transaction_proto.Transaction.Commit.Req()
        req.commit_req.CopyFrom(commit_req)
        self._execute(req)

    def rollback(self):
        req = transaction_proto.Transaction.Req()
        rollback_req = transaction_proto.Transaction.Rollback.Req()
        req.rollback_req.CopyFrom(rollback_req)
        self._execute(req)

    def close(self):
        self._transaction_was_closed = True
        self._request_iterator.close()

    def _execute(self, request):
        response_queue = queue.Queue()
        request_id = str(uuid.uuid4())
        request.id = request_id
        self._response_queues[request_id] = response_queue
        self._request_iterator.put(request)
        print(request)
        return self._fetch(request_id)

    def _stream(self, request, transform_response):
        response_queue = queue.Queue()
        request_id = str(uuid.uuid4())
        request.id = request_id
        self._response_queues[request_id] = response_queue
        self._request_iterator.put(request)
        return Stream(self, request_id, transform_response)

    def _fetch(self, request_id):
        try:
            return self._response_queues[request_id].get(block=False)
        except queue.Empty:
            pass

        while True:
            try:
                response = next(self._response_iterator)
                if response.id == request_id:
                    return response
                else:
                    response_queue = self._response_queues[response.id]
                    if response_queue is None:
                        raise GraknClientException("Received a response with unknown request id '" + response.id + "'.")
                    response_queue.put(response)
            except StopIteration:
                raise GraknClientException("The transaction has been closed and no further operation is allowed.")
            except grpc.RpcError as e:
                # noinspection PyUnresolvedReferences
                raise GraknClientException(e.details())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is None:
            pass
        else:
            return False

    @staticmethod
    def _transaction_type_proto(transaction_type):
        if transaction_type == Transaction.Type.READ:
            return transaction_proto.Transaction.Type.Value("READ")
        if transaction_type == Transaction.Type.WRITE:
            return transaction_proto.Transaction.Type.Value("WRITE")

    class Type(enum.Enum):
        READ = 0
        WRITE = 1


class RequestIterator(six.Iterator):

    CLOSE_STREAM = "CLOSE_STREAM"

    def __init__(self):
        self._request_queue = queue.Queue()

    def __iter__(self):
        return self

    # Essentially the gRPC stream is constantly polling this iterator. When we issue a new request, it gets put into
    # the back of the queue and gRPC will pick it up when it gets round to it (this is usually instantaneous)
    def __next__(self):
        request = self._request_queue.get(block=True)
        if request is RequestIterator.CLOSE_STREAM:
            # Close the stream.
            raise StopIteration()
        return request

    def put(self, request):
        self._request_queue.put(request)

    def close(self):
        self._request_queue.put(RequestIterator.CLOSE_STREAM)
