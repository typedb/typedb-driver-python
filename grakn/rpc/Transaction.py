import enum
import time
import uuid
import six

from six.moves import queue

from graknprotocol.protobuf.grakn_pb2_grpc import GraknStub
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.common.exception.GraknClientException import GraknClientException
from grakn.concept.ConceptManager import ConceptManager
from grakn.query.QueryManager import QueryManager


class Transaction(object):

    def __init__(self, channel, session_id, transaction_type, options=None):
        self._transaction_type = transaction_type
        self._concept_manager = ConceptManager()
        self._query_manager = QueryManager()
        self._collectors = {}

        self._grpc_stub = GraknStub(channel)
        self._request_iterator = RequestIterator()
        self._response_iterator = self._grpc_stub.transaction(self._request_iterator)
        self._stream_is_open = True
        self._transaction_was_closed = False

        open_req = transaction_proto.Transaction.Open.Req()
        open_req.session_id = session_id
        open_req.type = Transaction._transaction_type_proto(transaction_type)
        # TODO set open_req.options
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
        pass

    def rollback(self):
        pass

    def close(self):
        # TODO: use atomics?
        if not self._transaction_was_closed:
            self._transaction_was_closed = True
            for request_id, collector in self._collectors.items():
                collector.put(GraknClientException())
        if self._stream_is_open:
            self._stream_is_open = False
            self._request_iterator.close()

    def _execute(self, request):
        response_collector = queue.Queue()
        request_id = str(uuid.uuid4())
        request.id = request_id
        self._collectors[request_id] = response_collector
        self._request_iterator.put(request)
        # TODO: maybe the ResponseCollector should be responsible for triggering fetch? It's a bit weird that it does
        #   the blocking and take() just returns instantly
        return self._listen(request_id)
        # return response_collector.take(block=True)

    def _stream(self, request):
        pass
        # TODO: transformResponse?
        # TODO: return an Iterator

    def _listen(self, request_id):
        collector = self._collectors[request_id]
        if collector is None:
            raise GraknClientException()
        # TODO: so we don't actually need a blocking queue, right?
        if collector.qsize() > 0:
            return collector.take(block=False)

        while True:
            try:
                response = next(self._response_iterator)
                print(response)
                if isinstance(response, Exception):
                    raise response
                if response.id == request_id:
                    return response
                else:
                    incoming_response_collector = self._collectors[response.id]
                    if incoming_response_collector is None:
                        raise GraknClientException()
                    incoming_response_collector.put(response)
                # TODO onNext processing logic (and QueryIterator? hopefully not)
            except Exception as e:
                # TODO self.close()
                # TODO GraknClientException
                raise e

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
        if transaction_type == Transaction.Type.READ: return transaction_proto.Transaction.Type.Value("READ")
        if transaction_type == Transaction.Type.WRITE: return transaction_proto.Transaction.Type.Value("WRITE")

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


# TODO: do we even need this?
class ResponseCollector(object):
    def __init__(self):
        self._response_buffer = queue.Queue()

    def add(self, response):
        self._response_buffer.put(response)

    def take(self, block):
        return self._response_buffer.get(block=block)
        # TODO: In Client Python 1.8, single responses are returned synchronously; streaming responses return Iterators


class Response(object):
    pass
    # TODO: Response logic?
