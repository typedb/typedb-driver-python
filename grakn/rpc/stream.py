import six

import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.common.exception import GraknClientException


class Stream(six.Iterator):

    CONTINUE = "continue"
    DONE = "done"

    def __init__(self, transaction, request_id, transform_response):
        self._transaction = transaction
        self._request_id = request_id
        self._transform_response = transform_response
        self._current_iterator = None

    def __iter__(self):
        return self

    def __next__(self):
        if self._current_iterator is not None:
            try:
                return next(self._current_iterator)
            except StopIteration:
                self._current_iterator = None

        res = self._transaction._fetch(self._request_id)
        res_case = res.WhichOneof("res")
        if res_case == Stream.CONTINUE:
            continue_req = transaction_proto.Transaction.Req()
            continue_req.id = self._request_id
            setattr(continue_req, "continue", True)
            self._transaction._request_iterator.put(continue_req)
            return next(self)
        elif res_case == Stream.DONE:
            raise StopIteration()
        elif res_case is None:
            raise GraknClientException("The required field 'res' of type 'transaction_proto.Transaction.Res' was not set.")
        else:
            self._current_iterator = iter(self._transform_response(res))
            return next(self)
