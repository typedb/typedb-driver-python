from typing import Callable, List

import graknprotocol.protobuf.query_pb2 as query_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.concept.answer.concept_map import ConceptMap
from grakn.options import GraknOptions
from grakn.proto_builder import GraknProtoBuilder


class QueryManager(object):

    def __init__(self, transaction):
        self._transaction = transaction

    def match(self, query: str, options=GraknOptions()):
        request = query_proto.Query.Req()
        match_req = query_proto.Graql.Match.Req()
        match_req.query = query
        request.match_req.CopyFrom(match_req)
        return map(lambda proto_answer: ConceptMap.of, self._iterate_query(request, lambda res: res.query_res.match_res.answer, options))

    def insert(self, query: str, options=GraknOptions()):
        request = query_proto.Query.Req()
        insert_req = query_proto.Graql.Insert.Req()
        insert_req.query = query
        request.insert_req.CopyFrom(insert_req)
        return map(lambda proto_answer: ConceptMap.of, self._iterate_query(request, lambda res: res.query_res.insert_res.answer, options))

    def delete(self, query: str, options=GraknOptions()):
        request = query_proto.Query.Req()
        delete_req = query_proto.Graql.Delete.Req()
        delete_req.query = query
        request.delete_req.CopyFrom(delete_req)
        return self._run_query(request, options)

    def define(self, query: str, options=GraknOptions()):
        request = query_proto.Query.Req()
        define_req = query_proto.Graql.Define.Req()
        define_req.query = query
        request.define_req.CopyFrom(define_req)
        return self._run_query(request, options)

    def undefine(self, query: str, options=GraknOptions()):
        request = query_proto.Query.Req()
        undefine_req = query_proto.Graql.Undefine.Req()
        undefine_req.query = query
        request.undefine_req.CopyFrom(undefine_req)
        return self._run_query(request, options)

    def _run_query(self, query_req: query_proto.Query.Req, options: GraknOptions):
        req = transaction_proto.Transaction.Req()
        query_req.options.CopyFrom(GraknProtoBuilder.options(options))
        req.query_req.CopyFrom(query_req)
        # Using stream makes this request asynchronous; the user is not expected to iterate over this Stream
        self._transaction._stream(req)

    def _iterate_query(self, query_req: query_proto.Query.Req, response_reader: Callable[[transaction_proto.Transaction.Res], List], options: GraknOptions):
        req = transaction_proto.Transaction.Req()
        query_req.options.CopyFrom(GraknProtoBuilder.options(options))
        req.query_req.CopyFrom(query_req)
        return self._transaction._stream(req, response_reader)
