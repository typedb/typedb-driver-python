import graknprotocol.protobuf.logic_pb2 as logic_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from logic.rule import Rule


class LogicManager(object):

    def __init__(self, transaction):
        self._transaction = transaction

    def put_rule(self, label: str, when: str, then: str):
        req = logic_proto.LogicManager.Req()
        put_rule_req = logic_proto.LogicManager.PutRule.Req()
        put_rule_req.label = label
        put_rule_req.when = when
        put_rule_req.then = then
        req.put_rule_req.CopyFrom(put_rule_req)
        res = self._execute(req)
        return Rule._of(res.put_rule_res.rule)

    def get_rule(self, label: str):
        req = logic_proto.LogicManager.Req()
        get_rule_req = logic_proto.LogicManager.GetRule.Req()
        get_rule_req.label = label
        req.get_rule_req.CopyFrom(get_rule_req)

        response = self._execute(req)
        return Rule._of(response.get_rule_res.rule) if response.get_rule_res.WhichOneof("res") == "rule" else None

    def _execute(self, request: logic_proto.LogicManager.Req):
        req = transaction_proto.Transaction.Req()
        req.logic_manager_req.CopyFrom(request)
        return self._transaction._execute(req).logic_manager_res
