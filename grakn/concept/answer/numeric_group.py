import graknprotocol.protobuf.answer_pb2 as answer_proto
from grakn.concept.answer import numeric

class NumericGroup:
    def __init__(self, owner, numeric):
        self.owner = owner
        self.numeric = numeric


def _of(numeric_group_proto: answer_proto.NumericGroup):
    return NumericGroup(numeric_group_proto.owner, numeric._of(numeric_group_proto.number))