import graknprotocol.protobuf.answer_pb2 as answer_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto


class ConceptMap(object):

    def __init__(self):
        pass

    @staticmethod
    def of(concept_map_proto: answer_proto.ConceptMap):
        return ConceptMap()  # TODO
