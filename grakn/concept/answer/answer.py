import graknprotocol.protobuf.answer_pb2 as answer_proto

from grakn.common.exception import GraknClientException
from grakn.concept.answer import concept_map


class Answer(object):

    _CONCEPT_MAP = "concept_map"


def _of(proto_answer: answer_proto.Answer):
    answer_case = proto_answer.WhichOneof("answer")
    if answer_case == Answer._CONCEPT_MAP:
        return concept_map._of(proto_answer.concept_map)
    raise GraknClientException("The answer type " + answer_case + " was not recognised.")
