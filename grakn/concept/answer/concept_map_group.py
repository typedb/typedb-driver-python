import graknprotocol.protobuf.answer_pb2 as answer_proto
from grakn.concept.answer import concept_map
from grakn.concept.proto.concept_proto_reader import concept

class ConceptMapGroup:
    def __init__(self, owner, concept_maps):
        self.owner = owner
        self.concept_maps = concept_maps


def _of(concept_map_group_proto: answer_proto.ConceptMapGroup):
    return ConceptMapGroup(concept(concept_map_group_proto.owner), map(lambda cm: concept_map._of(cm), concept_map_group_proto.concept_maps))