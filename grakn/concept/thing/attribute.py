import graknprotocol.protobuf.concept_pb2 as concept_proto

from grakn.concept.thing.thing import Thing


class Attribute(Thing):
    pass


class BooleanAttribute(Attribute):

    @staticmethod
    def _of(thing_proto: concept_proto.Thing):
        pass  # TODO: attr value
        # return BooleanAttribute(thing_proto.iid.hex())


class LongAttribute(Attribute):
    pass


class DoubleAttribute(Attribute):
    pass


class StringAttribute(Attribute):
    pass


class DateTimeAttribute(Attribute):
    pass
