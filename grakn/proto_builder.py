import graknprotocol.protobuf.options_pb2 as options_proto

from grakn.options import GraknOptions


class GraknProtoBuilder(object):

    @staticmethod
    def options(options: GraknOptions):
        proto_options = options_proto.Options()
        if options.infer is not None:
            proto_options.infer = options.infer
        if options.explain is not None:
            proto_options.explain = options.explain
        if options.batch_size is not None:
            proto_options.batch_size = options.batch_size
        return proto_options
