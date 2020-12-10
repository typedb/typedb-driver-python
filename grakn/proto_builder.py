import graknprotocol.protobuf.options_pb2 as options_proto

from grakn.options import GraknOptions


def options(opts: GraknOptions):
    proto_options = options_proto.Options()
    if opts.infer is not None:
        proto_options.infer = opts.infer
    if opts.explain is not None:
        proto_options.explain = opts.explain
    if opts.batch_size is not None:
        proto_options.batch_size = opts.batch_size
    return proto_options
