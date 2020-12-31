package(default_visibility = ["//tests/behaviour:__subpackages__"])
load("//tools:behave_rule.bzl", "py_behave_test")

py_library(
    name = "steps",
    srcs = ["database_steps.py"],
    deps = [],
)

#py_behave_test(
#    name = "test",
#    feats = ["transaction.feature"],
#    background = ["//tests/behaviour:background"],
#    steps = [
#        ":steps",
#        "//tests/behaviour/connection:steps",
#    ],
#    deps = [
#        "//:client_python",
#    ],
#    size = "small",
#)
