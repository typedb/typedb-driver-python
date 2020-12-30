package(default_visibility = ["//test/behaviour:__subpackages__"])
load("//tools:behave_rule.bzl", "py_behave_test")

py_library(
    name = "steps",
    srcs = ["session_steps.py"],
    deps = [],
)

#py_behave_test(
#    name = "test",
#    feats = ["transaction.feature"],
#    background = ["//test/behaviour:background"],
#    steps = [
#        ":steps",
#        "//test/behaviour/connection:steps",
#    ],
#    deps = [
#        "//:client_python",
#    ],
#    size = "small",
#)
