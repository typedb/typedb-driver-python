package(default_visibility = ["//tests/behaviour:__subpackages__"])
load("//tools:behave_rule.bzl", "py_behave_test")

py_library(
    name = "steps",
    srcs = ["session_steps.py"],
    deps = [],
)

py_behave_test(
    name = "test",
    feats = ["@graknlabs_behaviour//connection:session.feature"],
    background = ["//tests/behaviour:background"],
    steps = [
        ":steps",
        "//tests/behaviour/connection:steps",
        "//tests/behaviour/connection/database:steps",
    ],
    deps = [
        "//:client_python",
        "//tests/behaviour/config:parameters",
    ],
    size = "small",
)
