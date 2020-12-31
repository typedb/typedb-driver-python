package(default_visibility = ["//tests/behaviour:__subpackages__"])
load("//tools:behave_rule.bzl", "py_behave_test")

py_library(
    name = "steps",
    srcs = ["transaction_steps.py"],
    deps = [],
)

py_behave_test(
    name = "test",
    feats = ["@graknlabs_behaviour//connection:transaction.feature"],
    background = ["//tests/behaviour:background"],
    steps = [
        ":steps",
        "//tests/behaviour/connection:steps",
        "//tests/behaviour/connection/database:steps",
        "//tests/behaviour/connection/session:steps",
    ],
    deps = [
        "//:client_python",
        "//tests/behaviour/config:parameters",
    ],
    size = "small",
)
