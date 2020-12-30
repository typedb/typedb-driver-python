package(default_visibility = ["//test/behaviour:__subpackages__"])
load("//tools:behave_rule.bzl", "py_cucumber_test")

py_library(
    name = "steps",
    srcs = ["transaction_steps.py"],
    deps = [],
)

py_cucumber_test(
    name = "test",
    feats = [
        "transaction.feature",
    ],
    background = [
        "//test/behaviour:background",
    ],
    steps = [
        ":steps",
    ],
    deps = [
        "//:client_python",
    ],
    size = "small",
)
