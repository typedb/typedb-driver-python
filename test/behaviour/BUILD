load("//tools:behave_rule.bzl", "py_cucumber_test")

py_cucumber_test(
    name = "transaction_test",
    feats = [
        "transaction.feature",
    ],
    steps = [
        "environment.py", # TODO: this should probably be a separate parameter?
        "steps/transaction_steps.py",
    ],
    deps = [
        "//:client_python",
    ],
    size = "small",
)
