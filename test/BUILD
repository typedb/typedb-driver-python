load("@graknlabs_common//test/server:rules.bzl", "native_grakn_artifact")
load("@rules_python//python:defs.bzl", "py_library", "py_test")

native_grakn_artifact(
    name = "native-grakn-artifact",
    mac_artifact = "@graknlabs_grakn_core_artifact_mac//file",
    linux_artifact = "@graknlabs_grakn_core_artifact_linux//file",
    windows_artifact = "@graknlabs_grakn_core_artifact_windows//file",
    visibility = ["//test:__subpackages__"],
)

py_test(
    name = "test_concept",
    srcs = [
        "integration/base.py",
        "integration/test_concept.py"
    ],
    deps = [
        "//:client_python",
    ],
    data = [":native-grakn-artifact"],
    args = ["$(location :native-grakn-artifact)"],
    python_version = "PY3"
)

py_test(
    name = "test_connection",
    srcs = [
        "integration/base.py",
        "integration/test_connection.py"
    ],
    deps = [
        "//:client_python",
    ],
    data = [":native-grakn-artifact"],
    args = ["$(location :native-grakn-artifact)"],
    python_version = "PY3"
)

py_test(
    name = "test_answer",
    srcs = [
        "integration/base.py",
        "integration/test_answer.py"
    ],
    deps = [
        "//:client_python",
    ],
    size = "large",
    data = [":native-grakn-artifact"],
    args = ["$(location :native-grakn-artifact)"],
    python_version = "PY3"
)

test_suite(
    name = "test_integration",
    tests = [
        ":test_concept",
        ":test_grakn",
        ":test_connection",
        ":test_answer",
    ]
)
