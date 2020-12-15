#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

load("@graknlabs_common//test/server:rules.bzl", "native_grakn_artifact")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
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
    name = "test_query",
    srcs = [
        "integration/base.py",
        "integration/test_query.py"
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
        ":test_connection",
        ":test_query",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "deployment/*",
        "integration/*",
    ]),
    license_type = "apache",
)
