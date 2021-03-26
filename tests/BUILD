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

# Note: Do NOT rename this folder to 'test' as it conflicts with a built-in Python package!
load("//tools:cluster_test_rule.bzl", "grakn_cluster_py_test")
load("@graknlabs_bazel_distribution//artifact:rules.bzl", "artifact_extractor")
load("@graknlabs_common//test:rules.bzl", "native_grakn_artifact")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_python//python:defs.bzl", "py_library", "py_test")

native_grakn_artifact(
    name = "native-grakn-core-artifact",
    mac_artifact = "@graknlabs_grakn_core_artifact_mac//file",
    linux_artifact = "@graknlabs_grakn_core_artifact_linux//file",
    windows_artifact = "@graknlabs_grakn_core_artifact_windows//file",
    output = "grakn-core-server-artifact.tar.gz",
    visibility = ["//tests:__subpackages__"],
)

native_grakn_artifact(
    name = "native-grakn-cluster-artifact",
    mac_artifact = "@graknlabs_grakn_cluster_artifact_mac//file",
    linux_artifact = "@graknlabs_grakn_cluster_artifact_linux//file",
    windows_artifact = "@graknlabs_grakn_cluster_artifact_windows//file",
    output = "grakn-cluster-server-artifact.tar.gz",
    visibility = ["//tests:__subpackages__"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "deployment/*",
        "integration/*",
    ]),
    license_type = "apache",
    size = "small",
)

py_test(
    name = "test_concurrent",
    srcs = [
        "integration/test_concurrent.py",
    ],
    deps = [
        "//:client_python",
    ],
    python_version = "PY3"
)

grakn_cluster_py_test(
    name = "test_cluster_failover",
    srcs = [
        "integration/test_cluster_failover.py",
    ],
    deps = [
        "//:client_python",
    ],
    size = "medium",
    native_grakn_cluster_artifact = ":native-grakn-cluster-artifact",
)

artifact_extractor(
    name = "grakn-extractor",
    artifact = ":native-grakn-core-artifact",
)
