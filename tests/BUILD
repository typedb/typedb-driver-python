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
load("@graknlabs_bazel_distribution//artifact:rules.bzl", "artifact_extractor")
load("@graknlabs_common//test/server:rules.bzl", "native_grakn_artifact")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_python//python:defs.bzl", "py_library", "py_test")

native_grakn_artifact(
    name = "native-grakn-artifact",
    mac_artifact = "@graknlabs_grakn_core_artifact_mac//file",
    linux_artifact = "@graknlabs_grakn_core_artifact_linux//file",
    windows_artifact = "@graknlabs_grakn_core_artifact_windows//file",
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

artifact_extractor(
    name = "grakn-extractor",
    artifact = ":native-grakn-artifact",
)
