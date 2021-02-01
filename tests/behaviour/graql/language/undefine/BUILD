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

package(default_visibility = ["//tests/behaviour:__subpackages__"])
load("//tools:behave_rule.bzl", "grakn_behaviour_py_test")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

grakn_behaviour_py_test(
    name = "test",
    feats = ["@graknlabs_behaviour//graql/language:undefine.feature"],
    background_core = ["//tests/behaviour/background:core"],
    background_cluster = ["//tests/behaviour/background:cluster"],
    steps = [
        "//tests/behaviour/connection:steps",
        "//tests/behaviour/connection/database:steps",
        "//tests/behaviour/connection/session:steps",
        "//tests/behaviour/connection/transaction:steps",
        "//tests/behaviour/graql:steps",
    ],
    deps = [
        "//:client_python",
        "//tests/behaviour:context",
        "//tests/behaviour:util",
        "//tests/behaviour/config:parameters",
        "//tests/behaviour/background",
    ],
    native_grakn_artifact_core = "//tests:native-grakn-core-artifact",
    native_grakn_artifact_cluster = "//tests:native-grakn-cluster-artifact",
    size = "medium",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "apache",
    size = "small",
)
