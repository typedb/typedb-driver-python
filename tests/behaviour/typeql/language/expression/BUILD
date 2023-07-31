#
# Copyright (C) 2022 Vaticle
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
load("//tool:behave_rule.bzl", "typedb_behaviour_py_test")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

typedb_behaviour_py_test(
    name = "test",
    feats = ["@vaticle_typedb_behaviour//typeql/language:expression.feature"],
    background_core = ["//tests/behaviour/background:core"],
    background_cluster = ["//tests/behaviour/background:cluster"],
    steps = [
        "//tests/behaviour/connection:steps",
        "//tests/behaviour/connection/database:steps",
        "//tests/behaviour/connection/session:steps",
        "//tests/behaviour/connection/transaction:steps",
        "//tests/behaviour/typeql:steps",
    ],
    deps = [
        "//:client_python",
        "//tests/behaviour:context",
        "//tests/behaviour/util:util",
        "//tests/behaviour/config:parameters",
        "//tests/behaviour/background",
        "@vaticle_typedb_driver_java//rust:typedb_client_python",
        "//:python-lib",
    ],
    native_typedb_artifact = "//tests:native-typedb-artifact",
    native_typedb_cluster_artifact = "//tests:native-typedb-cluster-artifact",
    size = "medium",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "apache-header",
    size = "small",
)
