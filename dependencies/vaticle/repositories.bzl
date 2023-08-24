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

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def vaticle_bazel_distribution():
    native.local_repository(
        name = "vaticle_bazel_distribution",
        path = "/Users/dmitry/Vaticle/bazel-distribution",
    )

def vaticle_dependencies():
#    git_repository(
#        name = "vaticle_dependencies",
#        remote = "https://github.com/vaticle/dependencies",
#        commit = "cbdaf0134a33eaa71bfe0edb9e4878250ec9cdc0", # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_dependencies
#    )
    native.local_repository(
        name = "vaticle_dependencies",
        path = "/Users/dmitry/Vaticle/dependencies",
    )

def vaticle_typedb_common():
    git_repository(
        name = "vaticle_typedb_common",
        remote = "https://github.com/vaticle/typedb-common",
        tag = "2.18.0" # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_common
    )

def vaticle_typeql():
    git_repository(
        name = "vaticle_typeql",
        remote = "https://github.com/dmikhalin/typeql",
        commit = "c3ab12f7e96079bb5dc6d7e962e5270fec4d976e",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typeql
    )

def vaticle_typedb_protocol():
    git_repository(
        name = "vaticle_typedb_protocol",
        remote = "https://github.com/vaticle/typedb-protocol",
        commit = "444dc87520160fd4385f47617b98df6d7e741184",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_protocol
    )

def vaticle_typedb_behaviour():
    git_repository(
        name = "vaticle_typedb_behaviour",
        remote = "https://github.com/vaticle/typedb-behaviour",
        commit = "26f4a19a797203c2eeb0682b6f7d25e238b97345",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_behaviour
    )

def vaticle_factory_tracing():
    git_repository(
        name = "vaticle_factory_tracing",
        remote = "https://github.com/vaticle/factory-tracing",
        tag = "2.12.0"  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_factory_tracing
    )

def vaticle_typedb_driver_java():
#    git_repository(
#        name = "vaticle_typedb_driver_java",
#        remote = "https://github.com/vaticle/typedb-driver-java",
#        commit = "c162f8a58e9c70e03517d5b854acb8b055e30bc5"  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_factory_tracing
#    )
    native.local_repository(
        name = "vaticle_typedb_driver_java",
        path = "/Users/dmitry/Vaticle/typedb-client-java",
    )
