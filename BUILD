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

exports_files(["requirements.txt", "deployment.bzl", "RELEASE_TEMPLATE.md"])

load("@graknlabs_client_python_pip//:requirements.bzl",
       graknlabs_client_python_requirement = "requirement")

load("@graknlabs_bazel_distribution//pip:rules.bzl", "assemble_pip", "deploy_pip")
load("@graknlabs_bazel_distribution_pip//:requirements.bzl", graknlabs_bazel_distribution_requirement = "requirement")
load("@graknlabs_bazel_distribution//github:rules.bzl", "deploy_github")
load("@graknlabs_bazel_distribution//artifact:rules.bzl", "artifact_extractor")

load("@graknlabs_dependencies//tool/release:rules.bzl", "release_validate_python_deps")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@graknlabs_dependencies//distribution:deployment.bzl", "deployment")
load(":deployment.bzl", github_deployment = "deployment")



py_library(
    name = "client_python",
    srcs = glob(["grakn/**/*.py"]),
    deps = [
        graknlabs_client_python_requirement("graknprotocol"),
        graknlabs_client_python_requirement("protobuf"),
        graknlabs_client_python_requirement("grpcio"),
        graknlabs_client_python_requirement("six"),
    ],
    visibility = ["//visibility:public"]
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        ".grabl/automation.yml",
        "grakn/**/*",
    ]),
    license_type = "apache",
    size = "small",
)

assemble_pip(
    name = "assemble-pip",
    target = ":client_python",
    package_name = "grakn-client",
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Environment :: Console",
        "Topic :: Database :: Front-Ends"
    ],
    url = "https://github.com/graknlabs/client-python/",
    author = "Grakn Labs",
    author_email = "community@grakn.ai",
    license = "Apache-2.0",
    requirements_file = "//:requirements.txt",
    keywords = ["grakn", "database", "graph", "knowledgebase", "knowledge-engineering"],
    description = "Grakn Client for Python",
    long_description_file = "//:README.md",
)


deploy_pip(
    name = "deploy-pip",
    target = ":assemble-pip",
    snapshot = deployment["pypi.snapshot"],
    release = deployment["pypi.release"],
)


deploy_github(
    name = "deploy-github",
    release_description = "//:RELEASE_TEMPLATE.md",
    title = "Grakn Client Python",
    title_append_version = True,
    organisation = github_deployment["github.organisation"],
    repository = github_deployment["github.repository"],
    draft = False
)

artifact_extractor(
    name = "grakn-extractor",
    artifact = "@graknlabs_grakn_core_artifact_linux//file",
)

release_validate_python_deps(
    name = "release-validate-python-deps",
    requirements = "//:requirements.txt",
    tagged_deps = [
        "graknprotocol",
    ],
)

# CI targets that are not declared in any BUILD file, but are called externally
filegroup(
    name = "ci",
    data = [
        "@graknlabs_dependencies//tool/bazelrun:rbe",
        "@graknlabs_dependencies//distribution/artifact:create-netrc",
        "@graknlabs_dependencies//tool/sync:dependencies",
        "@graknlabs_dependencies//tool/release:approval",
        "@graknlabs_dependencies//tool/release:create-notes",
    ],
)
