#
# Copyright (C) 2020 Grakn Labs
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

exports_files(["requirements.txt", "deployment.bzl", "RELEASE_TEMPLATE.md"])

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("@graknlabs_client_python_pip//:requirements.bzl",
       graknlabs_client_python_requirement = "requirement")

load("@graknlabs_bazel_distribution//pip:rules.bzl", "assemble_pip", "deploy_pip")
load("@graknlabs_bazel_distribution_pip//:requirements.bzl", graknlabs_bazel_distribution_requirement = "requirement")
load("@graknlabs_bazel_distribution//github:rules.bzl", "deploy_github")
load("@graknlabs_bazel_distribution//artifact:rules.bzl", "artifact_extractor")

load("@graknlabs_dependencies//tool/release:rules.bzl", "release_validate_deps")
load("@graknlabs_dependencies//distribution:deployment.bzl", "deployment")
load(":deployment.bzl", github_deployment = "deployment")



py_library(
    name = "client_python",
    srcs = glob(["grakn/**/*.py"]),
    deps = [
        "@graknlabs_protocol//grpc/python:protocol",
        graknlabs_client_python_requirement("protobuf"),
        graknlabs_client_python_requirement("grpcio"),
        graknlabs_client_python_requirement("six"),
    ],
    visibility =["//visibility:public"]
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
    install_requires=['grpcio==1.24.1,<2', 'protobuf==3.6.1', 'six>=1.11.0'],
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
)

py_test(
    name = "test_concept",
    srcs = [
        "tests/integration/base.py",
        "tests/integration/test_concept.py"
    ],
    deps = [
        ":client_python",
    ],
    data = ["@graknlabs_grakn_core_artifact//file"],
    args = ["$(location @graknlabs_grakn_core_artifact//file)"],
    python_version = "PY3"
)

py_test(
    name = "test_grakn",
    srcs = [
        "tests/integration/base.py",
        "tests/integration/test_grakn.py"
    ],
    deps = [
        ":client_python",
    ],
    data = ["@graknlabs_grakn_core_artifact//file"],
    args = ["$(location @graknlabs_grakn_core_artifact//file)"],
    python_version = "PY3"
)

py_test(
    name = "test_keyspace",
    srcs = [
        "tests/integration/base.py",
        "tests/integration/test_keyspace.py"
    ],
    deps = [
        ":client_python",
    ],
    data = ["@graknlabs_grakn_core_artifact//file"],
    args = ["$(location @graknlabs_grakn_core_artifact//file)"],
    python_version = "PY3"
)

py_test(
    name = "test_answer",
    srcs = [
        "tests/integration/base.py",
        "tests/integration/test_answer.py"
    ],
    deps = [
        ":client_python",
    ],
    size = "large",
    data = ["@graknlabs_grakn_core_artifact//file"],
    args = ["$(location @graknlabs_grakn_core_artifact//file)"],
    python_version = "PY3"
)

test_suite(
    name = "test_integration",
    tests = [
        ":test_concept",
        ":test_grakn",
        ":test_keyspace",
        ":test_answer",
    ]
)

artifact_extractor(
    name = "grakn-extractor",
    artifact = "@graknlabs_grakn_core_artifact//file",
)

release_validate_deps(
    name = "release-validate-deps",
    refs = "@graknlabs_client_python_workspace_refs//:refs.json",
    tagged_deps = [
        "@graknlabs_protocol",
    ],
    tags = ["manual"]  # in order for bazel test //... to not fail
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
