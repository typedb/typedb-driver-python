load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "graknlabs_grakn_core",
    remote = "https://github.com/graknlabs/grakn",
    commit = "54f1fbcdc5a90840efacbac55faee6ab19cd581f" # grabl-marker: do not remove this comment, this is used for dependency-update by @graknlabs_grakn_core
)

git_repository(
    name = "io_bazel_rules_python",
    remote = "https://github.com/bazelbuild/rules_python.git",
    commit = "e6399b601e2f72f74e5aa635993d69166784dde1",
)

load("@io_bazel_rules_python//python:pip.bzl", "pip_repositories", "pip_import")
pip_repositories()

load("@graknlabs_grakn_core//dependencies/compilers:dependencies.bzl", "grpc_dependencies")
grpc_dependencies()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", com_github_grpc_grpc_bazel_grpc_deps = "grpc_deps")
com_github_grpc_grpc_bazel_grpc_deps()

load("@stackb_rules_proto//python:deps.bzl", "python_grpc_compile")
python_grpc_compile()


git_repository(
    name = "graknlabs_grabl",
    remote = "https://github.com/graknlabs/grabl",
    commit = "ad79f87f869d25694fe11196e16be42a80e95d14",
)

git_repository(
    name="graknlabs_bazel_distribution",
    remote="https://github.com/graknlabs/bazel-distribution",
    commit="6298bcf46c0ae8b1b5c9bd5138e10be38a3a9bc3"
)

pip_import(
    name = "pypi_dependencies",
    requirements = "//:requirements.txt",
)
load("@pypi_dependencies//:requirements.bzl", "pip_install")
pip_install()

pip_import(
    name = "pypi_deployment_dependencies",
    requirements = "@graknlabs_bazel_distribution//pip:requirements.txt",
)
load("@pypi_deployment_dependencies//:requirements.bzl", "pip_install")
pip_install()


# ----- @graknlabs_grakn deps -----
git_repository(
 name="com_github_google_bazel_common",
 remote="https://github.com/graknlabs/bazel-common",
 commit="550f0490798a4e4b6c5ff8cac3b6f5c2a5e81e21",
)

load("@com_github_google_bazel_common//:workspace_defs.bzl", "google_common_workspace_rules")
google_common_workspace_rules()

load("@graknlabs_grakn_core//dependencies/maven:dependencies.bzl", maven_dependencies_for_build = "maven_dependencies")
maven_dependencies_for_build()

load("@graknlabs_grakn_core//dependencies/maven:dependencies.bzl", maven_dependencies_for_build = "maven_dependencies")
maven_dependencies_for_build()

# Load Graql dependencies
load("@graknlabs_grakn_core//dependencies/git:dependencies.bzl", "graknlabs_graql")
graknlabs_graql()

# Load client-java dependencies
load("@graknlabs_grakn_core//dependencies/git:dependencies.bzl", "graknlabs_client_java")
graknlabs_client_java()

# Load ANTLR dependencies for Bazel
load("@graknlabs_graql//dependencies/compilers:dependencies.bzl", "antlr_dependencies")
antlr_dependencies()

# Load ANTLR dependencies for ANTLR programs
load("@rules_antlr//antlr:deps.bzl", "antlr_dependencies")
antlr_dependencies()

load("@graknlabs_graql//dependencies/maven:dependencies.bzl", graql_dependencies = "maven_dependencies")
graql_dependencies()

load("@stackb_rules_proto//java:deps.bzl", "java_grpc_compile")
java_grpc_compile()

load("@graknlabs_grakn_core//dependencies/docker:dependencies.bzl", "docker_dependencies")
docker_dependencies()
