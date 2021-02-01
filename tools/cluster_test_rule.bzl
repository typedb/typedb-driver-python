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

def _rule_implementation(ctx):
    """
    Implementation of the rule py_behave_test.
    """

    # Store the path of the test source file. It is recommended to only have one source file.
    test_src = ctx.files.srcs[0].path
#
#    # behave requires a 'steps' folder to exist in the test root directory.
#    steps_out_dir = ctx.files.feats[0].dirname + "/steps"

    grakn_cluster_distro = str(ctx.files.native_grakn_cluster_artifact[0].short_path)

    cmd = "set -e && GRAKN_ARCHIVE=%s" % grakn_cluster_distro
    cmd += """

           if test -d grakn_distribution; then
             echo Existing distribution detected. Cleaning.
             rm -rf grakn_distribution
           fi
           mkdir grakn_distribution
           echo Attempting to unarchive Grakn distribution from $GRAKN_ARCHIVE
           if [[ ${GRAKN_ARCHIVE: -7} == ".tar.gz" ]]; then
             tar -xf $GRAKN_ARCHIVE -C ./grakn_distribution
           else
             if [[ ${GRAKN_ARCHIVE: -4} == ".zip" ]]; then
               unzip -q $GRAKN_ARCHIVE -d ./grakn_distribution
             else
               echo Supplied artifact file was not in a recognised format. Only .tar.gz and .zip artifacts are acceptable.
               exit 1
             fi
           fi
           GRAKN=$(ls ./grakn_distribution)
           echo Successfully unarchived Grakn distribution. Creating 3 copies.
           cp -r grakn_distribution/$GRAKN/ 1 && cp -r grakn_distribution/$GRAKN/ 2 && cp -r grakn_distribution/$GRAKN/ 3
           echo Starting 3 Grakn servers.
           ./1/grakn server --data data --address=127.0.0.1:11729:11730 --peers=127.0.0.1:11729:11730,127.0.0.1:21729:21730,127.0.0.1:31729:31730 &
           ./2/grakn server --data data --address=127.0.0.1:21729:21730 --peers=127.0.0.1:11729:11730,127.0.0.1:21729:21730,127.0.0.1:31729:31730 &
           ./3/grakn server --data data --address=127.0.0.1:31729:31730 --peers=127.0.0.1:11729:11730,127.0.0.1:21729:21730,127.0.0.1:31729:31730 &
           sleep 8

           """

    cmd += "python3 -m unittest %s && export RESULT=0 || export RESULT=1" % test_src
    cmd += """
           echo Tests concluded with exit value $RESULT
           echo Stopping servers.
           kill $(jps | awk '/GraknServer/ {print $1}' | paste -sd " " -)
#           kill $(lsof -i :11730 | grep LISTEN | awk '{print $2}')
           exit $RESULT
           """

    # We want a test target so make it create an executable output.
    # https://bazel.build/versions/master/docs/skylark/rules.html#test-rules
    ctx.actions.write(
        # Access the executable output file using ctx.outputs.executable.
        output=ctx.outputs.executable,
        content=cmd,
        is_executable=True
    )

    # The executable output is added automatically to this target.

    # Add the feature and step files for behave to the runfiles.
    # https://bazel.build/versions/master/docs/skylark/rules.html#runfiles
    return [DefaultInfo(
        # The shell executable - the output of this rule - can use these files at runtime.
        runfiles = ctx.runfiles(files = ctx.files.srcs + ctx.files.deps + ctx.files.native_grakn_cluster_artifact)
    )]

"""
Documentation

Args:
  name:
    A unique name for this rule.
  feats:
    Feature files used to run this target.
  steps:
    Files containing the mapping of feature steps to actual system API calls.
  background:
    The environment.py file containing test setup and tear-down methods.
  deps:
    System to test.
"""
grakn_cluster_py_test = rule(
    implementation=_rule_implementation,
    attrs={
        "srcs": attr.label_list(mandatory=True,allow_empty=False,allow_files=True),
        "deps": attr.label_list(mandatory=True,allow_empty=False),
        "native_grakn_cluster_artifact": attr.label(mandatory=True)
    },
    test=True,
)
