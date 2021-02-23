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

# =============================================================================
# Description: Adds a test rule for the BDD tool behave to the bazel rule set.
# Knowledge:
# * https://bazel.build/versions/master/docs/skylark/cookbook.html
# * https://bazel.build/versions/master/docs/skylark/rules.html
# * https://bazel.build/versions/master/docs/skylark/lib/ctx.html
# * http://pythonhosted.org/behave/gherkin.html
# =============================================================================

# TODO: We should split this rule up into grakn_py_behave_test and py_behave_test.
def _rule_implementation(ctx):
    """
    Implementation of the rule py_behave_test.
    """

    # Store the path of the first feature file. It is recommended to only have one feature file.
    feats_dir = ctx.files.feats[0].dirname

    # behave requires a 'steps' folder to exist in the test root directory.
    steps_out_dir = ctx.files.feats[0].dirname + "/steps"

    grakn_distro = str(ctx.files.native_grakn_artifact[0].short_path)

    cmd = "set -e && GRAKN_DISTRO=%s" % grakn_distro
    cmd += """

           if test -d grakn_distribution; then
             echo Existing distribution detected. Cleaning.
             rm -rf grakn_distribution
           fi
           mkdir grakn_distribution
           echo Attempting to unarchive Grakn distribution from $GRAKN_DISTRO
           if [[ ${GRAKN_DISTRO: -7} == ".tar.gz" ]]; then
             tar -xf $GRAKN_DISTRO -C ./grakn_distribution
           else
             if [[ ${GRAKN_DISTRO: -4} == ".zip" ]]; then
               unzip -q $GRAKN_DISTRO -d ./grakn_distribution
             else
               echo Supplied artifact file was not in a recognised format. Only .tar.gz and .zip artifacts are acceptable.
               exit 1
             fi
           fi
           DIRECTORY=$(ls ./grakn_distribution)

           if [[ $GRAKN_DISTRO == *"cluster"* ]]; then
             PRODUCT=Cluster
           else
             PRODUCT=Core
           fi

           echo Successfully unarchived Grakn $PRODUCT distribution.

           RND=20001
           while [ $RND -gt 20000 ]  # Guarantee fair distribution of random ports
           do
             RND=$RANDOM
           done
           PORT=$((40000 + $RND))

           echo Starting Grakn $PRODUCT Server.
           mkdir ./grakn_distribution/"$DIRECTORY"/grakn_test
           if [[ $PRODUCT == "Core" ]]; then
             ./grakn_distribution/"$DIRECTORY"/grakn server --port $PORT --data grakn_test &
           else
             ./grakn_distribution/"$DIRECTORY"/grakn server --address "127.0.0.1:$PORT:$(($PORT+1))" --data grakn_test &
           fi

           POLL_INTERVAL_SECS=0.5
           MAX_RETRIES=60
           RETRY_NUM=0
           while [[ $RETRY_NUM -lt $MAX_RETRIES ]]; do
             RETRY_NUM=$(($RETRY_NUM + 1))
             if [[ $(($RETRY_NUM % 4)) -eq 0 ]]; then
               echo Waiting for Grakn $PRODUCT server to start \($(($RETRY_NUM / 2))s\)...
             fi
             lsof -i :$PORT && STARTED=1 || STARTED=0
             if [[ $STARTED -eq 1 ]]; then
               break
             fi
             sleep $POLL_INTERVAL_SECS
           done
           if [[ $STARTED -eq 0 ]]; then
             echo Failed to start Grakn $PRODUCT server
             exit 1
           fi
           echo Grakn $PRODUCT database server started

           """
    # TODO: If two step files have the same name, we should rename the second one to prevent conflict
    cmd += "cp %s %s" % (ctx.files.background[0].path, feats_dir)
    cmd += " && rm -rf " + steps_out_dir
    cmd += " && mkdir " + steps_out_dir + " && "
    cmd += " && ".join(["cp %s %s" % (step_file.path, steps_out_dir) for step_file in ctx.files.steps])
    cmd += " && behave %s --no-capture -D port=$PORT && export RESULT=0 || export RESULT=1" % feats_dir
    cmd += """
           echo Tests concluded with exit value $RESULT
           echo Stopping server.
           kill $(lsof -i :$PORT | awk '/java/ {print $2}')
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
        runfiles = ctx.runfiles(files = ctx.files.feats + ctx.files.background + ctx.files.steps + ctx.files.deps + ctx.files.native_grakn_artifact)
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
py_behave_test = rule(
    implementation=_rule_implementation,
    attrs={
        # Do not declare "name": It is added automatically.
        "feats": attr.label_list(mandatory=True,allow_empty=False,allow_files=True),
        "steps": attr.label_list(mandatory=True,allow_empty=False),
        "background": attr.label_list(mandatory=True,allow_empty=False),
        "deps": attr.label_list(mandatory=True,allow_empty=False),
        "native_grakn_artifact": attr.label(mandatory=True)
    },
    test=True,
)


def grakn_behaviour_py_test(
        name,
        background_core,
        background_cluster,
        native_grakn_artifact_core,
        native_grakn_artifact_cluster,
        **kwargs):

    py_behave_test(
        name = name + "-core",
        background = background_core,
        native_grakn_artifact = native_grakn_artifact_core,
        **kwargs,
    )

    py_behave_test(
        name = name + "-cluster",
        background = background_cluster,
        native_grakn_artifact = native_grakn_artifact_cluster,
        **kwargs,
    )
