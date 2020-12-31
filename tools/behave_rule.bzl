# =============================================================================
# Description: Adds a test rule for the BDD tool behave to the bazel rule set.
# Knowledge:
# * https://bazel.build/versions/master/docs/skylark/cookbook.html
# * https://bazel.build/versions/master/docs/skylark/rules.html
# * https://bazel.build/versions/master/docs/skylark/lib/ctx.html
# * http://pythonhosted.org/behave/gherkin.html
# =============================================================================

# TODO: this should probably live in graknlabs_dependencies
"""
Implementation of the rule py_behave_test.
"""
def _rule_implementation(ctx):

    # Store the path of the first feature file. It is recommended to only have one feature file.
    feats_dir = ctx.files.feats[0].dirname

    # behave requires a 'steps' folder to exist in the test root directory.
    steps_out_dir = ctx.files.feats[0].dirname + "/steps"

    # TODO: If two step files have the same name, we should rename the second one to prevent conflict
    cmd = "cp %s %s" % (ctx.files.background[0].path, feats_dir)
    cmd += " && mkdir " + steps_out_dir + " && "
    cmd += " && ".join(["cp %s %s" % (step_file.path, steps_out_dir) for step_file in ctx.files.steps])
    cmd += " && behave %s" % feats_dir

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
        runfiles = ctx.runfiles(files = ctx.files.feats + ctx.files.background + ctx.files.steps + ctx.files.deps)
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
    },
    test=True,
)
