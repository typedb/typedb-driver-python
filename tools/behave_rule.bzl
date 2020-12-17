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
Private implementation of the rule py_cucumber_test.
"""
def _rule_implementation(ctx):

    # Store the path of the first feature file
    features_dir = ctx.files.feats[0].dirname

    # We want a test target so make it create an executable output.
    # https://bazel.build/versions/master/docs/skylark/rules.html#test-rules
    ctx.actions.write(
        # Access the executable output file using ctx.outputs.executable.
        output=ctx.outputs.executable,
        content="behave %s" % features_dir,
        is_executable=True
    )
    # The executable output is added automatically to this target.

    # Add the feature and step files for behave to the runfiles.
    # https://bazel.build/versions/master/docs/skylark/rules.html#runfiles
    return [DefaultInfo(
        # Create runfiles from the files specified in the data attribute.
        # The shell executable - the output of this rule - can use them at runtime.
        # It is also possible to define data_runfiles and default_runfiles.
        # However if runfiles is specified it's not possible to define the above
        # ones since runfiles sets them both.
        runfiles = ctx.runfiles(files = ctx.files.feats + ctx.files.steps + ctx.files.deps)
    )]

"""
An example documentation.

Args:
  name:
    A unique name for this rule.
  feats:
    Feature files used to run this target.
  steps:
    Files containing the mapping of feature steps to actual system API calls.
    Note: Since this rule implicitely uses the BDD tool "behave" they must
be in the "steps" folder (https://pythonhosted.org/behave/gherkin.html).
  deps:
    System to test.
"""
py_cucumber_test = rule(
    implementation=_rule_implementation,
    attrs={
        # Do not declare "name": It is added automatically.
        "feats": attr.label_list(allow_files=True),
        "steps": attr.label_list(allow_files=True),
        "deps": attr.label_list(mandatory=True,allow_empty=False),
    },
    test=True,
)
