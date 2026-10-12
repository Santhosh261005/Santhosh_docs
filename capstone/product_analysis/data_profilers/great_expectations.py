"""Optional Great Expectations integration; other profilers do not import GX."""

from copy import deepcopy
import os
import uuid
from ._audit import write_audit
from pyspark import StorageLevel


def profile_expectations(
    df,
    expectations,
    *,
    dataset_name=None,
    run_id=None,
    audit_table="datalake.operations.expectation_profiles"
):
    """Run an actual GX checkpoint and audit summary plus individual rules.

    A failed expectation is an audited quality finding, not an exception.
    Configuration, engine and audit write failures still raise exceptions.
    """
    if df.isStreaming:
        raise ValueError("Use a static foreachBatch DataFrame.")
    os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")
    import great_expectations as gx

    run_id = run_id or str(uuid.uuid4())
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_spark(name="spark_profiles")
    asset = source.add_dataframe_asset(name="records")
    batch = asset.add_batch_definition_whole_dataframe("whole_dataframe")
    suite = context.suites.add(gx.ExpectationSuite(name="student_rules"))
    for expectation in expectations:
        # GX assigns a suite-owned ID. Never mutate caller-owned rule objects.
        rule = deepcopy(expectation)
        rule.id = None
        suite.add_expectation(rule)
    definition = context.validation_definitions.add(
        gx.ValidationDefinition(name="profile", data=batch, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="profile_checkpoint",
            validation_definitions=[definition],
            result_format="SUMMARY",
        )
    )
    owns_cache = df.storageLevel == StorageLevel.NONE
    if owns_cache:
        df.cache()
    try:
        df.count()
        result = checkpoint.run(batch_parameters={"dataframe": df})
        base = {
            "run_id": run_id,
            "dataset_name": dataset_name,
            "profile_kind": "GREAT_EXPECTATIONS",
        }
        rows = [
            dict(
                base,
                status="PASSED" if result.success else "FAILED",
                details={"checkpoint": result.name, "success": bool(result.success)},
            )
        ]
        for validation in result.run_results.values():
            for rule in validation.results:
                rows.append(
                    dict(
                        base,
                        status="RULE_PASSED" if rule.success else "RULE_FAILED",
                        details=rule.to_json_dict(),
                    )
                )
        write_audit(df.sparkSession, rows, audit_table)
    finally:
        if owns_cache:
            df.unpersist()
    return None
