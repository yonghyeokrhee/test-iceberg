# Iceberg Partition Overwrite Demo

This repository demonstrates how overlapping micro-batches can be applied to an
Apache Iceberg table that is partitioned by `order_date`. You can start with a
pure-Python mock that illustrates the overwrite logic without requiring Spark,
or optionally run a Spark job against a local Iceberg catalog.

## Quick start (no Spark required)

```bash
python scripts/mock_iceberg_overwrite_demo.py
```

The script keeps an in-memory representation of the table, overwriting the
partitions touched by each batch and printing the resulting table contents along
with mock snapshot metadata. Use this to reason about how near-real-time updates
would behave before wiring up an actual engine.

## Optional: Run with Spark + Iceberg

You can validate everything against a real Iceberg table using the Spark job at
`scripts/run_iceberg_overwrite_demo.py`. This path depends on Java and Spark but
is fully automated through `uv`:

```bash
# 1. Create the virtual environment
uv venv .venv

# 2. Install runtime + test dependencies
source .venv/bin/activate
uv pip install '.[test]'
# ensure the runtime matches the bundled Iceberg jar
uv pip install 'pyspark>=3.5,<3.6'

# 3. (macOS example) provide a JDK if Java is not already available
curl -L -o vendor/corretto17.tar.gz \
  https://corretto.aws/downloads/latest/amazon-corretto-17-x64-macos-jdk.tar.gz
tar -xzf vendor/corretto17.tar.gz -C vendor
export JAVA_HOME="$(pwd)/vendor/amazon-corretto-17.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

# 4. Run the integration test to exercise the Spark job
pytest

# 5. Or submit the job manually with your JVM tooling
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  scripts/run_iceberg_overwrite_demo.py
```

The test automatically skips if the sandbox cannot start a PySpark gateway (for
example when socket access is restricted).

## Scenario overview

1. Create or mock an Iceberg table named `order_history` partitioned by
   `order_date`.
2. Replay three micro-batches that intentionally overlap the same partition to
   mimic near-real-time corrections and late-arriving data.
3. Overwrite only the touched partitions each time, observing how the final
   state and snapshot history evolves.

Feel free to adjust the mock data in either script to mirror your own ingestion
patterns or extend the simulation with additional partitions and late updates.

## Register the Iceberg table in AWS Glue

Once you are happy with the local table you can publish it to the AWS Glue Data
Catalog so Athena, EMR, or Glue jobs can query it.

1. Rewrite the metadata so every Iceberg file path points to your S3 prefix:

   ```bash
   python scripts/relocate_iceberg_table.py \
     --target-base s3://iceberg-test-database/iceberg_db.db/order_history
   ```

   This updates the metadata JSON plus every manifest so Athena/Glue do not try
   to read the original local filesystem paths.

2. Sync the rewritten warehouse to S3 (update the bucket path as needed):

   ```bash
   aws s3 sync warehouse/order_history s3://iceberg-test-database/iceberg_db.db/order_history \
     --profile yongs-dev --region ap-northeast-2
   ```

Alternatively, rerun `scripts/run_iceberg_overwrite_demo.py` with
`ICEBERG_WAREHOUSE_DIR` pointing at your target S3 URI so Spark writes there
directly (after you make the script S3-aware).

3. Install the boto3 dependency (`uv pip install .` will pick it up from
   `pyproject.toml`), then invoke the new helper to create/update the Glue
   database and table definition:

   ```bash
   python scripts/register_glue_table.py \
     --profile yongs-dev \
     --region ap-northeast-2 \
     --database iceberg_demo \
     --table order_history \
     --table-location s3://my-bucket/warehouse/order_history \
     --metadata-json warehouse/order_history/metadata/v4.metadata.json \
     --metadata-location s3://my-bucket/warehouse/order_history/metadata/v4.metadata.json \
     --replace
   ```

   The script inspects the Iceberg metadata JSON to infer the schema and
   partition keys, then calls `glue.create_table` (or `glue.update_table` when
   `--replace` is supplied). Use `--catalog-id` if you need to target a
   different AWS account and `--database-location` to override the default
   `LocationUri`.

4. Verify the registration:

   ```bash
   aws glue get-table --database-name iceberg_demo --name order_history \
     --profile yongs-dev --region ap-northeast-2
   ```

The Glue entry declares the table as Iceberg (`table_type=ICEBERG`) and points to
the metadata file you uploaded to S3, enabling downstream AWS analytics
services to query the same data your local Spark job produced.

## Simulate concurrent overwrite conflicts

To see how Iceberg protects against overlapping writers, run the concurrent demo
which spins up two writers that try to overwrite the same `order_date`
partition. One writer should succeed while the other fails with a validation
error, proving that Iceberg enforces optimistic locking.

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  scripts/run_concurrent_overwrite_demo.py \
    --warmup-batches 1 \
    --mode processes \
    --process-start-delay 5 \
    --row-repeat 200
```

The script loads the initial batch, then launches two Spark workers that wait
for a shared start time before overwriting the `2024-07-01` partition. With
`commit.retry.num-retries` forced to 0 one writer succeeds while the other
fails with a `CommitFailedException` because the metadata version it tried to
publish was already created by the first writer. Tune `--row-repeat`,
`--process-start-delay`, or drop down to the default threaded mode to explore
different contention patterns.
