Sort of Vibe coded benchmark


### Setup benchmark


### Machine Setup (e.g if running on EC2)
On mounted storage on an EC2 (prevents EBS variance when solutions need to spill).
Commands.
```
mount_name=$(sudo lsblk | awk '
NR > 1 && $1 ~ /^nvme/ && $7 == "" {
    # Convert SIZE column to bytes for comparison
    size = $4;
    unit = substr(size, length(size));
    value = substr(size, 1, length(size)-1);
    if (unit == "G") { value *= 1024^3; }
    else if (unit == "T") { value *= 1024^4; }
    else if (unit == "M") { value *= 1024^2; }
    else if (unit == "K") { value *= 1024; }
    else { value *= 1; }

    # Keep track of the largest size
    if (value > max) {
        max = value;
        largest = $1;
    }
}
END { if (largest) print largest; else print "No match found"; }
')

sudo mkfs -t xfs /dev/$mount_name

sudo rm -rf $HOME/benchmark_mount
sudo mkdir $HOME/benchmark_mount
sudo mount /dev/$mount_name $HOME/benchmark_mount

# make clone of repo on mount
sudo mkdir $HOME/benchmark_mount/db-benchmark-metal
sudo chown -R ubuntu:ubuntu $HOME/benchmark_mount


git clone https://github.com/Tmonster/ $HOME/benchmark_mount/db-benchmark-metal
cd $HOME/benchmark_mount/db-benchmark-metal
```

This also sets up a place for spark to spill when needed


### Benchmark Setup  

Run installs with uv
```
uv synv --extra duckdb --extra spark
```

Generate all data for the scale factor you want. This will use duckdb to generate the base tables in parquet form, and the tpch gen library to generate the update & delete files. 
```
uv run python -m setup.generate_data --sf {{sf}} --refresh
```

#### Analytical Benchmark
If you have generating data, you can run the command below

```
uv run python3 run_benchmark.py --engine spark --benchmark analytical --keep-tables --namespace tpch_sf1 --sf 1
````

For the analytical benchmark, there are no updates or deletes that happen, so we want to keep the tables, hence `--keep-tables`

If you already have a namespace with the tables at the desired scale factor you can run 

```
uv run python3 run_benchmark.py --engine spark --benchmark analytical --keep-tables --skip-datagen --namespace tpch_sf1 --sf 1
````


#### Power Benchmark
You can run the power benchmark with the following script.
```
uv run python run_benchmark.py --engine spark --benchmark power --sf {{sf}}
```
This will provision a new namespace, upload the data at the scale factor, and run the power benchmark.


### Notes when benchmarking

##### Spark


I got this error during the Power Test.

```
  Running RF1 (insert refresh data)...
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
  RF1 done: 5.378s
  Starting query stream 0...
  stream 0 [01/22] q14: 10.726s
  stream 0 [02/22] q02: 5.737s
  stream 0 [03/22] q09: 11.086s
  stream 0 [04/22] q20: 7.895s
  stream 0 [05/22] q06: 5.746s
  stream 0 [06/22] q17: 9.866s
  stream 0 [07/22] q18: 10.063s
  stream 0 [08/22] q08: 9.144s
  stream 0 [09/22] q21: 17.627s
  stream 0 [10/22] q13: 4.926s
  stream 0 [11/22] q03: 12.626s
  stream 0 [12/22] q22: 2.355s
  stream 0 [13/22] q16: 4.189s
  stream 0 [14/22] q04: 5.150s
  stream 0 [15/22] q11: 4.804s
  stream 0 [16/22] q15: 11.379s
  stream 0 [17/22] q01: 5.421s
  stream 0 [18/22] q10: 8.777s
  stream 0 [19/22] q19: 11.023s
  stream 0 [20/22] q05: 9.632s
  stream 0 [21/22] q07: 8.765s
  stream 0 [22/22] q12: 8.450s
  Running RF2 (delete refresh data)...
26/04/16 14:24:28 ERROR ReplaceDataExec: Data source write support IcebergBatchWrite(table=s3tablesbucket.bench_b2d74d02.lineitem, format=PARQUET) is aborting.
26/04/16 14:24:28 ERROR ReplaceDataExec: Data source write support IcebergBatchWrite(table=s3tablesbucket.bench_b2d74d02.lineitem, format=PARQUET) aborted.
  RF2 done: ERROR: An error occurred while calling o48.sql.
: org.apache.iceberg.exceptions.ValidationException: Missing required files to delete: s3://7c4db751-8784-48c4-pkusg9wff4xghasdoz1h5gbzt4x5heuc1b--table-s3/data/019d963a-84f0-7cfd-abf4-1e4d7da205e7.parquet
	at org.apache.iceberg.exceptions.ValidationException.check(ValidationException.java:49)
	at org.apache.iceberg.ManifestFilterManager.validateRequiredDeletes(ManifestFilterManager.java:278)
	at org.apache.iceberg.ManifestFilterManager.filterManifests(ManifestFilterManager.java:228)
	at org.apache.iceberg.MergingSnapshotProducer.apply(MergingSnapshotProducer.java:925)
	at org.apache.iceberg.BaseOverwriteFiles.apply(BaseOverwriteFiles.java:31)
	at org.apache.iceberg.SnapshotProducer.apply(SnapshotProducer.java:261)
	at org.apache.iceberg.BaseOverwriteFiles.apply(BaseOverwriteFiles.java:31)
	at org.apache.iceberg.SnapshotProducer.lambda$commit$2(SnapshotProducer.java:440)
	at org.apache.iceberg.util.Tasks$Builder.runTaskWithRetry(Tasks.java:413)
	at org.apache.iceberg.util.Tasks$Builder.runSingleThreaded(Tasks.java:219)
	at org.apache.iceberg.util.Tasks$Builder.run(Tasks.java:203)
	at org.apache.iceberg.util.Tasks$Builder.run(Tasks.java:196)
	at org.apache.iceberg.SnapshotProducer.commit(SnapshotProducer.java:438)
	at org.apache.iceberg.BaseOverwriteFiles.commit(BaseOverwriteFiles.java:31)
	at org.apache.iceberg.spark.source.SparkWrite.commitOperation(SparkWrite.java:238)
	at org.apache.iceberg.spark.source.SparkWrite$CopyOnWriteOperation.commitWithSerializableIsolation(SparkWrite.java:490)
	at org.apache.iceberg.spark.source.SparkWrite$CopyOnWriteOperation.commit(SparkWrite.java:453)
```

I have not tried to target the issue, but it seems like spark or S3Tables are not communicating correctly.
This has also prompted me to make sure spark only performs a merge-on-read deletion strategy.


At one point I triggered the following. Can't quiet remember the setup. But this goes to show how difficult setting up spark can be.
```
26/04/15 13:27:35 WARN GarbageCollectionMetrics: To enable non-built-in garbage collector(s) List(G1 Concurrent GC), users should configure it(them) to spark.eventLog.gcMetrics.youngGenerationGarbageCollectors or spark.eventLog.gcMetrics.oldGenerationGarbageCollectors

#
# A fatal error has been detected by the Java Runtime Environment:
#
#  SIGSEGV (0xb) at pc=0x0000f3d69225a018, pid=5686, tid=5883
#
# JRE version: OpenJDK Runtime Environment (21.0.10+7) (build 21.0.10+7-Ubuntu-124.04)
# Java VM: OpenJDK 64-Bit Server VM (21.0.10+7-Ubuntu-124.04, mixed mode, sharing, tiered, compressed oops, compressed class ptrs, g1 gc, linux-aarch64)
# Problematic frame:
# V  [libjvm.so+0xc3a018]  oopDesc::metadata_field(int) const+0x14
#
# Core dump will be written. Default location: Core dumps may be processed with "/usr/share/apport/apport -p%p -s%s -c%c -d%d -P%P -u%u -g%g -F%F -- %E" (or dumping to /home/ubuntu/IcebergEngineBenchmarking/core.5686)
#
# An error report file with more information is saved as:
# /home/ubuntu/IcebergEngineBenchmarking/hs_err_pid5686.log
[23.629s][warning][os] Loading hsdis library failed
#
# If you would like to submit a bug report, please visit:
#   https://bugs.launchpad.net/ubuntu/+source/openjdk-21
#
```



I need to tune the memory size for Java. 
On an EC2 