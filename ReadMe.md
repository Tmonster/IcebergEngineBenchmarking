Sort of Vibe coded benchmark


### Setup benchmark

#### Generate Data
First you need to generate data. This generates parquet files locally. Useful when running multiple power runs since you need a fresh install of data, generating it locally helps.

```
uv run python3 -m setup.generate_data --sf 1
```


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


git clone $(git remote get-url origin) $HOME/benchmark_mount/db-benchmark-metal
cd $HOME/benchmark_mount/db-benchmark-metal
```

This also sets up a place for spark to spill when needed


### Benchmark Setup  

Run installs with uv
```
uv synv --extra duckdb --extra spark
```

#### Analytical Benchmark
After generating data, you can run 

```
uv run python3 run_benchmark.py --engine spark --benchmark analytical --keep-tables --namespace tpch_sf1 --sf 1
````

#### Power Benchmark




### Notes when benchmarking

##### Spark

At one point I triggered the following

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