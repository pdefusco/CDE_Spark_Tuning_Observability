# CDE Spark Tuning & Observability

## CDE Setup

1. Maximizing parallelism
  - Run Datagen to create data at scale; create hive partitions
  - Run job with different executor and core settings; Job will filter and group by data and write out report.
    - Use DA with different min max executor count
    - No DA
    - Set higher initial executors
    - Salt table and rerun and make performance improves

#### Instructions

```
cde credential create --name docker-creds-max-parallel \
                      --type docker-basic \
                      --docker-server hub.docker.com \
                      --docker-username pauldefusco

cde resource create --name dex-spark-dbldatagen-max-parallel \
                    --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-great-expectations-data-quality \
                    --image-engine spark3 \
                    --type custom-runtime-image

cde resource create --name max_parallel

cde resource upload --name max_parallel \
                    --local-path code/1_max_parallelism/datagen.py \
                    --local-path code/1_max_parallelism/utils.py \
                    --local-path code/1_max_parallelism/etl.py \
                    --local-path code/1_max_parallelism/parameters.conf

cde job create --name datagen-max-parallel \
               --type spark \
               --application-file datagen.py \
               --mount-1-resource max_parallel \
               --runtime-image-resource-name dex-spark-dbldatagen-max-parallel

cde job create --name etl \
               --type spark \
               --application-file etl.py \
               --mount-1-resource max_parallel

cde job run --name datagen-max-parallel \
            --executor-cores 10 \
            --executor-memory "5g" \
            --min-executors 1 \
            --max-executors 20 \
            --arg 100 \
            --arg 1000000000

cde job run --name datagen-max-parallel \
            --executor-cores 10 \
            --executor-memory "5g" \
            --min-executors 1 \
            --max-executors 20 \
            --arg 100 \
            --arg 100000 \
            --arg True


## Without DA:

cde job run --name etl \
            --executor-cores 10 \
            --executor-memory "8g" \
            --driver-cores 2 \
            --driver-memory "2g" \
            --conf spark.dynamicAllocation.enabled=False
            --conf=spark.executor.instances=5

## With DA:

cde job run --name etl \
            --executor-cores 10 \
            --executor-memory "8g" \
            --min-executors 1 \
            --max-executors 10 \
            --driver-cores 2 \
            --driver-memory "2g"

cde job run --name etl



## PRINT ALL SPARK CONFS

cde spark submit code/printSparkConfs.py

cde spark submit code/printSparkConfs.py --conf spark.dynamicAllocation.enabled=False --conf=spark.executor.instances=4

cde resource create --name sparkconfs_resource
cde resource upload --name sparkconfs_resource --local-path code/printSparkConfs.py

cde job create --name printAllConfs --type spark --application-file printSparkConfs.py --mount-1-resource sparkconfs_resource

cde job run --name printAllConfs

cde job run --name printAllConfs --conf spark.dynamicAllocation.enabled=False --conf=spark.executor.instances=4



```





2. Caching Right
  - Run Datagen to create data at scale; create hive partitions
  - Run same job as above without caching.
  - Rerun job with caching of data
  - Experiment with different driver and executor memory settings (force OOM and then increase memory available)

3. Broadcast Hash Join Tuning
  - Run Datagen to create data at scale
    - One large table, one small table
    - Join on ID
  - Increase 10 mb threshold and rerun test
  - Experiment with higher driver and executor memory settings (memory and also memory fraction)

4. Shuffle sort merge join
  - Run Datagen to create data at scale; create hive partitions
    - One dataset; data must be identically partitioned
    - Second dataset; data is not identically partitioned
  - Run join on key; observe results

5. Iceberg Merge Into Tuning:
  - Run Datagen to create data at scale; create hive partitions; migrate table to iceberg
  - Try MI with COW vs MOR
