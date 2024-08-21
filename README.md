# CDE Spark Tuning & Observability

# Spark Transformations and Actions Refresher

Transformations:
  - used for transforming data
  - Can be divided into:
    - Narrow transformations
      - Performed in parallel on data partitions
      - Examples: select, filter, withColumn, drop
    - Wide transformations
      - Performed after grouping data from multiple partitions
      - Examples: groupby, join, cube, rollup, agg

  - Actions
    - Used to trigger some work (i.e. job)
    - Example: read, write, collect, take, count


## Maximizing Parallelism: Lab 1

In this lab we will demonstrate some basic Spark parallelism concepts with CDE.

In our first application we will analyze 1b banking transactions (153gb) and perform two simple transformations: select columns (narrow) and groupby (wide). We will run this twice with Static and Dynamic allocation and observe the difference in the Spark UI.

#### Instructions

0. Setup

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
                    --local-path code/1_max_parallelism/etl_1.py \
                    --local-path code/1_max_parallelism/parameters.conf

cde job create --name datagen-max-parallel \
               --type spark \
               --application-file datagen.py \
               --mount-1-resource max_parallel \
               --runtime-image-resource-name dex-spark-dbldatagen-max-parallel

cde job run --name datagen-max-parallel \
           --executor-cores 5 \
           --executor-memory "5g" \
           --min-executors 1 \
           --max-executors 20 \
           --arg 100 \
           --arg 1000000 \
           --arg pdefusco_012523 \
           --arg True

cde job run --name datagen-max-parallel \
          --executor-cores 5 \
          --executor-memory "10g" \
          --min-executors 1 \
          --max-executors 40 \
          --arg 100 \
          --arg 100000000 \
          --arg pdefusco_dec_17_23 \
          --arg False

```

1. Experiment: ETL Job with one Wide Transformation (GroupBy)

```
#1B row table params:
#username: pdefusco_dec_17_23

cde job create --name etl1 \
               --type spark \
               --application-file etl_1.py \
               --mount-1-resource max_parallel

```

Run with Static Allocation:

```
cde job run --name etl1 \
            --executor-cores 5 \
            --executor-memory "10g" \
            --driver-cores 2 \
            --driver-memory "2g" \
            --conf spark.dynamicAllocation.enabled=False \
            --conf=spark.executor.instances=10 \
            --arg pdefusco_012523
```

Spark UI Key Takeaways:

1. Jobs Tab: Notice that only two Jobs were created.
  - The first Spark Job reflects reading from the table
  - The second Spark Job reflects the action triggered by the show command
2. Stages Tab: Notice that three stages were created:
  - The first stage (stage 0) is part of the first job and it executed a File Scan and MapPartitions functions.
  - The second stage (stage 1) was skipped
  - The third stage (stage 2) is part of the second job and it executed a AQE Shuffle Read and MapPartitions functions.
3. Executors Tab: notice that five entries are listed. One driver and four executors.
  - Each executor has 10 cores and 5.8 gb of memory. Per Spark memory settings, storage memory is 40% of total memory - 300Mb overhead.
  - Notice the amount of data shown in executors (2gb ca.) is much lower than the actual data on disk (153gb). The data on executors is compressed.
4. Storage tab: this is empty as we didn't cache any data.
5. SQL/Dataframe tab: despite the shuffle on 1b records (153 gb), the job run (id 37442) only took 3.6 minutes.
  - mappartitions allows Spark to perform large aggregate queries with ease.
  - data on executors is compressed.

Run with Dynamic Allocation:

```
cde job run --name etl1 \
            --executor-cores 5 \
            --executor-memory "8g" \
            --min-executors 1 \
            --max-executors 10 \
            --driver-cores 2 \
            --driver-memory "2g" \
            --arg pdefusco_dec_17_23            
```

Spark UI Key Takeaways:

1. Jobs Tab: Notice that only two Jobs were created.
  - The first Spark Job reflects reading from the table
  - The second Spark Job reflects the action triggered by the show command
2. Stages Tab: Notice that three stages were created:
  - The first stage (stage 0) is part of the first job and it executed a File Scan and MapPartitions functions.
  - The second stage (stage 1) was skipped
  - The third stage (stage 2) is part of the second job and it executed a AQE Shuffle Read and MapPartitions functions.
3. Executors Tab: only one executor was spun up this time. We configured Spark DA to choose between 1 and 10 executors.
  - All data and tasks were processed by a single executor.
4. Storage tab: this is empty again as we didn't cache any data.
5. With DA the job run (id 37443) took even less (2 mins).
  - Only one executor was spun up unlike in the prior example where four were used. All 1235 tasks executed on the same executor.
  - Dynamic Allocation allows you to rely on Spark for selecting the ideal number of executors that are needed.

Comparing the Spark UI for both runs:

* The data, application code, and generated plans were identical.
* However one run took almost half the time as the other.
* By keeping data in one executor the Shuffle time was dramatically reduced.
* In both cases the number of partitions during the exchange was 200. This is the default setting in Spark which we have not (yet) changed.
* In both cases, Spark resources allocated are heavily underutilized.

## Maximizing Parallelism: Lab 2

In this lab we will revisit the prior example but perform more complex processing steps. We will build a use case that requires more of the allocated Spark resources. Along the way we will explore more Spark features for tuning performance including Memory management and other useful properties.

```
cde resource upload --name max_parallel \
                    --local-path code/1_max_parallelism/etl_2.py

cde job create --name etl2 \
               --type spark \
               --application-file etl_2.py \
               --mount-1-resource max_parallel

cde job run --name etl2 \
           --executor-cores 5 \
           --executor-memory "8g" \
           --min-executors 1 \
           --max-executors 10 \
           --driver-cores 2 \
           --driver-memory "2g" \
           --arg pdefusco_dec_17_23
```



```
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


3. Broadcast Hash Join Tuning


4. Shuffle sort merge join


5. Iceberg Merge Into Tuning:
  - Run Datagen to create data at scale; create hive partitions; migrate table to iceberg
  - Try MI with COW vs MOR
