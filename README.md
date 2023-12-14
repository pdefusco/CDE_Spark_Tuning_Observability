# CDE Spark Tuning & Observability

## CDE Setup

1. Maximizing parallelism
  - Run Datagen to create data at scale; create hive partitions
  - Run job with different executor and core settings; Job will filter and group by data and write out report.
    - Use DA with different min max executor count
    - No DA
    - Set higher initial executors
    - Salt table and rerun and see if performance improves

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
