**How to run?**

```
# 1) Execute framework (kafka, zookeeper, spark, postgres)
bash setup.sh

# 2) Create container to execute required tasks. you can use one of these command on another terminal from 1).
 - bash run_by_build.sh # run by docker build
 - bash run_by_pull.sh  # run by docker hub pull
```

**Requirements:**
* OS: macOS mojave 10.14.3
* Programing language:Python 3.6
* Processing framework: Spark 2.4.4
* Queuing technology: kafka 5.3.0 (zookeeper:3.4.9)
* Database technology: Postgres 11.5

