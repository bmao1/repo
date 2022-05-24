#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# pip install delta-spark==1.2.1

import os
import boto3
from pyspark.sql import SparkSession
from delta import *
from delta.tables import *
import pyspark


session = boto3.Session()
credentials = session.get_credentials()

# setup spark environment, delta-spark==1.2.x required #######################
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 2g --packages "io.delta:delta-core_2.12:1.2.1,org.apache.hadoop:hadoop-aws:3.3.1" pyspark-shell'

builder = SparkSession.builder.appName("test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", credentials.access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", credentials.secret_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# shuffle partition is setup to optimize merge performance in small scale local deploy, avoiding multiple small files by limit parallelism
spark.conf.set("spark.sql.shuffle.partitions",1)

sc = pyspark.SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

##############################################################
# Step 1: check delta table 
deltaTable = DeltaTable.forPath(spark, "s3a://s3-for-athena-bintest2/data/delta/observation/")
fullHistoryDF = deltaTable.history()
fullHistoryDF.show()

# Step 2: two options to rollback
deltaTable.restoreToVersion(3) # restore table to older version
deltaTable.restoreToTimestamp('2022-05-20') # restore to a specific timestamp

# Create manifest file
deltaTable.generate("symlink_format_manifest")
