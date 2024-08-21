#****************************************************************************
# (C) Cloudera, Inc. 2020-2024
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from utils import *
from datetime import datetime
import sys, random, os, json, random, configparser

## CDE PROPERTIES
config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
demo=config.get("general","demo")
#username=config.get("general","username")
print("ALL PROVIDED ARGS")
print(sys.argv)

datagen_partitions = int(sys.argv[1])
datagen_rows = int(sys.argv[2])
username = sys.argv[3]
skew = sys.argv[4]

print("\nRunning as Username: ", username)

dbname = "CDE_DEMO_{0}_{1}".format(username, demo)

print("\nUsing DB Name: ", dbname)

print("Number of Datagen Partitions: {}\n".format(datagen_partitions))
print("Number of Rows Requested: {}\n".format(datagen_rows))
print("Skew Option Selected: {}\n".format(skew))
print("Provided username: {}\n".format(username))

#---------------------------------------------------
#               CREATE SPARK SESSION WITH ICEBERG
#---------------------------------------------------

spark = SparkSession \
    .builder \
    .appName("BANK TRANSACTIONS LOAD") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .getOrCreate()

#---------------------------------------------------
#       SQL CLEANUP: DATABASES, TABLES, VIEWS
#---------------------------------------------------

print("JOB STARTED...")
#spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(dbname))

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(dbname))

print("SHOW DATABASES LIKE '{}'".format(dbname))
spark.sql("SHOW DATABASES LIKE '{}'".format(dbname)).show()
print("\n")

#---------------------------------------------------
#               CREATE BATCH DATA
#---------------------------------------------------

print("CREATING BANKING TRANSACTIONS\n")

dg = BankDataGen(spark, username, datagen_partitions, datagen_rows)

#bankTransactionsDf.writeTo("{0}.BANKING_TRANSACTIONS_{1}".format(dbname, username))\
#    .using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()

if skew == "True":
    bankTransactionsDfLeft = dg.bankDataGenSkewed()
    print("CREATING LEFT TABLE WITH SKEW\n")
    bankTransactionsDfLeft.write.mode("overwrite").\
                        saveAsTable("{0}.BANKING_TRANSACTIONS_SKEWED_LEFT_{1}".format(dbname, username), format="parquet")

    bankTransactionsDfRight = dg.bankDataGenSkewed()
    print("CREATING RIGHT TABLE WITH SKEW\n")
    bankTransactionsDfRight.write.mode("overwrite").\
                        saveAsTable("{0}.BANKING_TRANSACTIONS_SKEWED_RIGHT_{1}".format(dbname, username), format="parquet")


elif skew == "False":
    bankTransactionsDfLeft = dg.bankDataGen()
    print("CREATING LEFT TABLE WITHOUT SKEW\n")
    bankTransactionsDfLeft.write.mode("overwrite").\
                        saveAsTable("{0}.BANKING_TRANSACTIONS_RIGHT_{1}".format(dbname, username), format="parquet")

    bankTransactionsDfRight = dg.bankDataGen()
    print("CREATING RIGHT TABLE WITHOUT SKEW\n")
    bankTransactionsDfRight.write.mode("overwrite").\
                        saveAsTable("{0}.BANKING_TRANSACTIONS_RIGHT_{1}".format(dbname, username), format="parquet")

print("BATCH LOAD JOB COMPLETED\n")
