#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
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
from datetime import datetime
import sys, random, os, json, random, configparser

## CDE PROPERTIES
config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
demo=config.get("general","demo")

print("ALL PROVIDED ARGS")
print(sys.argv)

username = sys.argv[1]
print("Provided username: {}\n".format(username))

dbname = "CDE_DEMO_{0}_{1}".format(username, demo)

print("\nUsing DB Name: ", dbname)

skew = sys.argv[2]
print("SKEW: {}\n".format(skew))

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
#               LOAD DATA
#---------------------------------------------------

if skew == "True":
    bankingDf = spark.sql("SELECT * FROM {0}.BANKING_TRANSACTIONS_SKEWED_{1}".format(dbname, username))
    print("READING TABLE WITH SKEW\n")

elif skew == "False":
    bankTransactionsDf = dg.bankDataGen()
    print("READING TABLE WITHOUT SKEW\n")
    bankingDf = spark.sql("SELECT * FROM {0}.BANKING_TRANSACTIONS_{1}".format(dbname, username))

print("Print Number of Partitions: {}".format(bankingDf.rdd.getNumPartitions()))

#---------------------------------------------------
#               CAUSING THE SHUFFLE..
#---------------------------------------------------

# Narrow Transformation
selectDf = bankingDf.select('name', 'address', 'email', 'aba_routing',
                            'bank_country', 'transaction_amount',
                            'transaction_currency', 'credit_card_provider',
                            'event_type', 'event_ts', 'credit_card_balance',
                            'checking_acc_balance', 'checking_acc_2_balance',
                            'savings_acc_balance', 'savings_acc_2_balance')

# Wide Transformation
print("AVERAGE TRANSACTION AMOUNT BY COUNTRY")
byCountryDf = selectDf.groupBy('transaction_currency') \
                      .agg({'transaction_amount':'mean'})

#print("SORTING COUNTRIES BY AVG TRANSACTION")
#byCountryDf_sorted = byCountryDf.sort('avg(transaction_amount)', ascending=[True])

#print("AVERAGE TRANSACTION AMOUNT BY CURRENCY")
#byCurrencyDf = byCountryDf_sorted.groupBy('transaction_currency') \
#                            .agg({'credit_card_balance':'mean'})

print("SHOW AVERAGE TRANSACTION AMOUNT BY COUNTRY")
byCountryDf.show()
