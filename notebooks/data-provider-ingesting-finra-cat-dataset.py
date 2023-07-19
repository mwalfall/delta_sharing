# Databricks notebook source
# MAGIC %md
# MAGIC # FINRA CAT NMS Reference Data
# MAGIC The [FINRA CAT Reportable Equity Securities Symbol Master](https://www.catnmsplan.com/reference-data) dataset, lists all stocks and equity securities traded across the U.S. National Market System (NMS).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish Schedule
# MAGIC Symbol and member reference data is published on business days. It is not published on weekends or holidays. 
# MAGIC
# MAGIC | File | Timing |
# MAGIC |------|--------|
# MAGIC |CAT Reportable Equity Securities Symbol Master SOD|6 a.m. ET|
# MAGIC |CAT Reportable Equity Securities Symbol Master - Intraday|10:30 a.m. ET and approximately every 2 hours until EOD file is published|
# MAGIC |CAT Reportable Equity Securities Symbol Master EOD|6 p.m. ET|
# MAGIC |CAT Reportable Options Securities Symbol Master SOD|6 a.m. ET|
# MAGIC |CAT Reportable Options Securities Symbol Master – Intraday|10:30 a.m. ET and approximately every 2 hours until EOD file is published|
# MAGIC |CAT Reportable Options Securities Symbol Master EOD|8 p.m. ET|
# MAGIC |Member ID (IMID) List|6 a.m. ET|
# MAGIC |Member ID (IMID) Conflicts List|6 a.m. ET|

# COMMAND ----------

# MAGIC %md
# MAGIC ## Downloading the FINRA CAT Equity Securities Symbol File
# MAGIC A simple Python task can download the FINRA CAT equity symbol file at the start of the trading day.

# COMMAND ----------

# MAGIC %pip install holidays pytz

# COMMAND ----------

finraCATReferenceDataURLMap = {
  "CAT Reportable Equity Securities Symbol Master SOD": "https://files.catnmsplan.com/symbol-master/FINRACATReportableEquitySecurities_SOD.txt",
  "CAT Reportable Equity Securities Symbol Master EOD": "https://files.catnmsplan.com/symbol-master/FINRACATReportableEquitySecurities_EOD.txt",
  "CAT Reportable Equity Securities Symbol Master - Intraday Updates": "https://files.catnmsplan.com/symbol-master/FINRACATReportableEquitySecurities_Intraday.txt",
  "CAT Reportable Options Securities Symbol Master SOD": "https://files.catnmsplan.com/symbol-master/CATReportableOptionsSymbolMaster_SOD.txt",
  "CAT Reportable Options Securities Symbol Master EOD": "https://files.catnmsplan.com/symbol-master/CATReportableOptionsSymbolMaster_EOD.txt",
  "CAT Reportable Options Securities Symbol Master - Intraday Updates": "https://files.catnmsplan.com/symbol-master/CATReportableOptionsSymbolMaster_Intraday.txt",
  "Member ID (IMID) List": "https://files.catnmsplan.com/firm-data/IMID_Daily_List.txt",
  "Member ID (IMID) Conflicts List": "https://files.catnmsplan.com/firm-data/IMID_Conflict_List.txt"
}

# COMMAND ----------

referenceDataURLLookupKeys = list(finraCATReferenceDataURLMap.keys())
dbutils.widgets.dropdown("catReferenceDataType", next(iter(referenceDataURLLookupKeys)), referenceDataURLLookupKeys, "Reference Data Type")
dbutils.widgets.text("dbfsFileOutputWriteDirectory", "/tmp/finracatref", "DBFS File Output Write Directory")

# COMMAND ----------

import datetime
import holidays
import os
import pytz
import requests
import tempfile
from urllib.parse import unquote, urlparse

catReferenceDataType = dbutils.widgets.get("catReferenceDataType")

# Check to see if the notebooks are running on the right days
easternTime = pytz.timezone("US/Eastern")
today = datetime.datetime.now(easternTime)
weekday = today.weekday()
# If this runs on weekend then cancel the run
if weekday >= 5:
  dbutils.notebook.exit(catReferenceDataType + " is meant to only run on weekdays.")

# If this is a US holiday then cancel the run
us_holidays = holidays.US()
if today in us_holidays:
  dbutils.notebook.exit(catReferenceDataType + " is not meant to run on US holidays.")

# Taken from: https://stackoverflow.com/a/10748024
def time_in_range(start, end, x):
  """Return true if x is in the range [start, end]"""
  if start <= end:
    return start <= x <= end
  else:
    return start <= x or x <= end

def is_intraday_data_job(catReferenceDataType):
  return 'intraday' in catReferenceDataType.lower()
  
currentTime = today.time()
intradayStart = datetime.time(10, 25, 00)
intradayEnd = datetime.time(18, 00, 00)
if is_intraday_data_job and not time_in_range(intradayStart, intradayEnd, currentTime):
  dbutils.notebook.exit(catReferenceDataType + " is not meant to run outside of intraday hours.")
  
catReferenceDataURL = finraCATReferenceDataURLMap[catReferenceDataType]

request = requests.get(catReferenceDataURL, stream=True, allow_redirects=True)
fileNameFromURL = os.path.basename(unquote(urlparse(catReferenceDataURL).path))
dbfsFileOutputWriteDirectory = dbutils.widgets.get("dbfsFileOutputWriteDirectory")
localFilePath = "/dbfs" + dbfsFileOutputWriteDirectory + fileNameFromURL
print(f"Writing {catReferenceDataType} file to location: {localFilePath}")
with open(localFilePath, "wb") as binary_file:
  for chunk in request.iter_content(chunk_size=2048):
    if chunk:
      binary_file.write(chunk)
      binary_file.flush()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest CAT File into a Bronze Table
# MAGIC
# MAGIC To demonstrate, we can use Databricks Workflows to continuously update a bronze table in our Delta Lake each time an updated file is published.
# MAGIC
# MAGIC The [medallion architecture](https://docs.databricks.com/lakehouse/medallion.html) describes a series of data layers that denote the quality of data stored in the Lakehouse. 
# MAGIC
# MAGIC This architecture guarantees atomicity, consistency, isolation, and durability as data passes through multiple layers of validations and transformations before being stored in a layout optimized for efficient analytics.

# COMMAND ----------

from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.functions import col, lit, current_date, current_timestamp, concat_ws

# This schema works for all the CAT Equity Symbols Master
catEquitySymbolsMasterSchema = StructType([
  StructField("symbol", StringType(), False, {'comment': "The symbol for the equity instrument listing."}),
  StructField("issueName", StringType(), False, {'comment': "The full issue name related to the equity instrument."}),
  StructField("listingExchange", StringType(), True, {'comment': "The primary listed exchange for the equity instrument."}),
  StructField("testIssueFlag", StringType(), True, {'comment': "Denotes that the symbol is a technical test symbol that is reserved and available for testing trading systems."})
])

# This schema works for all the CAT Options Symbols Master
catOptionsSymbolsMasterSchema = StructType([
  StructField("optionKind", StringType(), True),
  StructField("optionID", StringType(), False),
  StructField("primaryDeliverable", StringType(), False),
  StructField("exerciseStyle", StringType(), True),
  StructField("settlementType", StringType(), True),
  StructField("testOptionSeriesFlag", StringType(), False)
])

catMemberIdListSchema = StructType([
  StructField("CRDID", LongType(), False),
  StructField("FINRAMember", StringType(), False),
  StructField("firmName", StringType(), False),
  StructField("IMID", StringType(), False),
  StructField("exchangeID", StringType(), False),
  StructField("ATSFlag", StringType(), True),
  StructField("defaultIMIDFlag", StringType(), True),
])

catMemberIdConflictsListSchema = StructType([
  StructField("CRDID", LongType(), False),
  StructField("firmName", StringType(), False),
  StructField("conflictIMID", StringType(), False),
  StructField("exchangeID", StringType(), False),
  StructField("defaultIMIDFlag", StringType(), True)
])

schemaLookupMap = {
  "CAT Reportable Equity Securities Symbol Master Schema": catEquitySymbolsMasterSchema,
  "CAT Reportable Options Securities Symbol Master Schema": catOptionsSymbolsMasterSchema,
  "Member ID (IMID) List": catMemberIdListSchema,
  "Member ID (IMID) Conflicts List": catMemberIdConflictsListSchema
}

bronzeTableLookupMap = {
  "CAT Reportable Equity Securities Symbol Master SOD": "cat_equity_master_sod",
  "CAT Reportable Equity Securities Symbol Master EOD": "cat_equity_master_eod",
  "CAT Reportable Equity Securities Symbol Master - Intraday Updates": "cat_equity_master_intraday",
  "CAT Reportable Options Securities Symbol Master SOD": "cat_options_master_sod",
  "CAT Reportable Options Securities Symbol Master EOD": "cat_options_master_eod",
  "CAT Reportable Options Securities Symbol Master - Intraday Updates": "cat_options_master_intraday",
  "Member ID (IMID) List": "cat_member_id",
  "Member ID (IMID) Conflicts List": "cat_member_id_conflicts"
}

silverTableLookupMap = {
  "CAT Reportable Equity Securities Symbol Master SOD": "cat_equity_master",
  "CAT Reportable Equity Securities Symbol Master EOD": "cat_equity_master",
  "CAT Reportable Equity Securities Symbol Master - Intraday Updates": "cat_equity_master",
  "CAT Reportable Options Securities Symbol Master SOD": "cat_options_master",
  "CAT Reportable Options Securities Symbol Master EOD": "cat_options_master",
  "CAT Reportable Options Securities Symbol Master - Intraday Updates": "cat_options_master",
  "Member ID (IMID) List": "cat_member_id",
  "Member ID (IMID) Conflicts List": "cat_member_id_conflicts"
}

dltLiveTableLookupKeysMap = {
  "CAT Reportable Equity Securities Symbol Master SOD": ["symbol", "listingExchange"],
  "CAT Reportable Equity Securities Symbol Master EOD": ["symbol", "listingExchange"],
  "CAT Reportable Equity Securities Symbol Master - Intraday Updates": ["symbol", "listingExchange"],
  "CAT Reportable Options Securities Symbol Master SOD": ["optionKind", "optionID"],
  "CAT Reportable Options Securities Symbol Master EOD": ["optionKind", "optionID"],
  "CAT Reportable Options Securities Symbol Master - Intraday Updates": ["optionKind", "optionID"],
  "Member ID (IMID) List": ["CRDID", "IMID", "exchangeID"],
  "Member ID (IMID) Conflicts List": ["CRDID", "conflictIMID", "exchangeID"]
}

finraCATReferenceDataSchemaMap = {
  "CAT Reportable Equity Securities Symbol Master SOD": "CAT Reportable Equity Securities Symbol Master Schema",
  "CAT Reportable Equity Securities Symbol Master EOD": "CAT Reportable Equity Securities Symbol Master Schema",
  "CAT Reportable Equity Securities Symbol Master - Intraday Updates": "CAT Reportable Equity Securities Symbol Master Schema",
  "CAT Reportable Options Securities Symbol Master SOD": "CAT Reportable Options Securities Symbol Master Schema",
  "CAT Reportable Options Securities Symbol Master EOD": "CAT Reportable Options Securities Symbol Master Schema",
  "CAT Reportable Options Securities Symbol Master - Intraday Updates": "CAT Reportable Options Securities Symbol Master Schema",
  "Member ID (IMID) List": "Member ID (IMID) List",
  "Member ID (IMID) Conflicts List": "Member ID (IMID) Conflicts List"
}

# COMMAND ----------

# Define a schema for the downloaded file, as well as 
catReferenceDataURL = finraCATReferenceDataURLMap[catReferenceDataType]
catDataSchemaSelection = finraCATReferenceDataSchemaMap[catReferenceDataType]
catDataSchema = schemaLookupMap[catDataSchemaSelection]
bronzeTableLocation = bronzeTableLookupMap[catReferenceDataType]
dltLiveTableKeys = dltLiveTableLookupKeysMap[catReferenceDataType]
compositeKeyColumns = [col(columnName) for columnName in dltLiveTableKeys]
fileNameFromURL = os.path.basename(unquote(urlparse(catReferenceDataURL).path))
databaseName = "capital_markets"
tableNameFromURL = fileNameFromURL.replace(".txt", "").lower()

# Create a "bronze" table if not exists
spark.sql(f"""
CREATE DATABASE IF NOT EXISTS {databaseName}
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {databaseName}.{tableNameFromURL}_bronze
(symbol string, issueName string, listingExchange string, testIssueFlag string, catReferenceDataType string, currentDate date, currentTimestamp timestamp, compositeKey string)
USING DELTA
""")

# COMMAND ----------

# Finally, we’ll ingest the latest equity symbols CSV file into a “bronze” Delta table
def load_CAT_reference_data(catFileSchema, dbfsFilePath, fileName):
  return (
    spark.read.option("header", "true")
      .schema(catFileSchema)
      .option("delimiter", "|")
      .format("csv")
      .load("dbfs:" + dbfsFilePath + fileName)
      .withColumn("catReferenceDataType", lit(catReferenceDataType))
      .withColumn("currentDate", current_date())
      .withColumn("currentTimestamp", current_timestamp())
      .withColumn("compositeKey", concat_ws("|", *compositeKeyColumns))
  )

df = load_CAT_reference_data(catDataSchema, dbfsFileOutputWriteDirectory, fileNameFromURL)
df.write.insertInto(f"{databaseName}.{tableNameFromURL}_bronze")
display(spark.table(f"{databaseName}.{tableNameFromURL}_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Share the dataset with recipients
# MAGIC
# MAGIC Now that we’ve created a streaming pipeline to ingest updates to the symbol file each trading day, we can leverage Delta Sharing to share the Delta Table with data recipients. 
# MAGIC
# MAGIC Creating a Delta Share on the Databricks Lakehouse Platform can be done with just a few clicks of the button or with a single SQL statement if [SQL syntax](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-share.html) is preferred.
# MAGIC

# COMMAND ----------

spark.sql("""
CREATE SHARE IF NOT EXISTS FINRA_CAT_Share
COMMENT 'Contains datasets for all stock and equity securities traded across the U.S. National Market System (NMS).'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding a table to the Share
# MAGIC A data provider can populate a Delta Share with one or more tables by [altering](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-alter-share.html#syntax) the share.
# MAGIC
# MAGIC Note that the full history of a Delta table must be shared to support reads using Structured Streaming. History sharing is enabled by default using the Databricks UI to add a Delta table to a Share. However, history sharing must be explicitly specified when using the SQL syntax.

# COMMAND ----------

spark.sql(f"""
ALTER SHARE FINRA_CAT_Share
ADD TABLE {databaseName}.{tableNameFromURL}_bronze
WITH HISTORY
""")
