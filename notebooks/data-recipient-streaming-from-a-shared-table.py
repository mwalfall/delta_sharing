# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming from a Shared Delta Table
# MAGIC As a data recipient, streaming from a shared Delta table is just as simple! 
# MAGIC
# MAGIC Data recipients can stream from a Delta Table shared through Unity Catalog using Databricks Runtime 12.1 or greater.
# MAGIC
# MAGIC A data recipient can read the shared Delta table as a Spark Structured Stream using the `deltaSharing` data source and supplying the name of the shared table.

# COMMAND ----------

# MAGIC %pip install yfinance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a helper function for getting the price history of a stock symbol

# COMMAND ----------

import yfinance as yf
import pyspark.sql.functions as F


def get_stock_prices(symbol: str, period="1wk", interval="1wk"):
   """ Scrapes the stock price history of a ticker symbol over the last 1 week.
   arguments:
       symbol (String) - The target stock symbol, typically a 3-4 letter abbreviation.
   returns:
       (Spark DataFrame) - The current price of the provided ticker symbol.
   """
   ticker = yf.Ticker(symbol)

   # Retrieve the last recorded stock price in the last week
   current_stock_price = ticker.history(period=period, interval=interval)

   # Convert to a Spark DataFrame
   df = spark.createDataFrame(current_stock_price)

   # Select only columns relevant to stock price and add an event processing timestamp
   event_ts = str(current_stock_price.index[0])
   df = (df.withColumn("event_ts", F.lit(event_ts))
           .withColumn("symbol", F.lit(symbol))
           .select(
              F.col("symbol"), F.col("Open"), F.col("High"), F.col("Low"), F.col("Close"),
              F.col("Volume"), F.col("event_ts").cast("timestamp"))
   )

   # Return the latest price information
   return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Join the stock history with the shared symbol table

# COMMAND ----------

# Grab the weekly price histories for 3 major tech stocks
aapl_stock_prices = get_weekly_stock_prices('AAPL')
msft_stock_prices = get_weekly_stock_prices('MSFT')
nvidia_stock_prices = get_weekly_stock_prices('NVDA')
all_stock_prices = aapl_stock_prices.union(msft_stock_prices).union(nvidia_stock_prices)

# Join the stock price histories with the equity symbols master stream
symbols_master = spark.readStream.format('deltaSharing').table('finra_catalog.finra.cat_equity_master_sod')
(symbols_master.join(all_stock_prices, on="symbol", how="inner")
               .select("symbol", "issueName", "listingExchange", "testIssueFlag", "catReferenceDataType",
                       "Open", "High", "Low", "Close", "Volume", "event_ts")
).display()

# COMMAND ----------


