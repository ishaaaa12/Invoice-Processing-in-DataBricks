# Databricks notebook source
base_path='/Volumes/workspace/default/raw_invoices'
files = dbutils.fs.ls(base_path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Raw Documents (PDFs)**
# MAGIC
# MAGIC
# MAGIC Invoices and receipts are not plain text files they are binary documents. To preserve their original structure and visual layout, we read them as binary data instead of attempting text extraction at this stage.

# COMMAND ----------

raw_df = (
  spark.read.format("binaryFile")
  .option("pathGlobFilter", "*.pdf")
  .load(base_path)
  .select("path", "content", "length", "modificationTime")
)

display(raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Intelligent Document Parsing**
# MAGIC
# MAGIC The ai_parse_document function of Databricks:
# MAGIC
# MAGIC - reads text from scanned or digital documents
# MAGIC - understands document layout (pages, blocks, tables)
# MAGIC - produces a structured representation of the document content
# MAGIC
# MAGIC The output is stored as a VARIANT column for downstream extraction and validation.

# COMMAND ----------

from pyspark.sql.functions import expr

parsed_df = raw_df.select(
    "path",
    expr("ai_parse_document(content) as parsed_document")
)


# COMMAND ----------

display(parsed_df.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC **Extracting Document Elements**
# MAGIC
# MAGIC The parsed document output contains a nested collection of elements, such as:
# MAGIC
# MAGIC Text blocks
# MAGIC Table cells
# MAGIC Headers and footers
# MAGIC Page-level metadata
# MAGIC
# MAGIC The elements array is extracted from VARIANT column as a JSON string.

# COMMAND ----------

elements_json_df = parsed_df.select(
    "path",
    expr("cast(variant_get(parsed_document, '$.document.elements') as string) as elements_json")
)

display(elements_json_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Defining a Structured Schema for Document Elements**
# MAGIC
# MAGIC Each element represents a small piece of the document such as:
# MAGIC - a line of text
# MAGIC - a table cell
# MAGIC - a header or footer
# MAGIC along with its page number and position (bounding box) in the document.
# MAGIC
# MAGIC The raw JSON string of document elements is converted into a typed Spark structure

# COMMAND ----------

from pyspark.sql.functions import col, from_json, explode

elements_schema = """
array<struct<
  id:int,
  type:string,
  content:string,
  page_id:int,
  bbox:struct<coord:array<int>, page_id:int>,
  description:string
>>
"""

elements_df = (
    elements_json_df
    .select("path", explode(from_json(col("elements_json"), elements_schema)).alias("element"))
    .select(
        "path",
        col("element.id").alias("element_id"),
        col("element.type").alias("element_type"),
        col("element.content").alias("content"),
        col("element.page_id").alias("page_id"),
        col("element.bbox.coord").alias("bbox_coord"),
        col("element.description").alias("description")
    )
)

display(elements_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Extracting Monetary Fields from Candidate Elements**
# MAGIC
# MAGIC After identifying candidate elements, we clean and normalize the extracted text to remove HTML artifacts, extra whitespace, and casing inconsistencies.
# MAGIC
# MAGIC We then extract invoice-level monetary fields (subtotal, shipping, tax, total) using label-based patterns. This step is intentionally tolerant and works even when multiple fields appear in the same table row.
# MAGIC
# MAGIC Finally, we reshape the extracted values into a canonical (field, amount) format, making the data easier to validate, aggregate, and analyze downstream.

# COMMAND ----------

keywords=["total","subtotal","tax","gst","vat","shipping","freight","discount","amount due", "balance due"]

from pyspark.sql.functions import lower
invoice_amount_candidates=(
    elements_df
    .withColumn("content_l",lower(col("content")))
    .where(" OR ".join([f"content_l LIKE '%{k}%'" for k in keywords]))
    .select("path","page_id","element_type","content","bbox_coord")
    .orderBy("path","page_id","element_id")
)

display(invoice_amount_candidates)

# COMMAND ----------

from pyspark.sql import functions as F

pairs_src = (
    invoice_amount_candidates
    .withColumn("plain", F.regexp_replace(F.col("content"), r"<[^>]+>", " "))
    .withColumn("plain", F.regexp_replace(F.col("plain"), r"\s+", " "))
    .withColumn("plain_l", F.lower(F.col("plain")))
)

invoice_amounts = (
    pairs_src
    .select(
        "path",
        F.regexp_extract(F.col("plain"), r"Subtotal:\s*\$?\s*(\d[\d,]*\.?\d{0,2})", 1).alias("subtotal"),
        F.regexp_extract(F.col("plain"), r"Shipping:\s*\$?\s*(\d[\d,]*\.?\d{0,2})", 1).alias("shipping"),
        F.regexp_extract(F.col("plain"), r"Tax[^:]*:\s*\$?\s*(\d[\d,]*\.?\d{0,2})", 1).alias("tax"),
        F.regexp_extract(F.col("plain"), r"(Total Due:|Amount Due:|Balance Due:)\s*\$?\s*(\d[\d,]*\.?\d{0,2})", 2).alias("total")
    )
    .selectExpr(
        "path",
        "stack(4, 'subtotal', subtotal, 'shipping', shipping, 'tax', tax, 'total', total) as (field, amount_str)"
    )
    .withColumn("amount", F.expr("try_cast(regexp_replace(amount_str, '[, ]', '') as double)"))
    .where(F.col("amount").isNotNull())
    .select("path", "field", "amount")
)

display(invoice_amounts)

# COMMAND ----------

# MAGIC %md
# MAGIC **Aggregating Extracted Fields to Invoice Level**
# MAGIC
# MAGIC Earlier steps produced multiple rows per invoice, as each document was decomposed into individual elements. Here, we reconstruct a one-row-per-invoice view.
# MAGIC
# MAGIC We first create a complete list of invoice paths, then pivot extracted (field, amount) values back into columns (subtotal, shipping, tax, total).

# COMMAND ----------

invoice_files = (
    elements_df
    .select("path")
    .distinct()
)


# COMMAND ----------

from pyspark.sql import functions as F

all_paths = invoice_files.select("path").distinct()

pivot_financials = (
    invoice_amounts
    .groupBy("path")
    .pivot("field", ["subtotal", "shipping", "tax", "total"])
    .agg(F.max("amount"))
)

invoice_financials = (
    all_paths
    .join(pivot_financials, on="path", how="left")
)

display(invoice_financials)

# COMMAND ----------

# MAGIC %md
# MAGIC **Validating Extracted Totals and Flagging Issues**
# MAGIC
# MAGIC Extraction alone is not enough, we also need trust signals.
# MAGIC
# MAGIC Here we compute a calculated_total = subtotal + shipping + tax (treating missing values as 0), compare it with the extracted total, and record the absolute difference.
# MAGIC
# MAGIC We then assign a simple status label to make issues explicit (missing fields or total mismatch), so the final output is not just extracted data, but validated, debuggable invoice-level results.

# COMMAND ----------

invoice_gold = (
    invoice_financials
    .withColumn(
        "calculated_total",
        F.coalesce(F.col("subtotal"), F.lit(0.0)) +
        F.coalesce(F.col("shipping"), F.lit(0.0)) +
        F.coalesce(F.col("tax"), F.lit(0.0))
    )
    .withColumn("diff", F.abs(F.col("total") - F.col("calculated_total")))
    .withColumn("is_consistent", F.col("diff") <= F.lit(0.05))
    .withColumn(
        "status",
        F.when(F.col("total").isNull(), "MISSING_TOTAL")
         .when(F.col("subtotal").isNull(), "MISSING_SUBTOTAL")
         .when(F.col("shipping").isNull(), "MISSING_SHIPPING")
         .when(F.col("tax").isNull(), "MISSING_TAX")
         .when(~F.col("is_consistent"), "TOTAL_MISMATCH")
         .otherwise("OK")
    )
)

display(invoice_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC After extraction, aggregation, and validation, we persist the final invoice-level dataset as a managed table.
# MAGIC
# MAGIC This invoice_gold table represents the clean, validated, one-row-per-invoice output of the pipeline and can now be used directly for analytics, reporting, or downstream AI applications.

# COMMAND ----------


spark.sql("CREATE SCHEMA IF NOT EXISTS gs_invoices")

invoice_gold.write.mode("overwrite").saveAsTable("gs_invoices.invoice_gold")
