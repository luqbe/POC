# Databricks notebook source

#READING POS_DATA_WITH_PPG FILE
df = spark.read.format('csv') \
    .option('inferSchema', 'true') \
    .option('header', 'true') \
    .load('/FileStore/tables/pos_data_with_ppg.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import when, col, lit

# Replace NULL or empty string in 'sales_per_mm_acv'
df = df.withColumn(
    "sales_per_mm_acv",
    when(col("sales_per_mm_acv").isNull() | (col("sales_per_mm_acv") == ""), lit(0))
    .otherwise(col("sales_per_mm_acv"))
)

# Replace NULL or empty string in 'acv_mm'
df = df.withColumn(
    "acv_mm",
    when(col("acv_mm").isNull() | (col("acv_mm") == ""), lit(0))
    .otherwise(col("acv_mm"))
)

display(df)

# COMMAND ----------

#TYPECAST SEVERALFIELDS TO DOUBLE
columns_to_cast = [
    "value_sales", "unit_sales", "eq_volume_sales",
    "base_value_sales", "base_unit_sales", "base_eq_volume_sales",
    "incremental_value_sales", "incremental_unit_sales", "incremental_eq_volume_sales",
    "weekly_acv", "acv_mm", "sales_per_mm_acv",
    "unit_price", "unit_base_price", "eq_price", "eq_base_price",
    "price_reduction_pct",
    "pct_sales_w_promo", "pct_units_w_promo", "pct_volume_w_promo",
    "pct_base_sales_w_promo", "pct_base_units_w_promo", "pct_base_volume_w_promo",
    "pct_incr_sales_w_promo", "pct_incr_units_w_promo", "pct_incr_volume_w_promo",
    "sales_w_any_promo", "units_w_any_promo", "volume_w_any_promo",
    "sales_w_no_promo", "units_w_no_promo", "volume_w_no_promo",
    "acv_display_only", "acv_feature_only", "acv_f_and_d_only", "acv_tpr_only",
    "acv_any_promo", "acv_f_or_d", "acv_no_promo"
]

# COMMAND ----------

from pyspark.sql.functions import col

for column in columns_to_cast:
    df = df.withColumn(column, col(column).cast("double"))
    


# COMMAND ----------

display(df)

# COMMAND ----------


#CREATE CONDITIONALLY DERIVED FIELD
from pyspark.sql.functions import when, col, lit

df = df.withColumn(
    "acv_unit_sales",
    when(col("weekly_acv") > 0, col("unit_sales")).otherwise(lit(0))
)

df = df.withColumn(
    "acv_display_only_unit_sales",
    when(col("weekly_acv") > 0, col("acv_display_only") * col("unit_sales")).otherwise(lit(0))
)

df = df.withColumn(
    "acv_feature_only_unit_sales",
    when(col("weekly_acv") > 0, col("acv_feature_only") * col("unit_sales")).otherwise(lit(0))
)

df = df.withColumn(
    "acv_f_and_d_only_unit_sales",
    when(col("weekly_acv") > 0, col("acv_f_and_d_only") * col("unit_sales")).otherwise(lit(0))
)

df = df.withColumn(
    "acv_tpr_only_unit_sales",
    when(col("weekly_acv") > 0, col("acv_tpr_only") * col("unit_sales")).otherwise(lit(0))
)

# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------


#ACV WITH CORRESPONDING UNIT SALES
df = df.withColumn(
    "price_reduction_pct_wtd",
    (col("price_reduction_pct") / 100.0) * col("unit_sales")
)
df = df.withColumn(
    "acvXunit",
    col("weekly_acv") * col("unit_sales")
)
df = df.withColumn(
    "acv_display_only_unit_wtd",
    col("acv_display_only") * col("unit_sales")
)
df = df.withColumn(
    "acv_feature_only_unit_wtd",
    col("acv_feature_only") * col("unit_sales")
)

# COMMAND ----------

display(df)

# COMMAND ----------

#AGGREGATE DATA AND SUMMING ALL NUMERIC VALUES
from pyspark.sql.functions import sum, col
from pyspark.sql.types import NumericType
from pyspark.sql import functions as F

# Grouping columns
group_cols = ["geography_description", "ppg", "week_ending_date"]

# Select numeric columns to aggregate
numeric_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, NumericType) and field.name not in group_cols]

# Create Aggregation Expressions
agg_exprs = [F.sum(col(c)).alias(c) for c in numeric_cols]
df_agg = df.groupBy(group_cols).agg(*agg_exprs)


# COMMAND ----------

display(df_agg)

# COMMAND ----------

from pyspark.sql import functions as F

# Derive normalized metrics safely
df_norm = df_agg \
    .withColumn("average_unit_size",
                F.when(F.col("unit_sales") > 0, F.col("eq_volume_sales") / F.col("unit_sales"))) \
    .withColumn("unit_price_wtd",
                F.when(F.col("unit_sales") > 0, F.col("value_sales") / F.col("unit_sales"))) \
    .withColumn("eq_price_wtd",
                F.when(F.col("eq_volume_sales") > 0, F.col("value_sales") / F.col("eq_volume_sales"))) \
    .withColumn("base_price_unit_wtd",
                F.when(F.col("base_unit_sales") > 0, F.col("base_value_sales") / F.col("base_unit_sales"))) \
    .withColumn("base_price_eq_wtd",
                F.when(F.col("base_eq_volume_sales") > 0, F.col("base_value_sales") / F.col("base_eq_volume_sales"))) \
    .withColumn("acv_mm_fallback",
                F.when(F.col("acv_mm").isNull(), F.col("weekly_acv") / 1_000_000).otherwise(F.col("acv_mm"))) \
    .withColumn("acv_pct",
                F.when(F.col("acv_mm_fallback").isNotNull() & (F.col("acv_mm_fallback") > 0),
                       F.col("weekly_acv") / F.col("acv_mm_fallback")))


# COMMAND ----------

display(df_norm)

# COMMAND ----------

# Calculate week_start_date as 6 days before
df_norm = df_norm.withColumn("week_start_date", date_sub(col("week_ending_date"), 6))

# COMMAND ----------

display(df_norm)

# COMMAND ----------

from pyspark.sql.functions import col, when

# Use 'acvXunit' as TDP and 'acv_pct' as ACV%
df_norm = df_norm.withColumn(
    "ips",
    when(
        (col("acv_pct").isNotNull()) & (col("acv_pct") != 0),
        col("acvXunit") / (col("acv_pct") / 100)
    ).otherwise(None)
)

# COMMAND ----------

display(df_norm)

# COMMAND ----------

#backfill eq_lbs,etc,.
from pyspark.sql import functions as F

df_filled = df_norm \
    .withColumn("eq_lbs",
                F.when((F.col("eq_volume_sales").isNull()) | (F.col("eq_volume_sales") == 0),
                       F.col("unit_sales") * F.col("average_unit_size"))
                .otherwise(F.col("eq_volume_sales"))) \
    .withColumn("eq_lbs_base",
                F.when((F.col("base_eq_volume_sales").isNull()) | (F.col("base_eq_volume_sales") == 0),
                       F.col("base_unit_sales") * F.col("average_unit_size"))
                .otherwise(F.col("base_eq_volume_sales"))) \
    .withColumn("eq_lbs_incr",
                F.when((F.col("incremental_eq_volume_sales").isNull()) | (F.col("incremental_eq_volume_sales") == 0),
                       F.col("incremental_unit_sales") * F.col("average_unit_size"))
                .otherwise(F.col("incremental_eq_volume_sales")))


# COMMAND ----------

display(df_filled)

# COMMAND ----------

# Compute base_price_eq_lbs_wtd as base_price_unit_wtd * average_unit_size
df_final = df_filled.withColumn(
    "base_price_eq_lbs_wtd",
    col("base_price_unit_wtd") * col("average_unit_size")
)

# COMMAND ----------

#display(df_final)
df_final.select("geography_description", "ppg", "base_price_unit_wtd", "average_unit_size", "base_price_eq_lbs_wtd").show()


# COMMAND ----------

