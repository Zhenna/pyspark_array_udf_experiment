# %%
from utilities import (
    load_parquet_as_spark_df,
    calculate_age_from_date,
    calculate_age_from_non_date,
    calculate_ages_from_date_array,
    calculate_ages_from_string_array,
)


# %%
df = load_parquet_as_spark_df("people")

# %%
df.show()

# %%

df.printSchema()

# %%
from pyspark.sql.functions import to_date, col

df = df.orderBy(col("dob_ddmmyyyy").desc()).limit(3)
# %%

# Run a built-in udf to extract the year from the date of birth
from pyspark.sql.functions import year

df_new = df.withColumn("dob_year", year(df["date of birth"]))

# %% Display new column

df_new.show()

# %%

# date_val = df.select("date of birth").dropna().first()["date of birth"]
# date_val

# %%


# %%

# calculate_age_from_date(date_val)
# %%

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Register UDF
udf_calculate_age_date = udf(calculate_age_from_date, IntegerType())

# %%

df_spark_1 = df.withColumn("age", udf_calculate_age_date(df["date of birth"]))
df_spark_1.select("date of birth", "age").show()

# %%
from pyspark.sql.functions import to_date

df_spark_2 = df.withColumn("dob_str_datetype", to_date("dob_str"))
df_spark_2 = df_spark_2.withColumn(
    "age", udf_calculate_age_date(df_spark_2["dob_str_datetype"])
)

df_spark_2.select("dob_str", "dob_str_datetype", "age").show()

# %%

from pyspark.sql.functions import to_date

df_spark_3 = df.withColumn("dob_yyyymmdd_todate", to_date("dob_yyyymmdd"))

df_spark_3.select("dob_yyyymmdd", "dob_yyyymmdd_todate").show()


"""
DateTimeException: [CAST_INVALID_INPUT] The value '1.9830508E7' of the type "STRING" cannot be cast to "DATE" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. 
"""
# %% still not a valid date string

from pyspark.sql.functions import to_date, col

df_spark_4 = df.withColumn(
    "dob_yyyymmdd_todate", to_date(col("dob_yyyymmdd"), "yyyyMMdd")
)

df_spark_4.select("dob_yyyymmdd", "dob_yyyymmdd_todate").show()

"""
[CANNOT_PARSE_TIMESTAMP] Text ''1.9830508E7' could not be parsed at index 0. Use `try_to_date` to tolerate invalid input string and return NULL instead.
"""
# %%

from pyspark.sql.functions import col

df_spark_5 = df.withColumn(
    "dob_yyyymmdd_todate",
    to_date(col("dob_yyyymmdd").cast("int"), "yyyyMMdd"),
)

df_spark_5 = df_spark_5.withColumn(
    "age", udf_calculate_age_date(df_spark_5["dob_yyyymmdd_todate"])
)

df_spark_5.select("dob_yyyymmdd", "dob_yyyymmdd_todate", "age").show()

"""
 Let’s break it down:

.dt.strftime("%Y%m%d")
Converts a date like 1959-05-31 → '19590531' (a string)

.astype(float)
Converts '19590531' → 19590531.0, but large integers like this become imprecise in float due to:

IEEE 754 float limitations

Rounding or truncation during conversion

Display as scientific notation like 1.9590531e+07, which may lose digits

When later converted back to int or parsed as date, the value may become 19590521 instead of 19590531 due to that float precision loss.
"""

# %%

# Register the function as a UDF
udf_calculate_age_nondates = udf(calculate_age_from_non_date, IntegerType())

# %%
from pyspark.sql.functions import col


df_spark_6 = df.withColumn("age", udf_calculate_age_nondates(df["dob_yyyymmdd"]))

df_spark_6.select("dob_yyyymmdd", "age").show()

# %%
# from pyspark.sql.functions import col


df_spark_7 = df.withColumn("age", udf_calculate_age_nondates(df["dob_ddmmyyyy"]))

df_spark_7.select("dob_ddmmyyyy", "age").show()

# %%

df.select("birthday_list").show()

# %%
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType


udf_calculate_age_date_array = udf(
    calculate_ages_from_date_array, ArrayType(IntegerType())
)

df_spark_8 = df.withColumn("age", udf_calculate_age_date_array(df["birthday_list"]))

df_spark_8.select("birthday_list", "age").show()

# %%

df.select("birthday_string_list").show()

# %%
# Register UDF

udf_calculate_age_str_array = udf(
    calculate_ages_from_string_array, ArrayType(StringType())
)

df_spark_9 = df.withColumn(
    "age", udf_calculate_age_str_array(df["birthday_string_list"])
)

df_spark_9.select("birthday_string_list", "age").show()
# %%

df_spark_9.select("age").dropna().first()["age"]

# %%
