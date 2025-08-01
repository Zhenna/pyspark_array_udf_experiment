# %%
from utilities import (
    load_parquet_as_spark_df,
    calculate_age_from_date,
    calculate_age_from_string,
    calculate_age_from_date_numeric,
    calculate_ages_from_date_array,
    calculate_ages_from_string_array,
)


# %%
df = load_parquet_as_spark_df("people")
# df.limit(20).toPandas().to_csv("sample_data.csv", index=False)

# %%
df.show()

# %%
df.printSchema()


# %%

# Run a built-in udf to extract the year from the date of birth
from pyspark.sql.functions import year

df_new = df.withColumn("dob_year", year(df["date of birth"]))
df_new.show()


# %%

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Register UDF
udf_calculate_age_date = udf(calculate_age_from_date, IntegerType())


df_spark_1 = df.withColumn("age", udf_calculate_age_date(df["date of birth"]))
df_spark_1.select("date of birth", "age").show()

# %%

# Register UDF
udf_calculate_age_date_str = udf(calculate_age_from_string, IntegerType())

df_spark_2_0 = df.withColumn("age", udf_calculate_age_date_str(df["dob_str"]))
df_spark_2_0.select("dob_str", "age").show()


# %%

# Register the function as a UDF
udf_calculate_age_nondates = udf(calculate_age_from_date_numeric, IntegerType())


df_spark_6 = df.withColumn("age", udf_calculate_age_nondates(df["dob_yyyymmdd"]))
df_spark_6.select("dob_yyyymmdd", "age").show()


# %%

df_spark_7 = df.withColumn("age", udf_calculate_age_date_str(df["dob_ddmmyyyy"]))
df_spark_7.select("dob_ddmmyyyy", "age").show()


# %%
from pyspark.sql.types import ArrayType, StringType, IntegerType


udf_calculate_age_date_array = udf(
    calculate_ages_from_date_array, ArrayType(IntegerType())
)

df_spark_8 = df.withColumn("age", udf_calculate_age_date_array(df["birthday_list"]))
df_spark_8.select("birthday_list", "age").show()

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
