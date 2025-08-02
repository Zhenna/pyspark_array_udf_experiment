from pyspark.sql.functions import udf
from pyspark.sql.types import (
    IntegerType,
    DateType,
    StringType,
    DataType,
    ArrayType,
    FloatType,
    IntegerType,
)

from pyspark.sql import DataFrame


from utilities import (
    load_parquet_as_spark_df,
    calculate_age_from_date,
    calculate_age_from_date_numeric,
    calculate_age_from_string,
    calculate_ages_from_date_array,
    calculate_ages_from_string_array,
)

# register UDF
udf_calculate_age_date = udf(calculate_age_from_date, IntegerType())
udf_calculate_age_date_str = udf(calculate_age_from_string, IntegerType())
udf_calculate_age_nondates = udf(calculate_age_from_date_numeric, IntegerType())
udf_calculate_age_date_array = udf(
    calculate_ages_from_date_array, ArrayType(IntegerType())
)
udf_calculate_age_str_array = udf(
    calculate_ages_from_string_array, ArrayType(StringType())
)

UDF_MAPPING = {
    DateType(): udf_calculate_age_date,
    StringType(): udf_calculate_age_date_str,
    FloatType(): udf_calculate_age_nondates,
    ArrayType(DateType()): udf_calculate_age_date_array,
    ArrayType(StringType()): udf_calculate_age_str_array,
}


def apply_udf_by_typedf(
    df: DataFrame, columns: list[str], udf_mapping: dict[DataType, callable]
) -> DataFrame:
    """
    Apply a matching UDF to each specified column based on its data type.

    Args:
        df (DataFrame): The input Spark DataFrame.
        columns (list[str]): Column names to transform.
        udf_mapping (dict): Mapping of DataType to corresponding UDF.

    Returns:
        DataFrame: Updated DataFrame with transformed columns.
    """
    for col_name in columns:
        col_type = df.schema[col_name].dataType

        matched = False
        for dtype_key, udf_func in udf_mapping.items():
            if isinstance(dtype_key, ArrayType) and isinstance(col_type, ArrayType):
                if type(col_type.elementType) == type(dtype_key.elementType):
                    print(
                        f"✅ Transforming array column: '{col_name}' | actual type: {col_type} | matched type: {dtype_key}"
                    )
                    df = df.withColumn(col_name, udf_func(col_name))
                    matched = True
                    break
            elif type(col_type) == type(dtype_key):
                print(
                    f"✅ Transforming column: '{col_name}' | actual type: {col_type} | matched type: {dtype_key}"
                )
                df = df.withColumn(col_name, udf_func(col_name))
                matched = True
                break

        if not matched:
            print(
                f"⚠️ No matching UDF found for column: '{col_name}' | actual type: {col_type}"
            )

    return df


if __name__ == "__main__":
    df = load_parquet_as_spark_df("people")

    # print(df.show())

    df_transformed = apply_udf_by_typedf(
        df=df,
        columns=[
            "date of birth",
            "dob_str",
            "dob_yyyymmdd",
            "dob_ddmmyyyy",
            "birthday_list",
            "birthday_string_list",
        ],
        udf_mapping=UDF_MAPPING,
    )

    df_transformed.show()
