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
    for col in columns:
        col_type = df.schema[col].dataType

        for dtype, udf_func in udf_mapping.items():
            is_array_match = (
                isinstance(dtype, ArrayType)
                and isinstance(col_type, ArrayType)
                and col_type.elementType == dtype.elementType
            )
            is_exact_match = isinstance(col_type, type(dtype))

            if is_array_match or is_exact_match:
                print(
                    f"✅ Transforming column: '{col}' | actual type: {col_type} | matched type: {dtype}"
                )
                df = df.withColumn(col, udf_func(col))
                break
        else:
            print(
                f"⚠️ No matching UDF found for column: '{col}' | actual type: {col_type}"
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
            # "birthday_string_list",
        ],
        udf_mapping=UDF_MAPPING,
    )

    df_transformed.show()
