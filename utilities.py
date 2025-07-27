from pyspark.sql import SparkSession, DataFrame
from typing import Iterable, List, Optional

from datetime import date, datetime
import pandas as pd


def calculate_age_from_non_date(dob_input) -> int | None:
    """
    Accepts various DOB formats:
    - date or datetime object
    - pandas.Timestamp
    - string or integer in YYYYMMDD or DDMMYYYY format

    Returns:
        int | None: Age in years or None if invalid or future date
    """
    try:
        if dob_input is None or pd.isna(dob_input):
            return None

        # Handle datetime-like objects
        if isinstance(dob_input, (pd.Timestamp, datetime)):
            dob = dob_input.date()
        elif isinstance(dob_input, date):
            dob = dob_input
        else:
            # Try parsing as integer or string
            dob_str = str(int(dob_input)).zfill(
                8
            )  # e.g., 19800531 -> '19800531', pad if needed

            # Try YYYYMMDD format first
            try:
                dob = datetime.strptime(dob_str, "%Y%m%d").date()
            except ValueError:
                # Try DDMMYYYY format next
                dob = datetime.strptime(dob_str, "%d%m%Y").date()

        return calculate_age_from_date(dob)

    except Exception:
        return None

    """
    "30-AUG-1978 00:00:00.000" ✅

    "30-Aug-1978 00:00:00.000" ✅
    """


"""
Yes — you can parse "30-AUG-1978 00:00:00.000" directly in Python using strptime() by transforming the input with .upper() or .title() only for the month part, but unfortunately strptime() itself does not accept uppercase %b ("AUG" fails, "Aug" works). So manual case normalization is required unless:
"""


def calculate_age_from_date(dob: date) -> int | None:
    """
    Calculates age from a date object.

    Returns None if the date is invalid or in the future.

    Parameters:
        dob (date): Date of birth as a datetime.date or pandas.Timestamp.

    Returns:
        int | None: Age in years, or None if input is invalid or dob > today.
    """

    today = date.today()

    if dob > today:
        return None

    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    return age


def calculate_ages_from_date_array(dobs: Iterable[date]) -> List[Optional[int]]:
    """
    Applies `calculate_age_from_date` to each item in an iterable of dates.

    Parameters:
        dobs (Iterable[date]): A list or iterable of datetime.date objects.

    Returns:
        List[Optional[int]]: A list of calculated ages or None for invalid/future dates.
    """
    return [calculate_age_from_date(dob) if dob else None for dob in dobs]


def calculate_ages_from_string_array(records: List[str]) -> List[Optional[str]]:
    """
    Parses an array of strings in the format 'id|name|sex|YYYYMMDD', calculates age,
    and replaces the date portion with the computed age.

    Parameters:
        records (List[str]): List of strings where the last part is DOB in 'YYYYMMDD'.

    Returns:
        List[Optional[str]]: List of strings with age replacing the last part,
                             or None if the format is invalid.
    """
    results = []

    for record in records:
        try:
            parts = record.split("|")
            if len(parts) < 4:
                results.append(None)
                continue

            dob_str = parts[-1]
            dob = datetime.strptime(dob_str, "%Y%m%d").date()
            age = calculate_age_from_date(dob)

            if age is None:
                results.append(None)
            else:
                # Replace the last part with the age
                new_record = "|".join(parts[:-1] + [str(age)])
                results.append(new_record)

        except Exception:
            results.append(None)

    return results


def load_parquet_as_spark_df(
    parquet_path: str, app_name: str = "LoadParquet"
) -> DataFrame:
    """
    Loads a Parquet file as a Spark DataFrame.

    Parameters:
        parquet_path (str): Path to the Parquet file or directory.
        app_name (str): Name for the Spark application (default is "LoadParquet").

    Returns:
        DataFrame: A Spark DataFrame loaded from the Parquet file.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    df = spark.read.parquet(parquet_path)
    return df
