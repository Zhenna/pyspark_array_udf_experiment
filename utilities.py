from pyspark.sql import SparkSession, DataFrame
from typing import Iterable, List, Optional

from datetime import date, datetime
import pandas as pd

from datetime import datetime
from typing import Optional


def calculate_age_from_date_numeric(dob_input: float | int) -> Optional[int]:
    """
    Parses numeric date of birth into a date and calculates age.
    Accepts float or integer in formats:
      - YYYYMMDD (e.g. 19801230)
      - DDMMYYYY (e.g. 30121980)

    Parameters:
        dob_input (float | int): Date of birth as a numeric value.

    Returns:
        Optional[int]: Age in years or None if invalid.
    """
    try:
        if dob_input is None:
            return None

        # Convert to int to drop any .0 from float
        # dob_str = str(int(float(dob_input))).zfill(8)
        dob_str = str(int(dob_input)).zfill(8)

        try:
            # Try YYYYMMDD
            dob = datetime.strptime(dob_str, "%Y%m%d").date()
        except ValueError:
            # Try DDMMYYYY
            dob = datetime.strptime(dob_str, "%d%m%Y").date()

        return calculate_age_from_date(dob)

    except Exception:
        return None


def calculate_age_from_string(dob_str: str) -> Optional[int]:
    """
    Parses a date string and calculates age. Supports:
      - 'YYYY-MM-DD'
      - 'DDMMYYYY' (with optional leading zero padding)

    Parameters:
        dob_str (str): Date of birth string.

    Returns:
        Optional[int]: Age in years or None if parsing fails.
    """
    if not isinstance(dob_str, str):
        return None

    dob_str = dob_str.strip()

    # Pad with leading zeros if numeric and less than 8 chars
    if dob_str.isdigit() and len(dob_str) < 8:
        dob_str = dob_str.zfill(8)

    try:
        if "-" in dob_str:
            dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
        else:
            dob = datetime.strptime(dob_str, "%d%m%Y").date()
        return calculate_age_from_date(dob)
    except Exception:
        return None


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
    Applies `calculate_age_from_date` to each item in an iterable,
    only if it is a non-null instance of `date`.

    Parameters:
        dobs (Iterable[date]): A list or iterable of datetime.date objects.

    Returns:
        List[Optional[int]]: A list of calculated ages or None for invalid inputs.
    """
    return [
        calculate_age_from_date(dob) if isinstance(dob, date) else None for dob in dobs
    ]


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
