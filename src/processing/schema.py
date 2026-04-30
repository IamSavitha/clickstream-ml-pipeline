#1 sparksession -> schema -> 
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)


def get_criteo_columns():
    """
    Returns column names for the Criteo dataset.

    Returns
    -------
    list
        List of column names
    """
    columns = ["label"] + \
              [f"I{i}" for i in range(1, 14)] + \
              [f"C{i}" for i in range(1, 27)]

    return columns


def get_criteo_schema():
    """
    Returns the schema for the Criteo dataset.

    Returns
    -------
    StructType
        Spark schema
    """

    fields = []

    # Label
    fields.append(StructField("label", IntegerType(), True))

    # Numeric features I1–I13
    for i in range(1, 14):
        fields.append(StructField(f"I{i}", DoubleType(), True))

    # Categorical features C1–C26
    for i in range(1, 27):
        fields.append(StructField(f"C{i}", StringType(), True))

    return StructType(fields)


def apply_column_names(df):
    """
    Assign column names to a DataFrame without headers.

    Parameters
    ----------
    df : DataFrame

    Returns
    -------
    DataFrame
    """

    columns = get_criteo_columns()
    return df.toDF(*columns)