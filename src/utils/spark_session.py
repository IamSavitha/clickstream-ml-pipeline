#1 next -> schema
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "ClickstreamMLPipeline") -> SparkSession:
    """
    Create and return a configured SparkSession for the project.

    Parameters
    ----------
    app_name : str
        Name of the Spark application shown in Spark UI.

    Returns
    -------
    SparkSession
        Configured Spark session.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    spark = create_spark_session()
    print("Spark session created successfully.")
    print(f"Spark version: {spark.version}")
    spark.stop()