import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def spark_sample_demo(app_name: str = "spark_sample_app", stop_after: bool = True):
    """
    Create a SparkSession, build a sample DataFrame, display it, and (optionally) stop Spark.

    Parameters
    ----------
    app_name : str, optional
        Name of the Spark application (default: "spark_sample_app").
    stop_after : bool, optional
        If True, the function will call spark.stop() before returning.
        Set to False if you want to keep the session alive for later use.

    Returns
    -------
    pyspark.sql.DataFrame
        The sample DataFrame that was displayed.
    pyspark.sql.SparkSession
        The SparkSession that was created (or retrieved).
    """
    # If a SparkSession already exists in the JVM, `getOrCreate` will return it.
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    # Define a tiny schema and some example data
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=True),
        ]
    )

    sample_rows = [
        (1, "Alice", 34),
        (2, "Bob", 28),
        (3, "Charlie", None),  # demonstrate a null value
        (4, "Diana", 45),
    ]

    # Create the DataFrame
    df = spark.createDataFrame(sample_rows, schema=schema)

    # Show the DataFrame (default prints first 20 rows)
    print("\n=== Sample Spark DataFrame ===")
    df.show(truncate=False)  # truncate=False prints full column values
    print("\nSchema:")
    df.printSchema()

    # Clean‑up (optional)
    if stop_after:
        spark.stop()
        print("\nSpark session stopped.")
    else:
        print("\nSpark session left running - you can reuse `spark` later.")

    # Return objects for further use if desired
    return df, spark


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3key", required=True, type=str, help="S3 file key")
    args = parser.parse_args()

    # keep the session alive for more work
    df_sample, spark = spark_sample_demo(stop_after=False)
    # Define schema for the dataframe of salaries
    salary_schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("salary", IntegerType(), nullable=False),
        ]
    )
    # Read csv file of salaries from a S3 bucket
    df_salary = (
        spark.read.format("csv")
        .option("header", True)
        .schema(salary_schema)
        .load(f"s3a://{args.s3key}")
    )
    df_salary.show()
    # Do more Spark transformations ...
    df_filtered = df_sample.filter(df_sample.age > 30)

    print("\nFiltered rows (age > 30):")
    df_filtered.show()

    print("\nFiltered rows with salary:")
    # Merge the two dataframes
    df_filtered_with_salary = df_filtered.join(
        df_salary, df_filtered.id == df_salary.id, "inner"
    ).drop(df_salary.id)
    df_filtered_with_salary.show()
    # Write the dataframe to the S3 bucket
    df_filtered_with_salary.write.format("csv").mode("overwrite").save(
        f"s3a://{args.s3key.split('/')[0]}/argo/spark/artifacts/processed"
    )
    # stop Spark explicitly
    spark.stop()
