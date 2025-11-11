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
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType

    # If a SparkSession already exists in the JVM, `getOrCreate` will return it.
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    # Define a tiny schema and some example data
    schema = StructType([
        StructField("id",   IntegerType(), nullable=False),
        StructField("name", StringType(),  nullable=False),
        StructField("age",  IntegerType(), nullable=True)
    ])

    sample_rows = [
        (1, "Alice",   34),
        (2, "Bob",     28),
        (3, "Charlie", None),   # demonstrate a null value
        (4, "Diana",   45)
    ]

    # Create the DataFrame
    df = spark.createDataFrame(sample_rows, schema=schema)

    # Show the DataFrame (default prints first 20 rows)
    print("\n=== Sample Spark DataFrame ===")
    df.show(truncate=False)          # truncate=False prints full column values
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
    # keep the session alive for more work
    df, spark = spark_sample_demo(stop_after=False)

    # Do more Spark transformations ...
    df_filtered = df.filter(df.age > 30)
    print("\nFiltered rows (age > 30):")
    df_filtered.show()
    df_filtered.write.format("parquet").mode("overwrite") \
        .save("s3a://customerintelligence/argo/files")
    # stop Spark explicitly
    spark.stop()