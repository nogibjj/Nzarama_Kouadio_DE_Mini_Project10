from pyspark.sql import SparkSession
import re

def load_data(spark, local_path="dataset/nba_games_stats.csv"):
    """
    Load data from a local CSV file into a PySpark DataFrame and clean up column names.
    :param spark: SparkSession object.
    :param local_path: Local path of the CSV file.
    :return: PySpark DataFrame containing the cleaned data.
    """
    # Load the data with PySpark
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    # Clean up column names by removing all non-alphanumeric characters
    for col_name in df.columns:
        new_col_name = re.sub(r'[^a-zA-Z0-9]', '', col_name)  # Remove all non-alphanumeric characters
        df = df.withColumnRenamed(col_name, new_col_name)
    
    return df

def explore_data(df):
    """
    Perform basic exploration on the DataFrame.
    :param df: PySpark DataFrame to explore.
    """
    # Show the first few rows
    print("First 5 rows:")
    df.show(5)

    # Print the schema
    print("\nSchema:")
    df.printSchema()

    # Count the number of rows
    row_count = df.count()
    print(f"\nTotal number of rows: {row_count}")

    # Describe statistics
    print("\nSummary statistics:")
    df.describe().show()
