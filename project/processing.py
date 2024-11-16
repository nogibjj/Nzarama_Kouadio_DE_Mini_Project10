from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum
import re

def load_data(spark, local_path="dataset/nba_games_stats.csv"):
    """
    Load data from a local CSV file into a PySpark DataFrame, clean up column names, and remove duplicate columns.
    :param spark: SparkSession object.
    :param local_path: Local path of the CSV file.
    :return: PySpark DataFrame containing the cleaned data.
    """
    # Load the data with PySpark
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    # Clean up column names by removing trailing periods and other non-alphanumeric characters
    for col_name in df.columns:
        new_col_name = re.sub(r'[^a-zA-Z0-9]', '', col_name)  # Remove all non-alphanumeric characters
        df = df.withColumnRenamed(col_name, new_col_name)

    # Check for duplicate columns and remove them
    column_set = set()
    duplicate_columns = [col for col in df.columns if col in column_set or column_set.add(col)]
    if duplicate_columns:
        print(f"Duplicate Columns Detected: {duplicate_columns}")
        for col in duplicate_columns:
            df = df.drop(col)

    return df

def explore_data(df):
    """
    Perform basic exploration on the DataFrame.
    :param df: PySpark DataFrame to explore.
    """
    # Show the first few rows
    print("First 3 rows:")
    df.show(3)

    # Count the number of rows/observations
    row_count = df.count()
    print(f"\nTotal number of observations: {row_count}")

    # Describe statistics of Points Scored, Assists and Blocks
    try:
        print("\nSummary statistics:")
        df.select("TeamPoints", "Assists", "Blocks").describe().show()
    except Exception as e:
        print(f"Error during describe(): {e}")

def process_data(spark, df):
    """
    Perform data processing using Spark SQL and DataFrame transformations.
    :param spark: SparkSession object.
    :param df: PySpark DataFrame to process.
    """
    # Register the DataFrame as a SQL table
    df.createOrReplaceTempView("nba_stats")

    # Query 1: Top 10 high-scoring games
    print("\nTop 10 high-scoring games:")
    spark.sql("""
        SELECT Team, Date, Game, TeamPoints, OpponentPoints
        FROM nba_stats
        WHERE TeamPoints > 120
        ORDER BY TeamPoints DESC
        LIMIT 10
    """).show()

    # Query 2: Average points per team
    print("\nAverage points per team:")
    spark.sql("""
        SELECT Team, AVG(TeamPoints) AS AvgTeamPoints
        FROM nba_stats
        GROUP BY Team
        ORDER BY AvgTeamPoints DESC
    """).show()

# First Transformation: Creating a winner column for each game and calculate the point difference

def declare_winner(df):
    # Add PointDifference column
    df = df.withColumn("PointDifference", df["TeamPoints"] - df["OpponentPoints"])
    # Add Winner column
    df = df.withColumn(
        "Winner",
        when(df["PointDifference"] > 0, df["Team"]).otherwise(df["Opponent"])
    )
    print("\nDataFrame with Winner column:")
    df.select(
        "Game", "Team", "Opponent", "TeamPoints", "OpponentPoints", "PointDifference", "Winner"
    ).show(10)

# Second Transformation: Calculate the Games Played by Each Team

def calculate_games(df):
    """
    Calculate the total, home, and away games played by each team.
    :param df: PySpark DataFrame containing the NBA stats data.
    :return: PySpark DataFrame with aggregated results.
    """
    # Add HomeGame and AwayGame columns
    df = df.withColumn("HomeGame", when(df["Home"] == "Home", 1).otherwise(0))
    df = df.withColumn("AwayGame", when(df["Home"] == "Away", 1).otherwise(0))

    # Group by team and calculate total, home, and away games
    total_games = df.groupBy("Team").agg(
        count("HomeGame").alias("TotalGames"),
        sum("HomeGame").alias("HomeGames"),
        sum("AwayGame").alias("AwayGames")
    )

    print("\nTotal home and away games played by each team:")
    total_games.select("Team", "HomeGames", "AwayGames", "TotalGames").orderBy("TotalGames", ascending=False).show()

    return total_games



    
