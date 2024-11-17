from pyspark.sql.functions import when, count, sum
import re


def load_data(spark, local_path="dataset/nba_games_stats.csv"):
    """
    Load data from a local CSV file into a PySpark DataFrame, clean up column names, and remove duplicate columns.
    """
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    # Clean column names
    for column_name in df.columns:
        new_col_name = re.sub(r"[^a-zA-Z0-9]", "", column_name)
        df = df.withColumnRenamed(column_name, new_col_name)

    # Remove duplicate columns
    column_set = set()
    duplicate_columns = [
        col for col in df.columns if col in column_set or column_set.add(col)
    ]
    if duplicate_columns:
        print(f"Duplicate Columns Detected: {duplicate_columns}")
        for col in duplicate_columns:
            df = df.drop(col)

    return df


def explore_data(df):
    """
    Perform basic exploration on the DataFrame.
    """
    output = {}

    # Collect first rows
    output["first_rows"] = df.limit(3).toPandas().to_markdown(index=False)

    # Count rows
    output["row_count"] = df.count()

    # Collect summary statistics
    try:
        summary_df = df.select("TeamPoints", "Assists", "Blocks").describe().toPandas()
        output["summary_stats"] = summary_df.to_markdown(index=False)
    except Exception as e:
        output["summary_stats"] = f"Error: {e}"

    return output


# Queries


def process_data(spark, df):
    """
    Perform SQL queries on the DataFrame.
    """
    output = {}

    # Register the DataFrame as a SQL table
    df.createOrReplaceTempView("nba_stats")

    # Query 1: Top 10 high-scoring games
    query1_df = spark.sql(
        """
        SELECT Team, Date, Game, TeamPoints, OpponentPoints
        FROM nba_stats
        WHERE TeamPoints > 120
        ORDER BY TeamPoints DESC
        LIMIT 10
    """
    )
    output["query1"] = query1_df.toPandas().to_markdown(index=False)

    # Query 2: Average points per team
    query2_df = spark.sql(
        """
        SELECT Team, AVG(TeamPoints) AS AvgTeamPoints
        FROM nba_stats
        GROUP BY Team
        ORDER BY AvgTeamPoints DESC
    """
    )
    output["query2"] = query2_df.toPandas().to_markdown(index=False)

    return output


# First Transformation: Creating a winner column for each game and calculate the point difference


def declare_winner(df):
    """
    Add a winner column to the DataFrame.
    """
    df = df.withColumn("PointDifference", df["TeamPoints"] - df["OpponentPoints"])
    df = df.withColumn(
        "Winner", when(df["PointDifference"] > 0, df["Team"]).otherwise(df["Opponent"])
    )
    winner_sample_df = df.select(
        "Game",
        "Team",
        "Opponent",
        "TeamPoints",
        "OpponentPoints",
        "PointDifference",
        "Winner",
    ).limit(10)
    return winner_sample_df.toPandas().to_markdown(index=False)


# Second Transformation: Calculate the Games Played by Each Team


def calculate_games(df):
    """
    Calculate the total games played by each team.
    """
    df = df.withColumn("HomeGame", when(df["Home"] == "Home", 1).otherwise(0))
    df = df.withColumn("AwayGame", when(df["Home"] == "Away", 1).otherwise(0))
    total_games = df.groupBy("Team").agg(
        count("HomeGame").alias("TotalGames"),
        sum("HomeGame").alias("HomeGames"),
        sum("AwayGame").alias("AwayGames"),
    )
    games_summary_df = total_games.select(
        "Team", "HomeGames", "AwayGames", "TotalGames"
    ).orderBy("TotalGames", ascending=False)
    return games_summary_df.toPandas().to_markdown(index=False)
