from pyspark.sql import SparkSession
from project.processing import *

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("NBAStatsProject").getOrCreate()
    
    # Load data from local file
    df = load_data(spark)
    
    # Explore data
    explore_data(df)

    # Process data (register as SQL table and do SQL Queries)
    process_data(spark, df)
    
    # Transformation 1
    declare_winner(df)

    # Transformation 2
    calculate_games(df)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
