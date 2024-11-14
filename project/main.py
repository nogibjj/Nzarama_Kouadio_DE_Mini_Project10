from pyspark.sql import SparkSession
from project.processing import load_data, explore_data

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("NBAStatsProject").getOrCreate()
    
    # Load data from local file
    df = load_data(spark)
    
    # Explore data
    explore_data(df)
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
