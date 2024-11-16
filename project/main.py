from pyspark.sql import SparkSession
from project.processing import *

def save_to_markdown(filename, content):
    """
    Save the given content to a markdown file.
    :param filename: Name of the markdown file.
    :param content: Content to write into the file.
    """
    with open(filename, "w") as f:
        f.write(content)

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("NBAStatsProject").getOrCreate()

    # Initialize content for markdown
    markdown_content = "# NBA Stats Analysis\n\n"

    # Load data from local file
    df = load_data(spark)
    markdown_content += "## Data Loading\n\nData loaded successfully.\n\n"

    # Explore data
    exploration_output = explore_data(df)
    markdown_content += "## Data Exploration\n\n"
    markdown_content += "### First 3 Rows\n\n" + exploration_output["first_rows"] + "\n\n"
    markdown_content += f"### Total Number of Observations/Rows: {exploration_output['row_count']}\n\n"
    markdown_content += "### Summary Statistics\n\n" + exploration_output["summary_stats"] + "\n\n"

    # Process data (register as SQL table and do SQL Queries)
    process_output = process_data(spark, df)
    markdown_content += "## SQL Queries\n\n"
    markdown_content += "### Query 1: Top 10 High-Scoring Games\n\n" + process_output["query1"] + "\n\n"
    markdown_content += "### Query 2: Average Points Per Team\n\n" + process_output["query2"] + "\n\n"

    # Transformation 1
    winner_output = declare_winner(df)
    markdown_content += "## Point differences in a match and winner declaration (for the 1st 10 rows)\n\n" + winner_output + "\n\n"

    # Transformation 2
    games_output = calculate_games(df)
    markdown_content += "## Games Played Analysis: Number of games played per team\n\n" + games_output + "\n\n"

    # Stop the Spark session
    spark.stop()

    # Save all outputs to a markdown file
    save_to_markdown("output.md", markdown_content)

if __name__ == "__main__":
    main()
