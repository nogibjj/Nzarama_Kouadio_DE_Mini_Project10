import pytest
from pyspark.sql import SparkSession
from project.processing import load_data, explore_data, declare_winner, calculate_games


@pytest.fixture(scope="module")
def spark():
    """Fixture to initialize a SparkSession."""
    return SparkSession.builder.appName("NBAStatsTest").getOrCreate()


@pytest.fixture(scope="module")
def sample_data(spark):
    """Fixture to create a sample DataFrame for testing."""
    data = [
        ("Game1", "TeamA", "TeamB", 120, 110, "Home"),
        ("Game2", "TeamB", "TeamA", 105, 115, "Away"),
        ("Game3", "TeamA", "TeamC", 130, 100, "Home"),
    ]
    columns = ["Game", "Team", "Opponent", "TeamPoints", "OpponentPoints", "Home"]
    return spark.createDataFrame(data, schema=columns)


def test_load_data(spark):
    """Test the load_data function."""
    df = load_data(spark, local_path="dataset/nba_games_stats.csv")
    assert df is not None
    assert len(df.columns) > 0


def test_explore_data(sample_data):
    """Test the explore_data function."""
    output = explore_data(sample_data)
    assert "first_rows" in output
    assert "row_count" in output
    assert output["row_count"] == 3  # Check sample data row count
    assert "summary_stats" in output


def test_declare_winner(sample_data):
    """Test the declare_winner function."""
    output = declare_winner(sample_data)
    assert output is not None


def test_calculate_games(sample_data):
    """Test the calculate_games function."""
    output = calculate_games(sample_data)
    assert output is not None
