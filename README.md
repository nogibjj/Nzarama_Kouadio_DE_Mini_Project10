
# DUKE MIDS DE Mini Project 10: NBA Stats Analysis Project


## Project Overview
This project showcases data engineering and analytics skills using PySpark to analyze NBA game statistics. The pipeline involves data ingestion, exploration, SQL querying, and advanced transformations to gain insights into team performances. Results were documented in a Markdown file, created programmatically.

## Dataset and Data Manipulation

### Dataset Overview:
We used an NBA games statistics dataset containing metrics such as team points, opponent points, assists, blocks, and game outcomes. The dataset was cleaned and prepared for analysis using PySpark's data manipulation capabilities.

#### Data Source:
* [NBA Games Stats CSV](https://raw.githubusercontent.com/anlane611/datasets/refs/heads/main/nba_games_stats.csv)

### Key Objectives:
1. **Data Exploration**: Understand the structure and key insights from the dataset.
2. **SQL Queries**: Use Spark SQL to extract specific insights like high-scoring games and team averages.
3. **Data Transformations**: Perform advanced transformations, such as calculating point differences and identifying winners.
4. **Game Analysis**: Analyze total games played, split into home and away matches.

### Key Findings:
- **Top Scoring Games**: Identified the 10 highest-scoring games based on team points.
- **Winning Teams**: Determined the winner for each game based on point differences.
- **Game Totals**: Analyzed the number of games played by each team, split into home and away.

## Project Structure

```plaintext
├── dataset/                     # Contains the NBA dataset
│   └── nba_games_stats.csv      # Source dataset
├── project/                     
│   ├── main.py                  # Main script to execute the analysis pipeline
│   ├── processing.py            # Functions for data loading, exploration, queries, and transformations
├── README.md                    # Project documentation
├── output.md                    # Automatically generated analysis results
├── requirements.txt             # Dependencies for running the project
```

## Steps and Features

### Data Exploration
- Displayed the first few rows of the dataset for a quick preview.
- Counted the total number of records.
- Generated summary statistics for key columns like points, assists, and blocks.

### Register the DataFrame as a Temporary SQL Table
- Registered the cleaned dataset as a temporary SQL table (`nba_stats`) to facilitate SQL-based analytics.

### Perform Spark SQL Queries
- **Top 10 High-Scoring Games**: Extracted games where teams scored over 120 points, sorted by highest scores.
- **Average Points per Team**: Calculated the average points scored by each team, ranked in descending order.

### Apply Data Transformations
- **Winner Identification**: Added a new column (`Winner`) to identify the winning team based on point differences.
- **Game Totals**: Grouped data by teams to calculate the number of home and away games played by each team.

## Setup Instructions

### Prerequisites
- Install Python 3.8+ and PySpark.
- Clone this repository to your local environment.

### Installation Steps
1. **Clone the Repository**:
    ```bash
    git clone https://github.com/nogibjj/Nzarama_Kouadio_DE_Mini_Project10.git
    ```
2. **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## CI/CD Pipeline
The project includes a CI/CD pipeline for automated testing and quality assurance:
- **Formatting**: Code is formatted with `black`.
- **Linting**: Ensures code quality with `flake8`.
- **Testing**: Runs test cases using `pytest`.

## Outputs
The analysis outputs were saved in `output.md` for documentation purposes. Key sections include:
1. Data Exploration
2. SQL Query Results
3. Transformation Insights

