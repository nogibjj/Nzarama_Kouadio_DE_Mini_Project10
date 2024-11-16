# NBA Stats Analysis

## Data Loading

Data loaded successfully.

## Data Exploration

### First Rows

| Team   |   Game | Date       | Home   | Opponent   | WINorLOSS   |   TeamPoints |   OpponentPoints |   FieldGoalsAttempted |   X3PointShotsAttempted |   FreeThrowsAttempted |   OffRebounds |   TotalRebounds |   Assists |   Steals |   Blocks |   Turnovers |   TotalFouls |   OppFieldGoalsAttempted |   Opp3PointShotsAttempted |   OppFreeThrowsAttempted |   OppOffRebounds |   OppTotalRebounds |   OppAssists |   OppSteals |   OppBlocks |   OppTurnovers |   OppTotalFouls |
|:-------|-------:|:-----------|:-------|:-----------|:------------|-------------:|-----------------:|----------------------:|------------------------:|----------------------:|--------------:|----------------:|----------:|---------:|---------:|------------:|-------------:|-------------------------:|--------------------------:|-------------------------:|-----------------:|-------------------:|-------------:|------------:|------------:|---------------:|----------------:|
| ATL    |      1 | 2014-10-29 | Away   | TOR        | L           |          102 |              109 |                    80 |                      22 |                    17 |            10 |              42 |        26 |        6 |        8 |          17 |           24 |                       90 |                        26 |                       33 |               16 |                 48 |           26 |          13 |           9 |              9 |              22 |
| ATL    |      2 | 2014-11-01 | Home   | IND        | W           |          102 |               92 |                    69 |                      20 |                    33 |             3 |              37 |        26 |       10 |        6 |          12 |           20 |                       81 |                        32 |                       21 |               11 |                 44 |           25 |           5 |           5 |             18 |              26 |
| ATL    |      3 | 2014-11-05 | Away   | SAS        | L           |           92 |               94 |                    92 |                      25 |                    11 |            10 |              37 |        26 |       14 |        5 |          13 |           25 |                       69 |                        17 |                       38 |               11 |                 50 |           25 |           7 |           9 |             19 |              15 |

### Total Rows: 9840

### Summary Statistics

| summary   |   TeamPoints |    Assists |     Blocks |
|:----------|-------------:|-----------:|-----------:|
| count     |     9840     | 9840       | 9840       |
| mean      |      103.652 |   22.5465  |    4.82764 |
| stddev    |       12.188 |    5.12299 |    2.53684 |
| min       |       64     |    6       |    0       |
| max       |      149     |   47       |   18       |

## SQL Queries

### Query 1: Top 10 High-Scoring Games

| Team   | Date       |   Game |   TeamPoints |   OpponentPoints |
|:-------|:-----------|-------:|-------------:|-----------------:|
| GSW    | 2016-11-23 |     15 |          149 |              106 |
| MIA    | 2018-03-19 |     71 |          149 |              141 |
| HOU    | 2017-12-31 |     35 |          148 |              142 |
| OKC    | 2018-01-20 |     46 |          148 |              124 |
| DET    | 2015-12-18 |     28 |          147 |              144 |
| DEN    | 2017-11-17 |     15 |          146 |              114 |
| DAL    | 2015-04-10 |     79 |          144 |              143 |
| CHI    | 2015-12-18 |     24 |          144 |              147 |
| MIN    | 2016-04-13 |     82 |          144 |              109 |
| GSW    | 2017-01-28 |     47 |          144 |               98 |

### Query 2: Average Points Per Team

| Team   |   AvgTeamPoints |
|:-------|----------------:|
| GSW    |        113.549  |
| HOU    |        109.543  |
| LAC    |        107.22   |
| CLE    |        107.165  |
| OKC    |        107.155  |
| TOR    |        106.302  |
| DEN    |        106.268  |
| POR    |        105.375  |
| BOS    |        104.777  |
| WAS    |        104.601  |
| NOP    |        104.534  |
| MIN    |        103.814  |
| PHO    |        103.723  |
| SAS    |        103.695  |
| ATL    |        102.976  |
| CHO    |        102.668  |
| IND    |        102.537  |
| SAC    |        102.39   |
| BRK    |        102.259  |
| LAL    |        102.11   |
| CHI    |        102.058  |
| DAL    |        101.936  |
| MIL    |        101.747  |
| DET    |        101.39   |
| ORL    |        100.558  |
| PHI    |        100.409  |
| MIA    |        100.329  |
| NYK    |         99.7622 |
| UTA    |         99.4177 |
| MEM    |         99.3049 |

## Winner Declaration

|   Game | Team   | Opponent   |   TeamPoints |   OpponentPoints |   PointDifference | Winner   |
|-------:|:-------|:-----------|-------------:|-----------------:|------------------:|:---------|
|      1 | ATL    | TOR        |          102 |              109 |                -7 | TOR      |
|      2 | ATL    | IND        |          102 |               92 |                10 | ATL      |
|      3 | ATL    | SAS        |           92 |               94 |                -2 | SAS      |
|      4 | ATL    | CHO        |          119 |              122 |                -3 | CHO      |
|      5 | ATL    | NYK        |          103 |               96 |                 7 | ATL      |
|      6 | ATL    | NYK        |           91 |               85 |                 6 | ATL      |
|      7 | ATL    | UTA        |          100 |               97 |                 3 | ATL      |
|      8 | ATL    | MIA        |          114 |              103 |                11 | ATL      |
|      9 | ATL    | CLE        |           94 |              127 |               -33 | CLE      |
|     10 | ATL    | LAL        |          109 |              114 |                -5 | LAL      |

## Games Played Analysis

| Team   |   HomeGames |   AwayGames |   TotalGames |
|:-------|------------:|------------:|-------------:|
| GSW    |         164 |         164 |          328 |
| DET    |         164 |         164 |          328 |
| LAL    |         164 |         164 |          328 |
| NYK    |         164 |         164 |          328 |
| CHO    |         164 |         164 |          328 |
| LAC    |         164 |         164 |          328 |
| UTA    |         164 |         164 |          328 |
| BOS    |         164 |         164 |          328 |
| TOR    |         164 |         164 |          328 |
| SAS    |         164 |         164 |          328 |
| POR    |         164 |         164 |          328 |
| DEN    |         164 |         164 |          328 |
| BRK    |         164 |         164 |          328 |
| DAL    |         164 |         164 |          328 |
| CLE    |         164 |         164 |          328 |
| MIA    |         164 |         164 |          328 |
| OKC    |         164 |         164 |          328 |
| PHO    |         164 |         164 |          328 |
| MIN    |         164 |         164 |          328 |
| MEM    |         164 |         164 |          328 |
| SAC    |         164 |         164 |          328 |
| ATL    |         164 |         164 |          328 |
| PHI    |         164 |         164 |          328 |
| WAS    |         164 |         164 |          328 |
| NOP    |         164 |         164 |          328 |
| IND    |         164 |         164 |          328 |
| HOU    |         164 |         164 |          328 |
| CHI    |         164 |         164 |          328 |
| ORL    |         164 |         164 |          328 |
| MIL    |         164 |         164 |          328 |

