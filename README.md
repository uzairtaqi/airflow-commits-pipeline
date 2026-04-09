# Airflow Commits Pipeline

This is an ETL pipeline that extracts commit data from the Apache Airflow GitHub repository for January 2026 and loads it into a SQLite database.

## Setup

### Requirements
- Python 3.x
- Install the requests library: `pip install requests`

### GitHub Token (Recommended)
Without a token GitHub limits you to 60 requests per hour. To avoid this:

1. Go to GitHub → Settings → Developer Settings → Personal Access Tokens
2. Generate a new fine-grained token
3. Set it in your terminal before running:
   - Windows: `set GITHUB_TOKEN=your_token`
   - Mac/Linux: `export GITHUB_TOKEN=your_token`

### Running the script

`py airflow_etl.py`

### Viewing the data
Install DB Browser for SQLite to view the database: https://sqlitebrowser.org/dl/

## Approach

I started by looking at the raw API response directly in my browser to understand the structure of the data and identify useful fields. From there I built the pipeline in three steps.

For the extract step I used the GitHub REST API to pull all commits from the main branch of the Apache Airflow repository for January 2026, paginating through the results 100 commits at a time.

For the transform step I flattened the nested JSON response into a single flat structure. I kept fields that I felt were most useful for downstream analysis — author and committer details, timestamps, verification status, and GitHub account information. I also cleaned the data by normalizing emails to lowercase and deriving a date only column from the full timestamp for easier querying. The transformation logic is intentionally simple — the data coming from GitHub is already reliable and anything more complex like creating derived fields felt like it would be better handled downstream in SQL.

For the load step I chose SQLite because it requires no setup or configuration — the database is just a file. I loaded everything into a single table because there aren't really any measures here, its just commit metadata, so its more of an event log than a fact table. Normalizing into a star schema would just be splitting attributes across tables for the sake of it without anything to actually aggregate.

## Known Limitations

- The date range is hardcoded for January 2026. To run for a different period the code would need to be updated directly.
- SQLite was chosen for simplicity and portability but would not be suitable for a production environment. A proper database like PostgreSQL or a cloud warehouse would be more appropriate.
- No Rate Limit Handling since it's only 1 month's of commits it can be done easily with a token. 
  
