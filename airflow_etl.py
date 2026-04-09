import os
import requests 
import sqlite3
import time

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

#Extracting commits from main airflow repo on github
def extract():

    headers = {"Accept": "application/vnd.github+json",
               "X-GitHub-Api-Version": "2026-03-10"}
    
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    page = 1
    all_commits = []
    url = "https://api.github.com/repos/apache/airflow/commits"
    sha = "main"
    since = "2026-01-01T00:00:00Z"
    until = "2026-02-01T00:00:00Z"
    per_page = 100

    while True:
        #sending get request with params that we need to filter for
        response = requests.get(url,
            headers = headers,
            params = {
                "sha": sha,
                "since": since,
                "until": until,
                "per_page": per_page,
                "page": page
            }
        )

        #when HTTP repsonse isn't OK, exit loop
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            break 

        commits = response.json()

        #when no more commits exist then exit loop
        if len(commits) == 0:
            print(f"No more commits left to extract.")
            print(f"Total commits found: {len(all_commits)}")
            break

        all_commits.extend(commits) #adds each commit to the list

        page += 1 
        time.sleep(0.5)

    return all_commits 

#Transforming the data from raw
def transform(data):
    
    #getting nested objects
    commit = data.get("commit") or {}
    git_author = data.get("author") or {}
    git_committer = data.get("committer") or {}
    author = commit.get("author") or {}
    committer = commit.get("committer") or {}
    verification = commit.get("verification") or {}

    #getting data points
    return {
     "sha": data.get("sha"),
     "message": commit.get("message"),
     "author_name": (author.get("name") or "").strip() or None,
     "author_email": (author.get("email") or "").lower(),
     "authored_at": author.get("date"),
     "authored_date": author.get("date")[:10],
     "committer_name": (committer.get("name") or "").strip() or None,
     "committer_email": (committer.get("email") or "").lower(),
     "committer_at": committer.get("date"),
     "committer_date": committer.get("date")[:10],
     "verified": verification.get("verified"),
     "reason": verification.get("reason"),
     "author_login": git_author.get("login"),
     "author_id": git_author.get("id"),
     "author_type": git_author.get("type"),
     "committer_login": git_committer.get("login"),
     "committer_id": git_committer.get("id"),
     "committer_type": git_committer.get("type"),
     "html_url": data.get("html_url")
    }

#Loading data into SQLite
def load(commits):

    #connect to db
    conn = sqlite3.connect('airflow_commits.db')
    cursor = conn.cursor()

    #create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS commits (
            sha TEXT PRIMARY KEY,
            message TEXT, 
            author_name TEXT,
            author_email TEXT,
            authored_at TEXT,
            authored_date TEXT, 
            committer_name TEXT,
            committer_email TEXT,
            committer_at TEXT,
            committer_date TEXT,
            verified INTEGER,
            reason TEXT,
            author_login TEXT,
            author_id INTEGER,
            author_type TEXT,
            committer_login TEXT,
            committer_id INTEGER,
            committer_type TEXT,
            html_url TEXT
                   )
                   ''')
    
    print("Table Created")

    #batch insert data
    cursor.executemany('''
        INSERT OR REPLACE INTO commits (
            sha, message, author_name, author_email, authored_at, authored_date,
            committer_name, committer_email, committer_at, committer_date,
            verified, reason, author_login, author_id, author_type,
            committer_login, committer_id, committer_type, html_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', [(
        c['sha'], c['message'], c['author_name'], c['author_email'],
        c['authored_at'], c['authored_date'], c['committer_name'],
        c['committer_email'], c['committer_at'], c['committer_date'],
        c['verified'], c['reason'], c['author_login'], c['author_id'],
        c['author_type'], c['committer_login'], c['committer_id'],
        c['committer_type'], c['html_url']
    ) for c in commits])

    print(f"Data Inserted: {len(commits)} rows")

    conn.commit()
    conn.close()

def main():
    #Call Extract
    extracted_commits = extract()
    if not extracted_commits:
        print("No commits extracted. Ending.")
        quit()

    #Call Transform
    records = []
    for commit in extracted_commits:
        result = transform(commit)
        records.append(result)

    #Call Load 
    load(records)

if __name__ == "__main__":
    main()