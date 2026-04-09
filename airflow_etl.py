import os
import requests 

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

#Extracting commits from main airflow repo on github
def extract():

    page = 1
    all_commits = []

    while True:
        #sending get request with params that we need to filter for
        response = requests.get(
            "https://api.github.com/repos/apache/airflow/commits",
            headers = {"Accept": "application/vnd.github+json",
                       "X-GitHub-Api-Version": "2026-03-10"},
            params = {
                "sha": "main", 
                "since": "2026-01-01T00:00:00Z",
                "until": "2026-02-01T00:00:00Z",
                "per_page": 100,
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
            print(f"No commits left to extract.")
            print(f"Total commits found: {len(all_commits)}")
            break

        all_commits.extend(commits) #adds each commit to the list

        page += 1 

    return all_commits 

def transform(data):
    
    #getting nested objects
    commit = data.get("commit")
    git_author = data.get("author")
    git_committer = data.get("committer")
    author = commit.get("author")
    committer = commit.get("committer")
    verification = commit.get("verification")

    #only getting data that is needed
    return {
     "sha": data.get("sha"),
     "message": commit.get("message"),
     "author_name": author.get("name"),
     "author_email": author.get("email"),
     "authored_at": author.get("date"),
     "authored_date": author.get("date")[:10],
     "committer_name": committer.get("name"),
     "committer_email": committer.get("email"),
     "committer_at": committer.get("date"),
     "committer_date": committer.get("date")[:10],
     "verified": verification.get("verified"),
     "reason": verification.get("reason"),
     "author_login": git_author.get("login"),
     "author_id": git_author.get("id"),
     "author_type": git_author.get("type"),
     "commiter_login": git_committer.get("login"),
     "commiter_id": git_committer.get("id"),
     "commiter_type": git_committer.get("type"),
     "html_url": data.get("html_url")
    }

extracted_commits = extract()
print(transform(extracted_commits[0]))