import os
import requests 

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")


def extract():

    page = 1
    all_commits = []

    while True:
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

        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            break 

        commits = response.json()

        if len(commits) == 0:
            print(f"No commits left to extract.")
            print(f"Total commits found: {len(all_commits)}")
            break

        all_commits.extend(commits)

        page += 1 

    return all_commits 

extracted_commits = extract()
print(extracted_commits[0])
     