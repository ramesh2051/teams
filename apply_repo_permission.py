import os
import csv
import requests
import logging
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
GITHUB_TOKEN = os.getenv("TARGET_GH_PAT")

if not GITHUB_TOKEN:
    raise Exception("TARGET_GITHUB_TOKEN not found in .env file.")

CSV_FILE = "user_repo_permission.csv"
API_URL = "https://api.github.com"

# === LOGGING CONFIGURATION ===
file_handler = logging.FileHandler("apply-permissions.log", encoding='utf-8')
console_handler = logging.StreamHandler()

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

file_logger = logging.getLogger('file_only')
file_logger.setLevel(logging.INFO)
file_logger.addHandler(file_handler)
file_logger.propagate = False

def handle_rate_limit(response):
    remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
    reset_time = int(response.headers.get("X-RateLimit-Reset", time.time()))
    if remaining == 0:
        wait_time = reset_time - int(time.time())
        if wait_time > 0:
            logging.warning(f"Rate limit reached. Waiting for {wait_time} seconds...")
            time.sleep(wait_time + 1)

def check_repo_exists(org, repo):
    url = f"{API_URL}/repos/{org}/{repo}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    response = requests.get(url, headers=headers)
    handle_rate_limit(response)
    return response.status_code == 200

def check_user_exists(username):
    url = f"{API_URL}/users/{username}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    response = requests.get(url, headers=headers)
    handle_rate_limit(response)
    return response.status_code == 200

def check_user_permission(org, repo, username):
    url = f"{API_URL}/repos/{org}/{repo}/collaborators/{username}/permission"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    response = requests.get(url, headers=headers)
    handle_rate_limit(response)

    if response.status_code == 200:
        data = response.json()
        permission = data.get("permission", None)
        logging.debug(f"Permission check for {username} on {org}/{repo}: {data}")
        return permission
    return None

def normalize_permission(p, compare_mode=False):
    p = p.lower()
    if compare_mode:
        # Normalize for comparison (GitHub API vs CSV)
        return {
            "pull": "read",
            "read": "read",
            "push": "write",
            "write": "write",
            "triage": "triage",
            "maintain": "maintain",
            "admin": "admin"
        }.get(p, p)
    else:
        # Normalize for API request
        return {
            "read": "pull",
            "write": "push",
            "triage": "triage",
            "maintain": "maintain",
            "admin": "admin",
            "pull": "pull",
            "push": "push"
        }.get(p, p)

def permission_hierarchy():
    return {
        "read": 1,
        "triage": 2,
        "write": 3,
        "maintain": 4,
        "admin": 5
    }

def is_permission_sufficient(current_permission, required_permission):
    hierarchy = permission_hierarchy()
    current_level = hierarchy.get(normalize_permission(current_permission, compare_mode=True), 0)
    required_level = hierarchy.get(normalize_permission(required_permission, compare_mode=True), 0)
    return current_level >= required_level

def add_user_permission(org, repo, username, permission):
    url = f"{API_URL}/repos/{org}/{repo}/collaborators/{username}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    data = {
        "permission": normalize_permission(permission, compare_mode=False)
    }
    response = requests.put(url, headers=headers, json=data)
    handle_rate_limit(response)

    if response.status_code in [201, 204]:
        logging.info(f"Set {username} permission to {permission} on {org}/{repo}.")
        return True
    else:
        logging.error(f"Failed to set {username} permission on {org}/{repo}: {response.status_code} {response.text}")
        return False

def main():
    success_count = 0
    error_count = 0
    skip_count = 0
    unmapped_count = 0

    try:
        with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            for row_num, row in enumerate(reader, start=2):
                try:
                    target_org = row["Target Organization"]
                    target_repo = row["Target Repository"]
                    classic_username = row["Username"]
                    emu_username = row.get("EMU User", "").strip()
                    permission = row["Normalized Permission"]

                    if not emu_username or emu_username.upper() == "UNMAPPED":
                        logging.warning(f"Skipping row {row_num}: No EMU User mapping for {classic_username}")
                        unmapped_count += 1
                        continue

                    if not all([target_org, target_repo, emu_username, permission]):
                        logging.warning(f"Skipping row {row_num}: Missing required data")
                        skip_count += 1
                        continue

                    if add_user_permission(target_org, target_repo, emu_username, permission):
                        success_count += 1
                    else:
                        error_count += 1

                except Exception as e:
                    logging.error(f"Error processing row {row_num}: {e}")
                    error_count += 1

    except FileNotFoundError:
        logging.error(f"CSV file '{CSV_FILE}' not found. Please run the fetch script first.")
        return
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return

if __name__ == "__main__":
    main()
