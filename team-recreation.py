import os
import csv
import logging
import requests
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('team_recreation.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class GitHubTeamRecreationFetcher:
    def __init__(self):
        self.token = os.getenv('TARGET_GH_PAT')
        self.org_name = os.getenv('TARGET_GH_ORG')
        self.input_csv = os.getenv('INPUT_CSV_FILE', 'github_teams.csv')
        if not self.token: raise ValueError("TARGET_GH_PAT environment variable is required")
        if not self.org_name: raise ValueError("TARGET_GH_ORG environment variable is required")
        self.headers = {
            'Authorization': f'token {self.token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        # Rate limiting settings
        self.rate_limit_delay = 1.0  # Default delay between requests in seconds
        self.max_retries = 3
        self.retry_delay = 60  # Delay when rate limit is hit
        logger.info(f"Initialized GitHub Team Recreation Fetcher for org: {self.org_name}, input CSV: {self.input_csv}")

    def make_api_request(self, method, url, **kwargs):
        """Make API request with rate limiting and retry logic."""
        for attempt in range(self.max_retries):
            try:
                # Add delay between requests
                time.sleep(self.rate_limit_delay)
                
                response = requests.request(method, url, headers=self.headers, **kwargs)
                
                # Check rate limit headers
                remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                
                # Log rate limit status
                if remaining <= 100:  # Warning when getting close to limit
                    logger.warning(f"Rate limit low: {remaining} requests remaining")
                
                # Handle rate limit exceeded
                if response.status_code == 403 and 'rate limit exceeded' in response.text.lower():
                    current_time = time.time()
                    sleep_time = max(reset_time - current_time, self.retry_delay)
                    logger.warning(f"Rate limit exceeded. Sleeping for {sleep_time:.0f} seconds...")
                    time.sleep(sleep_time)
                    continue
                
                # Handle other 4xx/5xx errors with exponential backoff
                # Don't retry on 404 (not found) as it's a valid response
                if response.status_code >= 400 and response.status_code != 404:
                    if attempt < self.max_retries - 1:
                        backoff_time = (2 ** attempt) * self.rate_limit_delay
                        logger.warning(f"Request failed with {response.status_code}. Retrying in {backoff_time:.1f}s...")
                        time.sleep(backoff_time)
                        continue
                
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    backoff_time = (2 ** attempt) * self.rate_limit_delay
                    logger.warning(f"Request exception: {e}. Retrying in {backoff_time:.1f}s...")
                    time.sleep(backoff_time)
                    continue
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts: {e}")
                    raise
        
        return response

    def read_teams_from_csv(self):
        logger.info(f"Reading team data from CSV: {self.input_csv}")
        if not os.path.exists(self.input_csv): raise FileNotFoundError(f"CSV not found: {self.input_csv}")
        with open(self.input_csv, 'r', encoding='utf-8') as csvfile:
            return list(csv.DictReader(csvfile))

    def create_team(self, team_name, team_slug, description="", privacy="closed", parent_team_id=None):
        if self.check_team_exists(team_name):
            logger.info(f"[SKIP] Team exists: {team_name}")
            return self.check_team_exists(team_name)
        payload = {"name": team_name, "description": description, "privacy": privacy}
        if parent_team_id: payload["parent_team_id"] = parent_team_id
        resp = self.make_api_request("POST", f"https://api.github.com/orgs/{self.org_name}/teams", json=payload)
        if resp.status_code == 201:
            logger.info(f"[CREATED] Team: {team_name} (ID: {resp.json().get('id')})")
            return resp.json()
        logger.error(f"[ERROR] Creating team {team_name}: {resp.status_code} - {resp.text}")
        return None

    def add_member_to_team(self, team_slug, username, role="member"):
        resp = self.make_api_request("PUT", f"https://api.github.com/orgs/{self.org_name}/teams/{team_slug}/memberships/{username}",
                            json={"role": role})
        if resp.status_code in [200, 201]:
            logger.info(f"[ADDED] Member {username} to {team_slug} with role: {role}")
            return True
        logger.error(f"[ERROR] Adding {username} to {team_slug} with role {role}: {resp.status_code} - {resp.text}")
        return False

    def recreate_teams_from_csv(self):
        teams_data = self.read_teams_from_csv()
        unique_teams = {}
        for row in teams_data:
            slug = row['team_slug']
            if slug not in unique_teams:
                unique_teams[slug] = {
                    'team_name': row['team_name'], 'team_description': row['team_description'],
                    'team_privacy': row.get('team_privacy', 'closed'), 'parent_team': row.get('parent_team', '').strip(),
                    'members': []  # Store tuples of (username, role)
                }
            # Only use EMU username for member addition
            if row.get('emu_members') and row.get('member_role'):
                emu_username = row['emu_members'].strip()
                member_role = row['member_role'].strip()
                # Avoid duplicates by checking if this member is already in the list
                if (emu_username, member_role) not in unique_teams[slug]['members']:
                    unique_teams[slug]['members'].append((emu_username, member_role))

        team_id_map, team_name_to_slug = {}, {}
        created, skipped = {}, {}
        
        # Create mapping from team name to slug for parent team resolution
        for slug, info in unique_teams.items():
            team_name_to_slug[info['team_name']] = slug
        
        logger.info(f"Processing {len(unique_teams)} teams")
        
        # Debug: Show what members will be added to each team
        for slug, info in unique_teams.items():
            if info['members']:
                logger.info(f"Team '{info['team_name']}' will have {len(info['members'])} members: {info['members']}")
            else:
                logger.info(f"Team '{info['team_name']}' will have no members")
        
        # First pass: Create parent teams and build team_id_map
        for slug, info in unique_teams.items():
            if not info['parent_team']:
                existing_team = self.check_team_exists(info['team_name'])
                if existing_team:
                    logger.info(f"[EXISTS] Team already exists: {info['team_name']} (ID: {existing_team['id']})")
                    team_id_map[slug] = existing_team['id']
                    created[slug] = existing_team
                    # Add members with their roles
                    for member_username, member_role in info['members']:
                        self.add_member_to_team(slug, member_username, member_role)
                else:
                    team = self.create_team(info['team_name'], slug, info['team_description'], info['team_privacy'])
                    if team:
                        team_id_map[slug] = team['id']
                        created[slug] = team
                        # Add members with their roles
                        for member_username, member_role in info['members']:
                            self.add_member_to_team(slug, member_username, member_role)
                    else: skipped[slug] = info

        # Second pass: Create child teams
        for slug, info in unique_teams.items():
            if info['parent_team']:
                # Convert parent team name to slug, then get the ID
                parent_slug = team_name_to_slug.get(info['parent_team'])
                parent_id = team_id_map.get(parent_slug) if parent_slug else None
                
                if not parent_id:
                    logger.warning(f"[WARNING] Parent team '{info['parent_team']}' not found for team '{info['team_name']}'")
                else:
                    logger.info(f"[INFO] Creating child team '{info['team_name']}' under parent '{info['parent_team']}' (ID: {parent_id})")
                
                existing_team = self.check_team_exists(info['team_name'])
                if existing_team:
                    logger.info(f"[EXISTS] Child team already exists: {info['team_name']} (ID: {existing_team['id']})")
                    team_id_map[slug] = existing_team['id']
                    created[slug] = existing_team
                    # Add members with their roles
                    for member_username, member_role in info['members']:
                        self.add_member_to_team(slug, member_username, member_role)
                else:
                    team = self.create_team(info['team_name'], slug, info['team_description'], info['team_privacy'], parent_id)
                    if team:
                        team_id_map[slug] = team['id']
                        created[slug] = team
                        # Add members with their roles
                        for member_username, member_role in info['members']:
                            self.add_member_to_team(slug, member_username, member_role)
                    else: skipped[slug] = info

        total = len(created) + len(skipped)
        logger.info(f"SUMMARY: Created={len(created)}, Skipped={len(skipped)}, Total={total}")
        return {**created, **skipped}

    def check_team_exists(self, team_name):
        page = 1
        while True:
            resp = self.make_api_request("GET", f"https://api.github.com/orgs/{self.org_name}/teams",
                                params={'page': page, 'per_page': 100})
            if resp.status_code != 200: break
            teams = resp.json()
            if not teams: break
            for team in teams:
                if team['name'] == team_name: return team
            page += 1
        return None

    def test_github_connection(self):
        user_resp = self.make_api_request("GET", "https://api.github.com/user")
        if user_resp.status_code != 200:
            logger.error(f"[ERROR] Auth failed: {user_resp.status_code} - {user_resp.text}")
            return False
        org_resp = self.make_api_request("GET", f"https://api.github.com/orgs/{self.org_name}")
        if org_resp.status_code != 200:
            logger.error(f"[ERROR] Org access failed: {org_resp.status_code} - {org_resp.text}")
            return False
        logger.info(f"[OK] Authenticated as {user_resp.json().get('login')} for org {self.org_name}")
        return True

    def check_rate_limit_status(self):
        """Check and display current rate limit status."""
        resp = self.make_api_request("GET", "https://api.github.com/rate_limit")
        if resp.status_code == 200:
            rate_limit = resp.json()
            core_limit = rate_limit['resources']['core']
            remaining = core_limit['remaining']
            limit = core_limit['limit']
            reset_time = core_limit['reset']
            
            # Convert reset time to readable format
            reset_datetime = datetime.fromtimestamp(reset_time)
            logger.info(f"Rate limit status: {remaining}/{limit} requests remaining. Resets at {reset_datetime}")
            
            return remaining, limit, reset_time
        else:
            logger.warning(f"Could not check rate limit status: {resp.status_code}")
            return None, None, None

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Recreate GitHub teams from CSV file")
    parser.add_argument('--rate-limit-delay', type=float, default=1.0, 
                       help="Delay between API requests in seconds (default: 1.0)")
    parser.add_argument('--csv-file', type=str, 
                       help="CSV file path (overrides INPUT_CSV_FILE env var)")
    args = parser.parse_args()
    
    # Override environment variables if provided
    if args.csv_file:
        os.environ['INPUT_CSV_FILE'] = args.csv_file
    
    try:
        logger.info("[START] GitHub team recreation process started")
        fetcher = GitHubTeamRecreationFetcher()
        
        # Set custom rate limit delay if provided
        if args.rate_limit_delay != 1.0:
            fetcher.rate_limit_delay = args.rate_limit_delay
            logger.info(f"Using custom rate limit delay: {args.rate_limit_delay} seconds")
        
        if not fetcher.test_github_connection():
            logger.error("[ABORT] GitHub connection test failed")
            return
        
        # Check rate limit status before starting
        logger.info("Checking rate limit status...")
        fetcher.check_rate_limit_status()
        
        fetcher.recreate_teams_from_csv()
        logger.info("[DONE] Team recreation process finished")
    except Exception as e:
        logger.error(f"[EXCEPTION] {str(e)}")
        raise

if __name__ == "__main__":
    main()
