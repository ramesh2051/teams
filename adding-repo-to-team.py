import os
import csv
import logging
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict

# Load .env variables
load_dotenv()

# Configure clean console logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class GitHubRepoTeamAssigner:
    def __init__(self):
        self.token = os.getenv('TARGET_GH_PAT')
        self.org_name = os.getenv('TARGET_GH_ORG')
        self.input_csv = os.getenv('INPUT_CSV_FILE', 'github_teams.csv')

        if not self.token:
            raise ValueError("TARGET_GH_PAT is required")
        if not self.org_name:
            raise ValueError("TARGET_GH_ORG is required")

        self.headers = {
            'Authorization': f'token {self.token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        # Rate limiting settings
        self.rate_limit_delay = 1.0  # Default delay between requests in seconds
        self.max_retries = 3
        self.retry_delay = 60  # Delay when rate limit is hit

        logger.info(f"Org: {self.org_name} | CSV: {self.input_csv}")

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
                elif remaining % 200 == 0:  # Log every 200 requests
                    logger.info(f"Rate limit status: {remaining} requests remaining")
                
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

    def read_teams_from_csv(self):
        if not os.path.exists(self.input_csv):
            raise FileNotFoundError(f"CSV not found: {self.input_csv}")
        with open(self.input_csv, 'r', encoding='utf-8') as csvfile:
            return list(csv.DictReader(csvfile))

    def get_team_id(self, team_slug):
        url = f"https://api.github.com/orgs/{self.org_name}/teams/{team_slug}"
        r = self.make_api_request("GET", url)
        if r.status_code == 200:
            return r.json()['id']
        elif r.status_code == 404:
            logger.warning(f"[SKIP] Team not found: {team_slug}")
        else:
            logger.error(f"[ERROR] Team {team_slug}: {r.status_code} - {r.text}")
        return None

    def check_repo_exists(self, repo_name):
        url = f"https://api.github.com/repos/{self.org_name}/{repo_name}"
        r = self.make_api_request("GET", url)
        if r.status_code == 200:
            return True
        elif r.status_code == 404:
            logger.warning(f"[SKIP] Repo not found: {repo_name}")
        else:
            logger.error(f"[ERROR] Repo {repo_name}: {r.status_code} - {r.text}")
        return False

    def add_repo_to_team(self, team_slug, repo_name, permission='pull'):
        url = f"https://api.github.com/orgs/{self.org_name}/teams/{team_slug}/repos/{self.org_name}/{repo_name}"
        r = self.make_api_request("PUT", url, json={'permission': permission})
        if r.status_code == 204:
            logger.info(f"[SUCCESS] {repo_name} -> {team_slug} ({permission})")
            return True
        elif r.status_code == 422:
            logger.warning(f"[SKIP] {repo_name} already in {team_slug} or bad permission")
        else:
            logger.error(f"[FAIL] {repo_name} -> {team_slug}: {r.status_code} - {r.text}")
        return False

    def process_team_repo_assignments(self, dry_run=False):
        logger.info(f"DryRun: {dry_run}")
        data = self.read_teams_from_csv()
        
        # Estimate API calls if not doing dry run
        if not dry_run:
            estimated_calls = self.estimate_api_calls(data)
            
            # Check rate limit status
            remaining, limit, reset_time = self.check_rate_limit_status()
            
            if remaining is not None and estimated_calls > remaining:
                logger.warning(f"Estimated calls ({estimated_calls}) exceed remaining limit ({remaining})")
                logger.warning("Consider using --dry-run first or waiting for rate limit reset")
                logger.info("Proceeding with execution - rate limiting will handle any issues automatically")
        
        team_info = {}
        assignments = defaultdict(lambda: defaultdict(set))

        for row in data:
            slug = row['team_slug'].strip()
            repo = row['repo_name'].strip()
            perm = row.get('repo_permission', 'pull').strip()
            team_info[slug] = {'name': row['team_name'].strip(), 'parent': row['parent_team'].strip()}
            if slug and repo:
                assignments[slug][repo].add(perm)

        hierarchy = ['pull', 'triage', 'push', 'maintain', 'admin']
        for slug in assignments:
            for repo in assignments[slug]:
                perms = assignments[slug][repo]
                for perm in reversed(hierarchy):
                    if perm in perms:
                        assignments[slug][repo] = perm
                        break

        results = {'successful': [], 'failed': [], 'skipped': []}
        sorted_teams = sorted(team_info, key=lambda x: 0 if not team_info[x]['parent'] else 1)

        for slug in sorted_teams:
            if not dry_run:
                if not self.get_team_id(slug):
                    results['skipped'].append(f"Team {slug} not found")
                    continue
            for repo, perm in assignments[slug].items():
                if dry_run:
                    logger.info(f"[DRY] {repo} -> {slug} ({perm})")
                    results['successful'].append(f"DRY: {repo} -> {slug} ({perm})")
                    continue
                if not self.check_repo_exists(repo):
                    results['skipped'].append(f"{repo} not found")
                    continue
                if self.add_repo_to_team(slug, repo, perm):
                    results['successful'].append(f"{repo} -> {slug} ({perm})")
                else:
                    results['failed'].append(f"{repo} -> {slug} ({perm})")
        return results

    def generate_report(self, results):
        total = sum(len(v) for v in results.values())
        logger.info(f"Success: {len(results['successful'])} | Fail: {len(results['failed'])} | Skipped: {len(results['skipped'])} | Total: {total}")
        for key in ['successful', 'failed', 'skipped']:
            for item in results[key]:
                logger.info(f"[{key.upper()}] {item}")

    def estimate_api_calls(self, data):
        """Estimate the total number of API calls needed for the operation."""
        logger.info("Estimating API calls needed...")
        
        unique_teams = set()
        unique_repos = set()
        assignments = 0
        
        for row in data:
            team_slug = row['team_slug'].strip()
            repo_name = row['repo_name'].strip()
            
            if team_slug:
                unique_teams.add(team_slug)
            if repo_name:
                unique_repos.add(repo_name)
            if team_slug and repo_name:
                assignments += 1
        
        # Estimate API calls:
        # 1. Rate limit check: 1 call
        # 2. Team existence check: 1 call per unique team
        # 3. Repo existence check: 1 call per unique repo
        # 4. Repo-team assignment: 1 call per assignment
        
        rate_limit_calls = 1
        team_checks = len(unique_teams)
        repo_checks = len(unique_repos)
        assignment_calls = assignments
        
        total_estimated = rate_limit_calls + team_checks + repo_checks + assignment_calls
        
        logger.info(f"Estimation: {len(unique_teams)} teams, {len(unique_repos)} repos, {assignments} assignments")
        logger.info(f"Estimated API calls: {total_estimated}")
        logger.info(f"Estimated time: {(total_estimated * self.rate_limit_delay) / 60:.1f} minutes")
        
        return total_estimated

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Assign repos to GitHub teams")
    parser.add_argument('--dry-run', action='store_true', help="Simulate without changes")
    parser.add_argument('--csv-file', type=str, help="CSV file path")
    parser.add_argument('--rate-limit-delay', type=float, default=1.0, 
                       help="Delay between API requests in seconds (default: 1.0)")
    parser.add_argument('--estimate-only', action='store_true',
                       help="Only estimate API calls needed, don't process assignments")
    args = parser.parse_args()

    if args.csv_file:
        os.environ['INPUT_CSV_FILE'] = args.csv_file

    try:
        assigner = GitHubRepoTeamAssigner()
        
        # Set custom rate limit delay if provided
        if args.rate_limit_delay != 1.0:
            assigner.rate_limit_delay = args.rate_limit_delay
            logger.info(f"Using custom rate limit delay: {args.rate_limit_delay} seconds")
        
        # Check rate limit status at start
        logger.info("Checking rate limit status...")
        remaining, limit, reset_time = assigner.check_rate_limit_status()
        
        if remaining is not None and remaining < 50:
            logger.warning(f"Low rate limit remaining ({remaining}). Consider waiting or using --dry-run")
        
        # Estimate API calls if requested
        if args.estimate_only:
            data = assigner.read_teams_from_csv()
            assigner.estimate_api_calls(data)
            logger.info("Estimation complete. Exiting without processing assignments.")
            return
        
        # Process assignments
        results = assigner.process_team_repo_assignments(dry_run=args.dry_run)
        assigner.generate_report(results)
        
        # Check final rate limit status
        if not args.dry_run:
            logger.info("Final rate limit status:")
            assigner.check_rate_limit_status()
        
        logger.info("Dry run complete." if args.dry_run else "Assignment complete.")
        
    except Exception as e:
        logger.error(f"[ERROR] {e}")
        raise

if __name__ == "__main__":
    main()
