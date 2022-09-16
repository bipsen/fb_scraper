from datetime import datetime
from pathlib import Path
import json
import argparse
from docket import Docket
import facebook_scraper


class ResumeManager:
    def __init__(self):
        self.start_url = None
        self.resume_file = None
        self.resume_dir = Path("resume_files")
        self.resume_dir.mkdir(exist_ok=True)

    def handle_pagination_url(self, url):
        """Save pagination url to resume from where we left off, if
        the script breaks"""
        self.start_url = url
        with open(self.resume_file, "w") as f:
            f.write(url + "\n")

    def update_target(self, group):
        """Make a new resume file for this group, or if it already exists,
        start from there"""
        # Update resume file to new group
        self.resume_file = self.resume_dir / f"resume_file_{group}"
        # Try to resume
        if self.resume_file.exists():
            with open(self.resume_file, "r") as f:
                existing_url = f.readline().strip()
            if existing_url:
                self.start_url = existing_url


def save_post(post, download_path):
    download_path.parents[0].mkdir(parents=True, exist_ok=True)
    with open(download_path, "w") as f:
        json.dump(post, f, default=str)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Harvest facebook posts from multiple posts",
        fromfile_prefix_chars="+",
    )
    parser.add_argument("--tablename", type=str)
    parser.add_argument("--username", type=str)
    parser.add_argument("--password", type=str)
    parser.add_argument(
        "--latest-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d")
    )
    args = parser.parse_args()

    groups = Docket(args.tablename)
    facebook_scraper.use_persistent_session(args.username, args.password)
    rm = ResumeManager()

    for group in groups.get_jobs():
        rm.update_target(group)
        for post in facebook_scraper.get_posts(
            group=group,
            start_url=rm.start_url,
            page_limit=None,
            request_url_callback=rm.handle_pagination_url,
            latest_date=args.latest_date,
        ):
            download_path = Path("downloads") / group / f"{post['post_id']}.json"
            save_post(post, download_path)
