import argparse
from datetime import datetime
from pathlib import Path
from time import sleep
from random import randint
import logging
import json
import facebook_scraper


def handle_pagination_url(url):
    global start_url
    start_url = url
    with open(resume_file, "w") as f:
        f.write(url + "\n")

parser = argparse.ArgumentParser(description='Harvest facebook posts from multiple posts')
parser.add_argument('--start-date', type=lambda s: datetime.strptime(s, '%Y-%m-%d'))
args = parser.parse_args()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s: %(message)s", level=logging.INFO
)
logging.getLogger("facebook_scraper").setLevel(logging.CRITICAL)

with open('groups.txt','r') as f:
    groups = f.read().splitlines()
if Path('finished_groups.txt').exists():
    with open('finished_groups.txt', 'r') as f:
        finished_groups = f.read().splitlines()
else:
    finished_groups = list()
groups = [g for g in groups if not g in finished_groups]

facebook_scraper.set_cookies('cookies.json')


for group in groups:
    logging.info(f"Now harvesting {group}")
    k = 0
    start_url = None
    download_path = Path('downloads') / group
    download_path.mkdir(exist_ok=True, parents=True)

    while True:
        resume_file = Path(f'resume_file_{group}')
        if resume_file.exists():
            with open(resume_file, "r") as f:
                existing_url = f.readline().strip()
            if existing_url:
                start_url = existing_url
                logging.info(f"Picking up from {start_url}")

        try:
            for post in facebook_scraper.get_posts(group=group, start_url=start_url, page_limit=None, request_url_callback=handle_pagination_url):
                if post['time'] and post['time'] < args.start_date:
                    if k > 5:
                        break
                    else:
                        k += 1
                else:
                    k = 0
                    with open(download_path / f"{post['post_id']}.json", 'w') as f:
                        json.dump(post, f, default=str)
        except facebook_scraper.exceptions.NotFound:
            logging.error("Group not found")
            break
        except facebook_scraper.exceptions.TemporarilyBanned:
            logging.warning(f"Temporarily banned")
            sleep(randint(1800, 5400))
        else:
            logging.info(f"Finished harvesting {group}")
            with open('finished_groups.txt', 'a') as f:
                f.write(f"{group}\n")
            sleep(randint(1800, 5400))
            break