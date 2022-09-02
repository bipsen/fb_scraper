import os
import socket
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from random import randint
import logging
import json
from time import sleep
from pyairtable import Table
from pyairtable.formulas import match
import facebook_scraper

def handle_pagination_url(url):
    global start_url
    start_url = url
    with open(resume_file, "w") as f:
        f.write(url + "\n")

parser = argparse.ArgumentParser(description='Harvest facebook posts from multiple posts')
parser.add_argument('--start-date', type=lambda s: datetime.strptime(s, '%Y-%m-%d'))
parser.add_argument('--airtable-name', type=str)
args = parser.parse_args()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s: %(message)s", level=logging.INFO
)
logging.getLogger("facebook_scraper").setLevel(logging.CRITICAL)

airtable_api_key = os.environ["AIRTABLE_API_KEY"]
airtable_base_id = os.environ["AIRTABLE_WEBSCRAPING_BASE_ID"]
table = Table(airtable_api_key, airtable_base_id, args.airtable_name)

while True:
    # Check if in progress
    record = table.first(formula=match({"Progress": "In progress", "Worker": socket.gethostname()}))
    # Else get first empty
    if not record:
        record = table.first(sort=["Id"], formula="Progress=''")
    if not record:
        logging.info(f"Finished harvesting")
        break
    group = record['fields']['Id']
    table.update(record["id"], {"Progress": "In progress", "Worker": socket.gethostname()})
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
            for post in facebook_scraper.get_posts(group=group, start_url=start_url, page_limit=None, request_url_callback=handle_pagination_url, cookies='from_browser'):
                if post['time'] and post['time'] < args.start_date:
                    if k > 5:
                        break
                    else:
                        k += 1
                else:
                    k = 0
                    with open(download_path / f"{post['post_id']}.json", 'w') as f:
                        json.dump(post, f, default=str)
                    sleep(1)
        except facebook_scraper.exceptions.NotFound:
            logging.error("Group not found")
            break
        except facebook_scraper.exceptions.TemporarilyBanned:
            sleep_time = randint(10800, 14400)
            logging.warning(f"Temporarily banned. Sleeping until {datetime.now() + timedelta(seconds=sleep_time)}")
            sleep(sleep_time)
        else:
            table.update(record["id"], {"Progress": "Done"})
            sleep_time = randint(10800, 14400)
            logging.info(f"Finished harvesting {group}. Sleeping until {datetime.now() + timedelta(seconds=sleep_time)}")
            sleep(sleep_time)
            break