from pathlib import Path
import json
from datetime import date
import logging
from functools import partial
import facebook_scraper
from backoff import on_exception, expo
from ratelimit import limits, sleep_and_retry
import click

logging.basicConfig(
    format="%(asctime)s | %(levelname)s: %(message)s", level=logging.INFO
)
logging.getLogger("facebook_scraper").setLevel(logging.CRITICAL)
logging.getLogger("backoff").addHandler(logging.StreamHandler())


class Scraper:
    """Wrapper for facebook-scraper that can take in a time frame, has implemented request backoff, saves resume info and more.
    Expects there to be a file called groups.txt with group ids."""

    def __init__(self, date_start, date_end):

        facebook_scraper.set_cookies("cookies.json")

        self.date_start = date_start
        self.date_end = date_end
        self.start_url = None
        self.k = None

    def scrape_group(self, group_id):
        logging.info(f"Now harvesting {group_id}")

        # If resume file exists, use it
        resume_file = Path(f"start_url_{group_id}")
        self.start_url = self.try_to_resume(resume_file)

        # Create download folder
        download_dir = Path("downloads") / group_id
        download_dir.mkdir(exist_ok=True, parents=True)

        # Create post iterator
        post_iterator = facebook_scraper.get_posts(
            group=group_id,
            page_limit=None,
            start_url=self.start_url,
            request_url_callback=self.handle_pagination_url,
        )

        # Get posts with while loop so we can use exponential backoff
        while True:
            try:
                post = self.get_next_post(post_iterator)

                # If post within time frame, save it
                if self.check_timeframe(post["time"]):
                    with open(download_dir / f"{post['post_id']}.json", "w") as f:
                        json.dump(post, f, default=str)

            # We reached the end of posts or end of time frame
            except StopIteration:
                break

            except facebook_scraper.exceptions.UnexpectedResponse:
                logging.error("facebook_scraper.exceptions.UnexpectedResponse")

            # Save resume file on other exceptions
            except Exception as e:
                if self.start_url:
                    with open(resume_file, "w") as f:
                        f.write(self.start_url)
                    logging.error("Saved resume info")
                raise e

        # Group finished
        logging.info(f"Completed {group_id}")
        resume_file.unlink(missing_ok=True)
        with open("finished_groups.txt", "a") as f:
            f.write(f"{group_id}\n")

    def check_timeframe(self, post_time, allowed_before=10):
        """Checks whether post is within date range. It expects an almost chronological order,
        allowing for up to k older-than-date-end posts in a row. Return True for download,
        False for skip and raises a StopIteration if allowed_before is exceeded."""

        # Post after limit, skip
        if post_time > self.date_end:
            return False

        # Post is from before limit, skip if k<=allowed_before else end
        if post_time <= self.date_start:
            if self.k > allowed_before:
                raise StopIteration
            else:
                self.k += 1
                return False

        # Post within time period
        self.k = 0
        return True

    def handle_pagination_url(self, url):
        """To paginate scraping between script breaks"""
        self.start_url = url

    @staticmethod
    def try_to_resume(resume_file):
        """Resume from file if exists"""
        if resume_file.exists():
            logging.info(f"Resuming from file {resume_file}")
            with open(resume_file, "r") as f:
                start_url = f.read()
            return start_url

    @staticmethod
    @sleep_and_retry
    @limits(calls=50, period=900)
    @on_exception(
        partial(expo, factor=60),
        facebook_scraper.exceptions.TemporarilyBanned,
        max_time=100000,
    )
    def get_next_post(post_iterator):
        return next(post_iterator)


@click.command()
@click.option(
    "--date-start", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.min)
)
@click.option(
    "--date-end", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.max)
)
def main(date_start, date_end):

    s = Scraper(date_start, date_end)

    # File with target group ids
    with open("groups.txt", "r") as f:
        groups = f.read().splitlines()

    # Keeps track of already done groups
    if Path("finished_groups.txt").exists():
        with open("finished_groups.txt", "r") as f:
            finished_groups = f.read().splitlines()
        groups = [g for g in groups if not g in finished_groups]

    for group in groups:
        s.scrape_group(group)


if __name__ == "__main__":
    main()
