from pathlib import Path
import json
from datetime import date
from time import sleep
import logging
import facebook_scraper
from ratelimit import limits, sleep_and_retry
import click

logging.basicConfig(
    format="%(asctime)s | %(levelname)s: %(message)s", level=logging.INFO
)
logging.getLogger("facebook_scraper").setLevel(logging.CRITICAL)


start_url = None


@click.command()
@click.option(
    "--date-start", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.min)
)
@click.option(
    "--date-end", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(date.max)
)
def main(date_start, date_end):

    # File with target group ids
    with open("groups.txt", "r") as f:
        groups = f.read().splitlines()

    # Keeps track of already done groups
    if Path("finished_groups.txt").exists():
        with open("finished_groups.txt", "r") as f:
            finished_groups = f.read().splitlines()
        groups = [g for g in groups if not g in finished_groups]

    facebook_scraper.set_cookies("cookies.json")

    k = 0  # For casual time window

    for group in groups:

        logging.info(f"Now harvesting {group}")

        resume_file = Path(f"start_url_{group}")
        start_url = try_to_resume(resume_file)

        # Create download folder
        download_dir = Path("downloads") / group
        download_dir.mkdir(exist_ok=True, parents=True)

        while True:
            try:
                for post in facebook_scraper.get_posts(
                    group=group,
                    page_limit=None,
                    start_url=start_url,
                    request_url_callback=handle_pagination_url,
                ):

                    check_limit()

                    # Check time
                    if post["time"] > date_end:
                        continue
                    elif post["time"] <= date_start:
                        k += 1
                        if k == 20:
                            break
                        else:
                            continue
                    else:
                        # If post within time frame, save post
                        with open(download_dir / f"{post['post_id']}.json", "w") as f:
                            json.dump(post, f, default=str)
                        k = 0

                logging.info(f"Completed {group}")
                remove_resume_file(resume_file)
                with open("finished_groups.txt", "a"):
                    f.write(f"{group}\n")
                break

            except facebook_scraper.exceptions.TemporarilyBanned:
                logging.info(
                    f"Temporary ban while harvesting {group}. Sleeping 10 minutes."
                )
                sleep(600)

            except Exception as e:
                if start_url:
                    with open(resume_file, "w") as f:
                        f.write(start_url)
                    logging.error("Saved resume info")
                raise e


@sleep_and_retry
@limits(calls=50, period=900)
def check_limit():
    """Dummy function to use the ratelimit library with
    the post generator"""
    return


def handle_pagination_url(url):
    """To paginate scraping between script breaks"""
    global start_url
    start_url = url


def try_to_resume(resume_file):
    """Resume from file if exists"""
    if resume_file.exists():
        logging.info(f"Resuming from file {resume_file}")
        with open(resume_file, "r") as f:
            start_url = f.read()
        return start_url


def remove_resume_file(resume_file):
    if resume_file.exists():
        resume_file.unlink()


if __name__ == "__main__":
    main()
