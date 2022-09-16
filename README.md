# A little wrapper script for facebook-scraper

The main purpose of this script is continuously running facebook-scraper with many groups.

You can run this script in the terminal like

    python script.py --tablename=AIRTABLE_TABLE_NAME --username=FACEBOOK_USERNAME --password=PASSWORD --latest-date=2000-01-01
    
or simply by filling out `args.txt` and running

    python script.py +args.txt
    
The script uses [docket](https://github.com/bipsen/docket), so that needs to be set up first.

## To do

* Docket logging makes facebook-scraper log everything.
