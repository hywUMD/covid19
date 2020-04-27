from datetime import date, timedelta, datetime
from pathlib import Path
import json
import os

import argparse

parser = argparse.ArgumentParser(
    description="extract geotagged tweets and aggregate tweeets by day"
)
parser.add_argument(
    "--inputdir", 
    default="../covid19_tweets/",
    help="dir where raw tweets locate"
)
parser.add_argument(
    "--outputdir", 
    default="../covid19_tweets_geotagged/",
    help="dir where raw tweets locate"
)
parser.add_argument(
    "--startdate",
    default="20200226",
    help="start date (included)"
)
parser.add_argument(
    "--enddate",
    default=date.today().strftime("%Y%m%d"),
    help="end date (not included)"
)




args = parser.parse_args()
if __name__ == "__main__":
    # create outputdir
    Path(args.outputdir).mkdir(parents=True, exist_ok=True)

    start_date = datetime.strptime(args.startdate, "%Y%m%d")
    end_date = datetime.strptime(args.enddate, "%Y%m%d")
    delta = end_date -start_date

    for i in range(delta.days):
        current_date = start_date + timedelta(days=i)
        current_date_str = current_date.strftime("%Y%m%d")

        tweet_id_set = set()
        current_date_path_in = Path(args.inputdir) / current_date_str
        fout = Path(args.outputdir).joinpath(current_date_str + ".txt")

        for logfile in current_date_path_in.iterdir():
            if logfile.is_file():
                with open(logfile) as fin:
                    for line in fin:
                        tweet = json.loads(line)

                        fout.write_text("\n")