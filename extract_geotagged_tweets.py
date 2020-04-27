from datetime import date, timedelta, datetime
from pathlib import Path
import json
import os
import time

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

header = "tweet_id	tweet	username	timestamp	latitude	longitude	place	boundingbox	following	followers	favourites_count	favorite_count	is_quote_status	quote_count	reply_count	retweeted	retweet_count	negativesum	racecat1	racecat2	racecat3	raceterm1	raceterm2	raceterm3	virusterm1	virusterm2	virusterm3".split()
# row_header = "\t".join(headers)
# print("header len: " + str(len(headers.split())))

def process_tweet(tweet):
    row = []

    row.append(tweet.get('id_str', ''))
    row.append(tweet.get('text', ''))
    user = tweet.get('user', {})
    row.append(user.get('screen_name', ''))
    row.append(tweet.get('created_at', ''))
    coordinates = tweet.get('coordinates')
    if not coordinates:
        coordinates = {'coordinates': ['', '']}
    row.append(str(coordinates['coordinates'][1]))
    row.append(str(coordinates['coordinates'][0]))

    place = tweet.get('place')
    if place:
        name = place.get('name', '')
        bounding_box = place.get(
            'bounding_box',
            {'coordinates': [['']]}
        )
        bounding_box_str = json.dumps(bounding_box['coordinates'][0])[1:-1]
        row.append(name)
        row.append(bounding_box_str)
    else:
        row.append('')
        row.append('')
    row.append(str(user.get('friends_count', 0)))
    row.append(str(user.get('followers_count', 0)))
    row.append(str(user.get('favourites_count', 0)))
    row.append(str(tweet.get('favorite_count', 0)))
    row.append(str(tweet.get('is_quote_status', False)))
    row.append(str(tweet.get('quote_count', 0)))
    row.append(str(tweet.get('reply_count', 0)))
    row.append(str(tweet.get('retweeted', False)))
    row.append(str(tweet.get('retweet_count', 0)))
    row.extend(['' for x in range(10)])
    # print("row len: " + str(len(row)))
    return row


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
        fout.write_text("\t".join(header) + "\n")

        for logfile in current_date_path_in.iterdir():
            if logfile.is_file():
                with open(logfile, encoding='ISO-8859-1') as fin:
                    try:
                        for line in fin:
                            try:
                                tweet = json.loads(line)
                            except json.decoder.JSONDecodeError:
                                print("Malformat tweet json...")
                                # time.sleep(10)
                                continue
                            if (
                                tweet.get('id_str') not in tweet_id_set 
                                and 
                                (tweet.get('coordinates') or tweet.get('place'))
                            ):
                                
                                row = process_tweet(tweet)
                                if len(row) == len(header):
                                    fout.write_text("\t".join(row) + "\n")
                                    print("Found a tweet in..." + str(logfile))
                                    print("\t\t\t" + tweet.get('id_str'))
                                    tweet_id_set.add(tweet.get('id_str'))
                    except UnicodeDecodeError:
                        print("Malformat file")
                        # time.sleep(10)
        
        print("Finished...\t" + str(fout))