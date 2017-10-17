"""
Scraper for /r/changemyview data
"""

import os
import time
import pickle
import numpy as np
import pandas as pd
import praw
import argparse
from utils import can_fail
from cmv_types import GatherSub, GatherCMVSub, GatherComment, GatherCMVComment, GatherCMVSubAuthor
from cmv_tables import init_tables

# sqlalchemy imports
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

END_2016 = 1483228800
START_2013 = 1356998400
START_2015 = 1420070400
START_2016 = 1451606400
JAN_20_2016 = 1453334399 
JAN_4_2016 = 1451811999
MID_2016 = 1464739200

START_BDAY_2016 = 1461110400
END_BDAY_2016 = 1461130000






class CMVScraper:
    """
    Class to scrape /r/changemyview for MACS 302 and possibly thesis.
    """
    def __init__(self, start_date, end_date, new_tables, pwd_file, 
            cmv_com_content, all_com_content, echo):
        """
        Initializes the class with an instance of the praw.Reddit class.
        """
        self.cmv_com_content = cmv_com_content
        self.all_com_content = all_com_content
        with open(pwd_file, 'r') as f:
            password = f.read()[:-1]

        # sqlalchemy connection
        self.engine = sqlalchemy.create_engine('mysql://jmcclellan:{}@mpcs53001.cs.uchicago.edu/jmcclellanDB?charset=utf8'.format(password), echo=echo)
        session = sessionmaker(bind=self.engine)
        self.session = session()
        if new_tables:
            CMVScraper.init_tables(self.engine)

        # PRAW objects
        self.praw_agent = praw.Reddit("cmv_scrape", # Site ID
                                      user_agent = "/u/shugamoe /r/changemyview scraper")
        self.subreddit = self.praw_agent.subreddit("changemyview")

        self.praw_agent.read_only = True # We"re just here to look

        # Start and end_date dates of interest
        self.date_start_date = start_date
        self.date_end_date = end_date

        # If more than a day between start_date and end_date break up the date into
        # approximately day sized chunks to avoid 503 error.
        if end_date - start_date > 86400:
            self.date_chunks = np.ceil(np.linspace(start_date, end_date, num=
                (end_date - start_date) / 85400))

        # Example instances to to tinker with
        self.eg_submission = self.praw_agent.submission("5kgxsz")
        self.eg_comment = self.praw_agent.comment("cr2jp5a")
        self.eg_user = self.praw_agent.redditor("RocketCity1234")

    def look_at_comment(self, com_id):
        """
        """
        return self.praw_agent.comment(com_id)

    def look_at_submission(self, sub_id):
        """
        """
        return self.praw_agent.submission(sub_id)

    @staticmethod
    def init_tables(engine):
        """
        Create new tables in db
        """
        init_tables(engine)

    @staticmethod
    def arg_parser():
        """
        Handles arguments for CLI
        """
        parser = argparse.ArgumentParser(description="Scrape CMV Submissions and author information")
        parser.add_argument("--start_date", "-s", default=START_2016, type=int,
                            help="Start Date (UTC Epoch) of CMV Submissions to gather")
        parser.add_argument("--end_date", "-e", default=JAN_4_2016, type=int,
                            help="End Date (UTC Epoch) of CMV Submissions to gather")
        parser.add_argument("--new_tables", action="store_true", default=False,
                            help="Creates new tables in the database")
        parser.add_argument("--pwd_file", type=str, default="pwd.txt",
                            help="File containing the password")
        parser.add_argument("--cmv_com_content", type=bool, default=True,
                            help="Gather the content of CMV Comments")
        parser.add_argument("--all_com_content", type=bool, default=False,
                            help="Gather the content of All Comments")
        parser.add_argument("--echo", action="store_true", default=False,
                            help="Echo sqlalchemy engine")

        parser_args = parser.parse_args()

        return parser_args

    @can_fail
    def scrape_submissions(self):
        """
        This function gathers the submission IDs for submissions in
        /r/changemyview
        """
        @can_fail
        def scrape_submissions_between(self, date_start_date, date_end_date):
            """
            """
            date_start_date_string = (
                time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(date_start_date)))
            date_end_date_string = (
                time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(date_end_date)))
            print("Gathering {} to {}".format(date_start_date_string, date_end_date_string))

            for sub_instance in self.subreddit.submissions(date_start_date, date_end_date):
                GatherCMVSub(sub_instance, self).save_to_db()

        if hasattr(self, "date_chunks"):
            print("Time window too large, gathering submissions in chunks")
            second_last_index = len(self.date_chunks) - 1
            for i in range(second_last_index):
                if i == 0:
                    date_start_date = self.date_chunks[i]
                    date_end_date = self.date_chunks[i + 1]
                else:
                    date_start_date = self.date_chunks[i] + 1
                    date_end_date = self.date_chunks[i + 1]
                scrape_submissions_between(self, date_start_date, date_end_date)
            # num_subs_gathered = len(self.cmv_subs)
            # print("{} submissions gathered".format(num_subs_gathered))
        else:
            scrape_submissions_between(self, self.date_start_date, self.date_end_date)


    def scrape_author_histories(self):
        """
        """
        
        def scrape_author_history(author):
            """
            """
            print("Retrieving history for: {}".format(author))
            SubAuthor = GatherCMVSubAuthor(author, self)
            SubAuthor.get_history_for("comments")
            SubAuthor.get_history_for("submissions")
            SubAuthor.save_to_db()

        get_auth_hist_vrized = np.vectorize(scrape_author_history,
                otypes="?") # otypes kwarg to avoid double appplying func
        get_auth_hist_vrized(np.array([cmv_sub.author for cmv_sub in self.session.query(GatherCMVSub.sqla_mapping)]))


    @staticmethod
    def make_output_dir(dir_name):
        """
        Creates an output directory in current folder if it does not exist
        already and returns the current directory
        """
        cur_path = os.path.split(os.path.abspath(__file__))[0]
        output_fldr = dir_name
        output_dir = os.path.join(cur_path, output_fldr)
        if not os.access(output_dir, os.F_OK):
            os.makedirs(output_dir)

        return output_dir


def main():
    """
    """
    args = CMVScraper.arg_parser()
    print(args)
    global smodder 
    smodder = CMVScraper(**vars(args))
    smodder.scrape_submissions()
    smodder.scrape_author_histories()


if __name__ == "__main__":
    main()
    
#    SMODDER.update_cmv_submissions()
#    SMODDER.update_author_history()
#    with open("test.pkl", "wb") as output:
#        pickle.dump(SMODDER, output)
