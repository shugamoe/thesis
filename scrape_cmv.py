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
from cmv_types import CMVSubmission, CMVSubAuthor, CMVAuthSubmission, CMVAuthComment
from cmv_tables import init_tables

# sqlalchemy imports
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

END_2016 = 1483228800
START_2013 = 1356998400
START_2015 = 1420070400
START_2016 = 1451606400
MID_2016 = 1464739200

START_BDAY_2016 = 1461110400
END_BDAY_2016 = 1461130000




class CMVScraperModder:
    """
    Class to scrape /r/changemyview for MACS 302 and possibly thesis.
    """
    def __init__(self, start_date, end_date, new_tables, db_loc):
        """
        Initializes the class with an instance of the praw.Reddit class.
        """
        # sqlalchemy connection
        self.engine = create_engine("sqlite:///" + db_loc, echo=True)
        session = sessionmaker(bind=self.engine)
        self.session = session()
        if new_tables:
            CMVScraperModder.init_tables()

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

    @staticmethod
    def init_tables():
        """
        Create new tables in db
        """
        init_tables()

    @staticmethod
    def arg_parser():
        """
        Handles arguments for CLI
        """
        parser = argparse.ArgumentParser(description="Scrape CMV Submissions and author information")
        parser.add_argument("--start_date", "-s", default=START_BDAY_2016, type=int,
                            help="Start Date (UTC Epoch) of CMV Submissions to gather")
        parser.add_argument("--end_date", "-e", default=END_BDAY_2016, type=int,
                            help="End Date (UTC Epoch) of CMV Submissions to gather")
        parser.add_argument("--new_tables", action="store_false", # Stores True as default lol
                            help="Creates new tables in the database")
        parser.add_argument("--db_loc", type=str, default="cmv_related.db",
                            help="Location of the sqlite3 database")

        parser_args = parser.parse_args()

        return parser_args

    @can_fail
    def scrape_submissions(self):
        """
        This function gathers the submission IDs for submissions in
        /r/changemyview
        """
        if hasattr(self, "date_chunks"):
            print("Time window too large, gathering submissions in chunks")
            second_last_index = len(self.date_chunks) - 2
            for i in range(second_last_index):
                if i == 0:
                    date_start_date = self.date_chunks[i]
                    date_end_date = self.date_chunks[i + 1]
                else:
                    date_start_date = self.date_chunks[i] + 1
                    date_end_date = self.date_chunks[i + 1]
                self._scrape_submissions_between(date_start_date, date_end_date)
            # num_subs_gathered = len(self.cmv_subs)
            # print("{} submissions gathered".format(num_subs_gathered))
        else:
            self._scrape_submissions_between(self.date_start_date, self.date_end_date)

    @can_fail
    def _scrape_submissions_between(self, date_start_date, date_end_date):
        """
        """
        date_start_date_string = (
            time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(date_start_date)))
        date_end_date_string = (
            time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(date_end_date)))
        print("Gathering {} to {}".format(date_start_date_string, date_end_date_string))

        for sub_instance in self.subreddit.submissions(date_start_date, date_end_date):
            CMVSubmission(sub_instance, self.session).save_to_db()

    def scrape_author_histories(self):
        """
        """
        if hasattr(self, "cmv_subs"):
            pass
        else:
            self.scrape_submissions()
        
        get_auth_hist_vrized = np.vectorize(self._scrape_author_history,
                otypes="?") # otypes kwarg to avoid double appplying func
        get_auth_hist_vrized(self.cmv_subs["author"].unique())
        
    def _scrape_author_history(self, author):
        """
        """
        print("Retrieving history for: {}".format(author))
        SubAuthor = CMVSubAuthor(self.praw_agent.redditor(author))
        # SubAuthor.get_history_for("comments")
        SubAuthor.get_history_for("submissions")
        
        # if hasattr(self, "cmv_author_coms"):
        #    self.cmv_author_coms= self.cmv_author_coms.append_date(
        #            SubAuthor.get_post_df("comments"))
        # else:
        #    self.cmv_author_coms = SubAuthor.get_post_df("comments")

        if hasattr(self, "cmv_author_subs"):
            self.cmv_author_subs = self.cmv_author_subs.append_date(
                SubAuthor.get_post_df("submissions"))
        else:
            self.cmv_author_subs = SubAuthor.get_post_df("submissions")


    def update_author_history(self):
        """
        """
        if hasattr(self, "cmv_author_subs"):
            pass
        else:
            self.scrape_author_histories()
        # Update Submissions
        sub_inst_series = self.cmv_author_subs[["sub_inst"]]

        sub_inst_series = sub_inst_series.assign(
            **{label: None for label in 
                    list(CMVAuthSubmission.STATS_TEMPLATE.keys())})
        sub_inst_series.loc[:, sorted(list(CMVAuthSubmission.STATS_TEMPLATE.keys()))] = (
            sub_inst_series["sub_inst"].apply(
                    lambda sub_inst: CMVAuthSubmission(sub_inst)
                    .get_stats_series()))
   
        #sub_inst_series[sorted(list(CMVAuthSubmission.STATS_TEMPLATE.keys()))] = (
                #sub_inst_series["sub_inst"].apply(
                    #lambda sub_inst: CMVAuthSubmission(sub_inst)
                    #.get_stats_series()))
        self.cmv_author_subs = self.cmv_author_subs.merge(sub_inst_series,
                                                          on="sub_inst", copy=False)
        self.cmv_author_subs.drop_duplicates(subset="sub_id", inplace=True)
        self.cmv_author_subs.dropna(axis=0, how="all", inplace=True)

        # Update Comments
        # com_inst_series = self.cmv_author_coms[["com_inst"]]
        # print("Comment instances gathered")
        # com_inst_series = com_inst_series.assign(
                 # **{label: None for label in
                     # list(CMVAuthComment.STATS_TEMPLATE.keys())})
        # com_inst_series.loc[:, sorted(list(CMVAuthComment.STATS_TEMPLATE.keys()))] = (
            # com_inst_series["com_inst"].apply(
                # lambda com_inst: CMVAuthComment(com_inst).get_stats_series()
            # ))
        #com_inst_series[sorted(list(CMVAuthComment.STATS_TEMPLATE.keys()))] = (
        #com_inst_series["com_inst"].apply(
                #lambda com_inst: CMVAuthComment(com_inst).get_stats_series()
            #))
        # print("Comment stats extracted")
        # self.cmv_author_coms = self.cmv_author_coms.merge(com_inst_series,
                # on="com_inst", copy=False)
        # self.cmv_author_coms.drop_duplicates(subset="com_id", inplace=True)
        # print("Comment stats merged")

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
    args = CMVScraperModder.arg_parser()
    print(args)
    global smodder 
    smodder = CMVScraperModder(**vars(args))
    smodder.scrape_submissions()


if __name__ == "__main__":
    main()
    
#    SMODDER.update_cmv_submissions()
#    SMODDER.update_author_history()
#    with open("test.pkl", "wb") as output:
#        pickle.dump(SMODDER, output)
