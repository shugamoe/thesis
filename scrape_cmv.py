"""
Scraper for /r/changemyview data
"""

import os
import time
import pickle
import numpy as np
import pandas as pd
import praw
from utils import can_fail
from cmv_types import CMVSubmission, CMVSubAuthor, CMVAuthSubmission, CMVAuthComment   

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
    INIT_SUB_COL_NAMES = ["id", "author", "sub_inst"]
    def __init__(self, start, end):
        """
        Initializes the class with an instance of the praw.Reddit class.
        """
        # PRAW objects
        self.praw_agent = praw.Reddit("cmv_scrape", # Site ID
                                      user_agent = "/u/shugamoe /r/changemyview scraper")
        self.subreddit = self.praw_agent.subreddit("changemyview")

        self.praw_agent.read_only = True # We"re just here to look

        # Start and end dates of interest
        self.date_start = start
        self.date_end = end

        # If more than a day between start and end break up the date into
        # approximately day sized chunks to avoid 503 error.
        if end - start > 86400:
            self.date_chunks = np.ceil(np.linspace(start, end, num=
                (end - start) / 85400))

        # Example instances to to tinker with
        self.eg_submission = self.praw_agent.submission("5kgxsz")
        self.eg_comment = self.praw_agent.comment("cr2jp5a")
        self.eg_user = self.praw_agent.redditor("RocketCity1234")

    @can_fail
    def get_all_submissions(self):
        """
        This function gathers the submission IDs for submissions in
        /r/changemyview
        """
        if hasattr(self, "date_chunks"):
            print("Time window too large, gathering submissions in chunks")
            second_last_index = len(self.date_chunks) - 2
            for i in range(second_last_index):
                if i == 0:
                    date_start = self.date_chunks[i]
                    date_end = self.date_chunks[i + 1]
                else:
                    date_start = self.date_chunks[i] + 1
                    date_end = self.date_chunks[i + 1]

                self._get_submissions_between(date_start, date_end)
            num_subs_gathered = len(self.cmv_subs)
            print("{} submissions gathered".format(num_subs_gathered))
        else:
            self._get_submissions_between(self.date_start, self.date_end)

    @can_fail
    def _get_submissions_between(self, date_start, date_end):
        """
        """
        date_start_string = (
            time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(date_start)))
        date_end_string = (
            time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(date_end)))
        print("Gathering {} to {}".format(date_start_string, date_end_string))
        sub_df_dict = {col_name: [] for col_name in self.INIT_SUB_COL_NAMES}

        for sub in self.subreddit.submissions(date_start, date_end):
            try:
                sub_df_dict["author"].append(sub.author.name)
            except AttributeError: # If author is None, then user is deleted
                sub_df_dict["author"].append("[deleted]")

            sub_df_dict["id"].append(sub.id)
            sub_df_dict["sub_inst"].append(sub)

        sub_df = pd.DataFrame(sub_df_dict)
        if hasattr(self, "cmv_subs"):
            sub_df.set_index("id", drop=False, inplace=True)
            self.cmv_subs = self.cmv_subs.append(sub_df)
        else:
            self.cmv_subs = sub_df.set_index("id", drop=False)

    def update_cmv_submissions(self):
        """
        This function retrieves following information about submissions:
            - Whether the OP awarded a delta
            - How many deltas the OP awarded
            - Number of top level replies
        """
        if hasattr(self, "cmv_subs"):
            pass
        else:
            self.get_all_submissions()

        all_subs = self.cmv_subs
        valid_subs = all_subs[all_subs["author"] != "[deleted]"][["sub_inst"]]
        valid_subs = valid_subs.assign(
            **{label: None for label in
               list(CMVSubmission.STATS_TEMPLATE.keys())})
        valid_subs.loc[:, sorted(list(CMVSubmission.STATS_TEMPLATE.keys()))] = (
            valid_subs["sub_inst"].apply(lambda sub_inst:
                                         CMVSubmission(sub_inst).get_stats_series()))

        # valid_subs[sorted(list(CMVSubmission.STATS_TEMPLATE.keys()))] = (
                #valid_subs["sub_inst"].apply(lambda sub_inst:
                    #CMVSubmission(sub_inst).get_stats_series()))

        # TODO(jcm): Get index matching without column duplication working, 
        # matching objects is slower than matching strings
        self.cmv_subs = all_subs.merge(valid_subs, on="sub_inst", copy=False)

    def _get_sub_info(self, sub_inst):
        """
        This function retrieves the following information for a single 
        submission:
            - Whether the OP awarded a delta
            - How many deltas the OP awarded
            - Number of top level replies
        """
        submission = CMVSubmission(sub_inst)
        submission.parse_root_comments(None)

        return submission.get_stats_series()
    
    def get_author_histories(self):
        """
        """
        if hasattr(self, "cmv_subs"):
            pass
        else:
            self.get_all_submissions()
        
        get_auth_hist_vrized = np.vectorize(self._get_author_history,
                otypes="?") # otypes kwarg to avoid double appplying func
        get_auth_hist_vrized(self.cmv_subs["author"].unique())
        
    def _get_author_history(self, author):
        """
        """
        print("Retrieving history for: {}".format(author))
        SubAuthor = CMVSubAuthor(self.praw_agent.redditor(author))
        # SubAuthor.get_history_for("comments")
        SubAuthor.get_history_for("submissions")
        
        # if hasattr(self, "cmv_author_coms"):
        #    self.cmv_author_coms= self.cmv_author_coms.append(
        #            SubAuthor.get_post_df("comments"))
        # else:
        #    self.cmv_author_coms = SubAuthor.get_post_df("comments")

        if hasattr(self, "cmv_author_subs"):
            self.cmv_author_subs = self.cmv_author_subs.append(
                SubAuthor.get_post_df("submissions"))
        else:
            self.cmv_author_subs = SubAuthor.get_post_df("submissions")


    def update_author_history(self):
        """
        """
        if hasattr(self, "cmv_author_subs"):
            pass
        else:
            self.get_author_histories()
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
