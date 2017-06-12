"""
Scraper for /r/changemyview data
"""

import os
import time
import pickle
import numpy as np
import pandas as pd
import praw
from prawcore.exceptions import Forbidden, NotFound

END_2016 = 1483228800
START_2013 = 1356998400
START_2015 = 1420070400
START_2016 = 1451606400
MID_2016 = 1464739200

START_BDAY_2016 = 1461110400
END_BDAY_2016 = 1461130000


def can_fail(praw_call, *args, **kwargs):
    """
    A decorator to handle praw calls that can encounter sever errors
    """
    # pdb.set_trace()
    def robust_praw_call(self, *args, **kwargs):
        call_successful = False
        sleep_time = 120 # Wait 2 minutes if initial call fails
        while not call_successful:
            try:
                praw_call_result = praw_call(self, *args, **kwargs)
                call_successful = True
            except NotFound:
                print("User wasn't found")
                call_successful = True
            except Forbidden:
                # TODO(jcm): Weird bug. "X was suspended" prints twice.
                try:
                    print("{} was suspended".format(self.user_name))
                except:
                    pass
                call_successful = True
            except AttributeError as e:
                print("\n\t{}".format(str(e)))
                print("\tTrying: {}".format(praw_call.__name__))
                call_successful = True
            except RuntimeError as e:
                print("\n\t{}".format(str(e)))
                print("\tTrying: {}".format(praw_call.__name__))
                with open("runtime_error.pkl", "wb") as output:
                    pickle.dump(SModder, output)
                call_successful = True
            except Exception as e:
                print("\n\t{}".format(str(e)))
                print("\tTrying: {}".format(praw_call.__name__))
                if sleep_time > 600:
                    call_successful = True
                else:
                    print("\tWill now wait {} seconds before pinging server again".format(
                        sleep_time))
                    ping_time = time.strftime("%m/%d %H:%M:%S", time.localtime(
                        sleep_time + time.mktime(time.localtime())))
                    print("\tServer ping at: {}".format(ping_time))
                    time.sleep(sleep_time)
                    sleep_time += 60
        if "praw_call_result" not in locals():
            praw_call_result = None

        return praw_call_result

    return robust_praw_call


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

        df = pd.DataFrame(sub_df_dict)
        if hasattr(self, "cmv_subs"):
            df.set_index("id", drop=False, inplace=True)
            self.cmv_subs = self.cmv_subs.append(df)
        else:
            self.cmv_subs = df.set_index("id", drop=False)

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

        return(submission.get_stats_series())
    
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

# Would like to have this inherit from praw"s submissions class but with the way
# I"m scraping the data I would have to tinker with a praw"s sublisting class
# and subreddit class.
class CMVSubmission:
    """
    A class of a /r/changemyview submission
    """
    STATS_TEMPLATE = {"num_root_comments": 0,
                      "num_user_comments": 0,
                      "num_OP_comments": 0,
                      "OP_gave_delta": False,
                      "num_deltas_from_OP": 0,
                      "created_utc": None,
                      "content": None,
                      "title": None}

    @can_fail
    def __init__(self, sub_inst):
        self.submission = sub_inst

        try:
            self.author = self.submission.author.name
        except AttributeError:
            self.author = "[removed]"
        print(self.author)

        # Important Variables to track
        self.stats = {"num_root_comments": 0,
                      "num_user_comments": 0,
                      "OP_gave_delta": False,
                      "created_utc": None,
                      "num_OP_comments": 0,
                      "num_deltas_from_OP": 0,
                      "content": None,
                      "title": None}
        self.stats["content"] = self.submission.selftext
        self.stats["title"] = self.submission.title
        self.stats["created_utc"] = self.submission.created_utc
        self.parse_root_comments(self.submission.comments)

    @can_fail
    def parse_root_comments(self, comment_tree):
        """
        """
        for com in comment_tree:
            if isinstance(com, praw.models.MoreComments):
                self.parse_root_comments(com.comments())
            elif com.stickied:
                continue # Sticked comments are not replies to view
            else:
                self.stats["num_user_comments"] += 1
                self.stats["num_root_comments"] += 1
                if str(com.author) == self.author:
                    self.stats["num_OP_comments"] += 1
                self.parse_replies(com.replies)

        self.parsed = True

    @can_fail
    def parse_replies(self, reply_tree):
        """
        """
        reply_tree.replace_more(limit=None)

        for reply in reply_tree.list():
            try:
                if str(reply.author) == "DeltaBot":
                    self.parse_delta_bot_comment(reply)
                else:
                    self.stats["num_user_comments"] += 1
            except AttributeError: # If author is None, then user is deleted
                self.stats["num_user_comments"] += 1

            # Check for OP comments
            try:
                if str(reply.author) == self.author:
                    self.stats["num_OP_comments"] += 1
                else:
                    self.stats["num_user_comments"] += 1
            except AttributeError: # If author is None, then user is deleted
                pass

    @can_fail
    def parse_delta_bot_comment(self, comment):
        """
        """
        text = comment.body
        if "Confirmed" in text: # If delta awarded
            parent_com = comment.parent()

            # This is probably overkill, but I check to make sure DeltaBot
            # actually responded to a comment and not a submission.
            # (Submission are always by OP, comments are not.)
            if isinstance(parent_com, praw.models.Comment):
                if parent_com.author.name == self.author:
                    self.stats["OP_gave_delta"] = True
                    self.stats["num_deltas_from_OP"] += 1
    
    def get_stats_series(self):
        """
        This function returns a series so this class can update the submissions
        dataframe
        """
        info_series = pd.Series(self.stats)
        info_series.sort_index(inplace=True)
        return info_series


# TODO(jcm): Implement the inheritance from praw"s Redditor class, would be a 
# more effective use of OOP
class CMVSubAuthor:
    """
    Class for scraping the history of an author of /r/changemyview
    """
    STATS_TEMPLATE = {"sub_id": [],
                      "com_id": [],
                      "sub_inst": [],
                      "com_inst": [],
                      "com_newest": []}
    def __init__(self, redditor_inst):
        """
        """
        self.user = redditor_inst
        self.user_name = redditor_inst.name

        # Important variables to track
        self.history = {"sub_id": [],
                        "com_id": [],
                        "sub_inst": [],
                        "com_inst": [],
                        "com_newest": []}
    
    @can_fail
    def get_history_for(self, post_type):
        """
        """
        # Get posts
        post_generator = getattr(self.user, post_type)
        posts = post_generator.new(limit=None)

        posts_retrieved = 0
        post_prefix = post_type[:3]
        for post in posts:
            posts_retrieved += 1
            self.history[post_prefix + "_id"].append(post.id)
            self.history[post_prefix + "_inst"].append(post)
            if post_prefix == "com":
                self.history["com_newest"].append(True)

        if posts_retrieved in  [999, 1000]:
            print("\t999 or 1000 {} retrieved exactly,"
                  " attempting to retrive more for {}.".format(
                    post_type, self.user_name))
            self.get_more_history_for(post_prefix, post_type,
                                      post_generator)
        elif posts_retrieved > 1000:
            print("{} {} retrieved, don't have to worry about comment limit".format(
                posts_retrieved, post_type))
        else:
            print("\t{} {} retrieved for {}".format(posts_retrieved,
                                                    post_type, self.user_name))

    @can_fail
    def get_more_history_for(self, post_prefix, post_type, post_generator):
        """
        """
        con_posts = post_generator.controversial(limit=None)
        hot_posts = post_generator.hot(limit=None)
        top_posts = post_generator.top(limit=None)

        new_posts_found, same_posts_found = 0, 0
        for post_types in zip(con_posts, hot_posts, top_posts):
            for post in post_types:
                if post not in self.history[post_prefix + "_id"]:
                    new_posts_found += 1
                    self.history[post_prefix + "_id"].append(post.id)
                    self.history[post_prefix + "_inst"].append(post)
                    if post_prefix == "com":
                        self.history["com_newest"].append(False)
                else:
                    same_posts_found += 1
 
        if new_posts_found == 3000:
            print("Maximum number (3000) of new {} found".format(post_type))
        else:
            print("\t{} new and {} same {} found".format(new_posts_found,
                same_posts_found, post_type))

    def get_post_df(self, post_type):
        """
        This function returns a series so this class can update the authors"
        comments or submissions dataframe in CMVScraperModder.
        """
        attribution_dict = {post_type_key: value for post_type_key, value in
                            self.history.items() if post_type[:3] ==
                            post_type_key[:3]}
        attribution_dict.update({"author": self.user_name})
        return pd.DataFrame(attribution_dict)

# TODO(jcm): Make CMVSubmission inherit from CMVAuthSubmission(?)
class CMVAuthSubmission:
    """
    """
    STATS_TEMPLATE = {"created_utc": None,
                      "score": None,
                      "subreddit": None,
                      "content": None,
                      "num_root_comments": 0,
                      "num_user_comments": 0,
                      "num_unique_users": 0,
                      "has_deleted_user": False,
                      "title": None}

    @can_fail
    def __init__(self, submission_inst):
        """
        """
        self.submission = submission_inst
        self.stats = {"created_utc": None,
                      "score": None,
                      "subreddit": None,
                      "content": None,
                      "num_root_comments": 0,
                      "num_user_comments": 0,
                      "has_deleted_user": False,
                      "title": None}
        self.unique_users = set()

        # Stats that can be gathered right off the bat
        self.stats["created_utc"] = self.submission.created_utc
        self.stats["score"] = self.submission.score
        self.stats["subreddit"] = self.submission.subreddit_name_prefixed
        self.stats["content"] = self.submission.selftext
        self.stats["title"] = self.submission.title
        
        # self.parse_root_comments(self.submission.comments)
        # self.num_unique_users = len(self.unique_users)
        self.parsed = True

    @can_fail
    def parse_root_comments(self, comment_tree=None):
        """
        """
        # if str(self.submission) == "5d3bj5":
            # pdb.set_trace()

        for com in comment_tree:
            if isinstance(com, praw.models.MoreComments):
                self.parse_root_comments(com.comments())
            elif com.stickied:
                continue # Sticked comments are not replies to view
            else:
                self.stats["num_user_comments"] += 1
                self.stats["num_root_comments"] += 1
                try:
                    com_author = com.author.name
                except AttributeError: # If author is None, then user is deleted
                    self.stats["has_deleted_user"] = True
                    com_author = "[deleted]"
                self.unique_users.add(com_author)
                self.parse_replies(com.replies)

    @can_fail
    def parse_replies(self, reply_tree):
        """
        """
        reply_tree.replace_more(limit=None)

        for reply in reply_tree.list():
            self.stats["num_user_comments"] += 1
            try:
                reply_author = reply.author.name
            except AttributeError: # If author is None, then user is deleted
                self.stats["has_deleted_user"] = True
                reply_author = "[deleted]"
            self.unique_users.add(reply_author)

    def get_stats_series(self):
        """
        """
        info_series = pd.Series(self.stats)
        info_series.sort_index(inplace=True)
        return info_series

# STATS_TEMPLATE for date, score, subreddit. Could probably include a general
# method to update that dictionary in self.stats as well. Would also reduce
# redundancy in having 2 get_stats_series.
class CMVAuthComment:
    """
    """
    STATS_TEMPLATE = {"created_utc": None,
            "score": None,
            "subreddit": None,
            "content": None,
            "edited": None,
            "num_replies": 0,
            "parent_submission": None,
            "parent_comment": False} 
    @can_fail
    def __init__(self, comment_inst):
        """
        """
        self.comment = comment_inst
        self.stats = {"created_utc": None,
                      "score": None,
                      "subreddit": None,
                      "content": None,
                      "edited": None, 
                      "num_replies": 0,
                      "parent_submission": None,
                      "parent_comment": False}
        self.unique_users = set()

        # Stats that can be gathered right away
        self.stats["created_utc"] = self.comment.created_utc
        self.stats["score"] = self.comment.score
        self.stats["subreddit"] = (self.comment.submission.
                                  subreddit_name_prefixed)
        self.stats["content"] = self.comment.body
        self.stats["edited"] = self.comment.edited
        self.stats["parent_submission"] = self.comment.submission
        self.stats["parent_comment"] = self.comment.parent()

        self.parse_replies(comment_inst.replies)
        self.stats["num_unique_users"] = len(self.unique_users)
        self.parsed = True

    @can_fail
    def parse_replies(self, reply_tree):
        """
        """
        reply_tree.replace_more(limit=None)

        for reply in reply_tree.list():
            self.stats["num_user_comments"] += 1
            try:
                reply_author = reply.author.name
            except AttributeError: # If author is None, then user is deleted
                self.stats["has_deleted_user"] = True
                reply_author = "[deleted]"
            self.unique_users.add(reply_author)

    def get_stats_series(self):
        """
        """
        info_series = pd.Series(self.stats)
        info_series.sort_index(inplace=True)
        return info_series

if __name__ == "__main__":
    SModder = CMVScraperModder(START_2016, END_2016)

    SModder.update_cmv_submissions()
    SModder.update_author_history()
    with open("test.pkl", "wb") as output:
        pickle.dump(SModder, output)
