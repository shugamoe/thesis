"""
File that holds objects that handle the scraping of the related CMV data types of interest.
"""


import pdb
import praw
from sqlalchemy.exc import OperationalError, IntegrityError
from utils import can_fail, MOD_KEY
from cmv_tables import SQLASub, SQLACMVSub, SQLAComment, SQLACMVComment, SQLACMVSubAuthor, SQLACMVModComment



class GatherSub:
    """
    A class of a /r/changemyview submission
    """
    sqla_mapping = SQLASub

    @can_fail
    def __init__(self, sub_inst, scraper):
        self.submission = sub_inst
        self.db_session = scraper.session
        self.scraper = scraper

        self.stats = {
              "score": None,
              "content": None,
              "title": None,
              "reddit_id": None,
              "author": None,
              "subreddit": None,
              "edited": False,
              "title": None,
              "unique_commentors": 0,
              "author_comments": 0,
              "total_comments": 0,
              "direct_comments": 0,
                      }

        # Get author first
        try:
            self.stats["author"] = self.submission.author.name
        except AttributeError:
            self.stats["author"] = "[deleted]"

        # Gather info from submission itself
        self.stats["score"] = self.submission.score
        self.stats["content"] = self.submission.selftext
        self.stats["date_utc"] = self.submission.created_utc
        self.stats["reddit_id"] = self.submission.id
        self.stats["subreddit"] = self.submission.subreddit_name_prefixed
        self.stats["edited"] = self.submission.edited
        self.stats["title"] = self.submission.title

        # Gather info from submission's comments
        self.parse_root_comments(self.submission.comments)

    @can_fail
    def parse_root_comments(self, comment_tree):
        """
        """
        for com in comment_tree:
            if isinstance(com, praw.models.MoreComments):
                self.parse_root_comments(com.comments())
            elif com.stickied:
                if com.author == "DeltaBot":
                    pass
                continue # Sticked comments are not replies to view
            else:
                self.stats["total_comments"] += 1
                self.stats["direct_comments"] += 1
                if str(com.author) == self.stats["author"]:
                    self.stats["author_comments"] += 1
                self.parse_replies(com.replies)
                if self.scraper.all_com_content:
                    GatherComment(com, self.scraper).save_to_db()

    @can_fail
    def parse_replies(self, reply_tree):
        """
        """
        reply_tree.replace_more(limit=None)
        for reply in reply_tree.list():
            self.stats["total_comments"] += 1
            try:
                reply_author = reply.author.name
                self.unique_users.add(reply_author)
            except AttributeError: # If author is None, then user is deleted
                pass

            # Check for OP comments
            try:
                if str(reply.author) == self.author:
                    self.stats["author_comments"] += 1
                else:
                    self.stats["total_comments"] += 1
            except AttributeError: # If author is None, then user is deleted
                pass

    
    def save_to_db(self):
        """
        Writes the stats of the object to the SQL datebase
        """
        def save_new_info():
            """
            Wrapper for standard procedure, no integrity error
            """
            try:
                sqla_obj = self.sqla_mapping(**self.stats)
                self.db_session.add(sqla_obj)
                self.db_session.commit()
            except OperationalError:
                self.db_session.rollback()
                self.stats["title"] = self.stats["title"].encode("unicode_escape")
                self.stats["content"] = self.stats["content"].encode("unicode_escape")
                sqla_obj = self.sqla_mapping(**self.stats)
                self.db_session.add(sqla_obj)
                self.db_session.commit()
        try:
            save_new_info()
        except IntegrityError:
            self.db_session.rollback()
            pass


# Would like to have this inherit from praw"s submissions class but with the way
# I"m scraping the data I would have to tinker with a praw"s sublisting class
# and subreddit class.
class GatherCMVSub:
    """
    A class of a /r/changemyview submission
    """
    sqla_mapping = SQLACMVSub

    @can_fail
    def __init__(self, sub_inst, scraper):
        self.submission = sub_inst
        self.db_session = scraper.session
        self.scraper = scraper

        self.stats = {
            "score": None,
            "content": None,
            "reddit_id": None,
            "author": None,
            "subreddit": None,
            "edited": False,
            "title": None,
            "unique_commentors": 0,
            "author_comments": 0,
            "total_comments": 0,
            "direct_comments": 0,
            "deltas_from_author": 0,
            "deltas_from_other": 0,
            "cmv_mod_comments": 0
}

        # Get author first
        try:
            self.stats["author"] = self.submission.author.name
        except AttributeError:
            self.stats["author"] = "[deleted]"

        # Gather info from submission itself
        self.stats["score"] = self.submission.score
        self.stats["content"] = self.submission.selftext
        self.stats["date_utc"] = self.submission.created_utc
        self.stats["reddit_id"] = self.submission.id
        self.stats["subreddit"] = self.submission.subreddit_name_prefixed
        self.stats["edited"] = self.submission.edited
        self.stats["title"] = self.submission.title

        # Gather info from submission's comments
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
                self.stats["total_comments"] += 1
                self.stats["direct_comments"] += 1

                try:
                    com_author = str(com.author)
                    if com_author == self.stats["author"]:
                        self.stats["author_comments"] += 1
                    elif com_author in MOD_KEY.keys():
                        # Could be mod comment
                        if MOD_KEY[com_author] < com.created_utc:
                            # Confirm moderator was made moderator at current time
                            self.stats["cmv_mod_comments"] += 1
                            GatherCMVModComment(com, self.scraper).save_to_db()
                            if com_author == "DeltaBot":
                                self.parse_delta_bot_comment(com)

                # Author is likely "[deleted]"
                except AttributeError:
                    pass
                self.parse_replies(com.replies)

                if self.scraper.cmv_com_content:
                    GatherCMVComment(com, self.scraper).save_to_db()

    @can_fail
    def parse_replies(self, reply_tree):
        """
        """
        reply_tree.replace_more(limit=None)

        for reply in reply_tree.list():
            try:
                if str(reply.author) == "DeltaBot":
                    self.parse_delta_bot_comment(reply)
                    # GatherCMVModComment(reply, self.scraper).save_to_db()
                else:
                    self.stats["total_comments"] += 1
            except AttributeError: # If author is None, then user is deleted
                self.stats["total_comments"] += 1

            # Check for OP comments
            try:
                if str(reply.author) == self.author:
                    self.stats["author_comments"] += 1
                else:
                    self.stats["total_comments"] += 1
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
                try:
                    if parent_com.author.name == self.stats["author"]:
                        self.stats["deltas_from_author"] += 1
                    else:
                        self.stats["deltas_from_other"] += 1
                except AttributeError: # Person awarding delta deleted their comment,
                        # TODO(jcm), the parent author is saved in the delta bot comment status
                        # Count it as a delta from other
                    self.stats["deltas_from_other"] += 1

    def save_to_db(self):
        """
        Writes the stats of the object to the SQL datebase
        """
        def save_new_info():
            """
            Wrapper for standard procedure, no integrity error
            """
            try:
                sqla_obj = self.sqla_mapping(**self.stats)
                self.db_session.add(sqla_obj)
                self.db_session.commit()
            except OperationalError:
                self.db_session.rollback()
                self.stats["title"] = self.stats["title"].encode("unicode_escape")
                self.stats["content"] = self.stats["content"].encode("unicode_escape")
                sqla_obj = self.sqla_mapping(**self.stats)
                self.db_session.add(sqla_obj)
                self.db_session.commit()
        try:
            save_new_info()
        except IntegrityError:
            self.db_session.rollback()
            pass


class GatherCMVModComment:
    """
    """
    sqla_mapping = SQLACMVModComment

    @can_fail
    def __init__(self, comment_inst, scraper):
        """
        """
        if comment_inst.id == "d29udmn":
            # pdb.set_trace()
            pass
        self.scraper = scraper
        self.db_session = scraper.session
        self.comment = comment_inst
        self.stats = {
            "score": None,
            "content": None,
            "date_utc": None,
            "reddit_id": None,
            "author": None,
            "subreddit": None,
            "edited": None,
            "parent_submission_id": None,
            "total_children": 0,
            "replies": 0,
            "author_children": 0,
            "parent_comment_id": None,
            }
        self.unique_repliers = set()

        # Get author first
        try:
            self.stats["author"] = self.comment.author.name
        except AttributeError:
            self.stats["author"] = "[deleted]"

        # Stats that can be gathered right away
        self.stats["score"] = self.comment.score
        self.stats["content"] = self.comment.body
        self.stats["date_utc"] = self.comment.created_utc
        self.stats["reddit_id"] = self.comment.id
        self.stats["subreddit"] = (self.comment.submission.
                                  subreddit_name_prefixed)
        self.stats["edited"] = self.comment.edited
        self.stats["parent_submission_id"] = self.comment.submission
        self.stats["parent_comment_id"] = (self.comment.parent().id if 
                isinstance(self.comment.parent(), praw.models.reddit.comment.Comment) else None)

        self.parse_replies(comment_inst.replies)
        self.stats["unique_repliers"] = len(self.unique_repliers)

    @can_fail
    def parse_replies(self, reply_tree):
        """
        """
        reply_tree.replace_more(limit=None)

        for reply in reply_tree.list():
            try:
                if str(reply.author) == "DeltaBot":
                    self.parse_delta_bot_comment(reply)
                    GatherCMVModComment(reply, self.scraper).save_to_db()
                else:
                    self.stats["total_children"] += 1
            except AttributeError: # If author is None, then user is deleted
                self.stats["total_children"] += 1

            # Check for OP comments
            try:
                if str(reply.author) == self.author:
                    self.stats["author_children"] += 1
                else:
                    self.stats["total_children"] += 1
            except AttributeError: # If author is None, then user is deleted
                pass


    def save_to_db(self):
        """
        Writes the stats of the object to the SQL datebase
        """
        def save_new_info():
            """
            Wrapper for standard procedure, no integrity error
            """
            try:
                sqla_obj = self.sqla_mapping(**self.stats)
                self.db_session.add(sqla_obj)
                self.db_session.commit()
            except OperationalError:
                self.db_session.rollback()
                self.stats["title"] = self.stats["title"].encode("unicode_escape")
                self.stats["content"] = self.stats["content"].encode("unicode_escape")
                sqla_obj = self.sqla_mapping(**self.stats)
                self.db_session.add(sqla_obj)
                self.db_session.commit()
        try:
            save_new_info()
        except IntegrityError:
            self.db_session.rollback()
            pass


class GatherComment:
    """
    """
    sqla_mapping = SQLAComment

    @can_fail
    def __init__(self, comment_inst, scraper):
        """
        """
        self.scraper = scraper
        self.db_session = scraper.session
        self.comment = comment_inst
        self.stats = {
            "score": None,
            "content": None,
            "date_utc": None,
            "reddit_id": None,
            "author": None,
            "subreddit": None,
            "edited": None,
            "parent_submission_id": None,
            "replies": 0,
            "author_children": 0,
            "total_children": 0,
            "parent_comment_id": None,
            "unique_repliers": 0
            }
        self.unique_repliers = set()

        # Get author first
        try:
            self.stats["author"] = self.comment.author.name
        except AttributeError:
            self.stats["author"] = "[deleted]"

        # Stats that can be gathered right away
        self.stats["score"] = self.comment.score
        self.stats["content"] = self.comment.body
        self.stats["date_utc"] = self.comment.created_utc
        self.stats["reddit_id"] = self.comment.id
        self.stats["subreddit"] = (self.comment.submission.
                                  subreddit_name_prefixed)
        self.stats["edited"] = self.comment.edited
        self.stats["parent_submission_id"] = self.comment.submission.id
        self.stats["parent_comment_id"] = (self.comment.parent().id if 
                isinstance(self.comment.parent(), praw.models.reddit.comment.Comment) else None)

        self.parse_replies(comment_inst.replies)
        self.stats["unique_repliers"] = len(self.unique_repliers)

    @can_fail
    def parse_replies(self, reply_tree):
        """
        """
        reply_tree.replace_more(limit=None)

        for reply in reply_tree.list():
            try:
                self.unique_repliers.add(reply.author)
            except AttributeError: # If author is None, then user is deleted
                pass
            self.stats["total_children"] += 1

            # Test whether the reply is a direct reply to the current comment
            if isinstance(reply.parent(), praw.models.reddit.comment.Comment):
                if reply.parent().id == self.stats["reddit_id"]:
                    self.stats["replies"] += 1

            # Check for OP comments
            try:
                if str(reply.author) == self.author:
                    self.stats["author_children"] += 1
                else:
                    self.stats["total_children"] += 1
            except AttributeError: # If author is None, then user is deleted
                pass

    def save_to_db(self):
        """
        Writes the stats of the object to the SQL datebase
        """
        def save_new_info():
            """
            Wrapper for standard procedure, no integrity error
            """
            try:
                sqla_obj = self.sqla_mapping(**self.stats)
                self.db_session.add(sqla_obj)
                self.db_session.commit()
            except OperationalError:
                self.db_session.rollback()
                self.stats["content"] = self.stats["content"].encode("unicode_escape")
                sqla_obj = self.sqla_mapping(**self.stats)
                self.db_session.add(sqla_obj)
                self.db_session.commit()

        try:
            save_new_info()
        except IntegrityError:
            self.db_session.rollback()
            pass

class GatherCMVComment:
    """
    """
    sqla_mapping = SQLACMVComment

    @can_fail
    def __init__(self, comment_inst, scraper):
        """
        """
        try:
            self.OP = comment_inst.submission().author
        except:
            self.OP = "[deleted]"
        self.scraper = scraper
        self.db_session = scraper.session
        self.comment = comment_inst
        self.stats = {
            "score": None,
            "content": None,
            "date_utc": None,
            "reddit_id": None,
            "author": None,
            "subreddit": None,
            "edited": None,
            "parent_submission_id": None,
            "replies": 0,
            "author_children": 0,
            "total_children": 0,
            "parent_comment_id": None,
            "deltas_from_other": 0,
            "deltas_from_OP": 0,
            "unique_repliers": 0
            }
        self.unique_repliers = set()

        # Get author first
        try:
            self.stats["author"] = self.comment.author.name
        except AttributeError:
            self.stats["author"] = "[deleted]"

        # Stats that can be gathered right away
        self.stats["score"] = self.comment.score
        self.stats["content"] = self.comment.body
        self.stats["date_utc"] = self.comment.created_utc
        self.stats["reddit_id"] = self.comment.id
        self.stats["subreddit"] = (self.comment.submission.
                                  subreddit_name_prefixed)
        self.stats["edited"] = self.comment.edited
        self.stats["parent_submission_id"] = self.comment.submission.id
        self.stats["parent_comment_id"] = (self.comment.parent().id if 
                isinstance(self.comment.parent(), praw.models.reddit.comment.Comment) else None)

        self.parse_replies(comment_inst.replies)
        self.stats["unique_repliers"] = len(self.unique_repliers)
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

                    # Adds comment to DB
                    GatherCMVModComment(reply, self.scraper).save_to_db()
                else:
                    self.stats["total_children"] += 1
                self.unique_repliers.add(reply.author)
            except AttributeError: # If author is None, then user is deleted
                self.stats["total_replies"] += 1

            # Test whether the reply is a direct reply to the current comment
            if isinstance(reply.parent(), praw.models.reddit.comment.Comment):
                if reply.parent().id == self.stats["reddit_id"]:
                    self.stats["replies"] += 1

            # Check for OP comments
            try:
                if str(reply.author) == self.author:
                    self.stats["author_children"] += 1
                else:
                    self.stats["total_children"] += 1
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
            #
            # Also confirm that the comment deltabot responded to is in turn
            # responding to the comment the current instance of the class is
            # gathering stats for
            if ((isinstance(parent_com, praw.models.Comment)) and
                    (parent_com.parent().id == self.stats["reddit_id"])):
                try:
                    if parent_com.author.name == self.OP:
                        self.stats["deltas_from_OP"] += 1
                    else:
                        self.stats["deltas_from_other"] += 1
                except AttributeError: # Person awarding delta deleted their comment,
                        # Count it as a delta from other
                    self.stats["deltas_from_other"] += 1


    def save_to_db(self):
        """
        Writes the stats of the object to the SQL datebase
        """
        def save_new_info():
            """
            Wrapper for standard procedure, no integrity error
            """
            try:
                sqla_obj = self.sqla_mapping(**self.stats)
                self.db_session.add(sqla_obj)
                self.db_session.commit()
            except OperationalError:
                self.db_session.rollback()
                self.stats["title"] = self.stats["title"].encode("unicode_escape")
                self.stats["content"] = self.stats["content"].encode("unicode_escape")
                sqla_obj = self.sqla_mapping(**self.stats)
                self.db_session.add(sqla_obj)
                self.db_session.commit()

            return self.stats["deltas_from_OP"] + self.stats["deltas_from_other"]
        try:
            return save_new_info()
        except IntegrityError:
            self.db_session.rollback()
            pass


class GatherCMVSubAuthor:
    """
    Class for scraping the history of an author of /r/changemyview
    """
    sqla_mapping = SQLACMVSubAuthor
    praw_model = {"com": praw.models.reddit.comment.Comment,
                  "sub": praw.models.reddit.submission.Submission
                  }
    gather_model = {"com": [GatherComment, GatherCMVComment],
                    "sub": [GatherSub, GatherCMVSub]
                     }

    def __init__(self, redditor_name, scraper):
        """
        """
        self.user = scraper.praw_agent.redditor(redditor_name)
        self.scraper = scraper
        self.db_session = scraper.session

        # Important variables to track
        self.stats = {"user_name": redditor_name,
                      "submissions": 0,
                      "comments": 0,
                      "cmv_comments": 0,
                      "cmv_submissions": 1,
                      "deltas_awarded": 0
                }

        self.history = {"com_id": set([cmv_com.reddit_id for cmv_com in 
                            self.db_session.query(SQLACMVComment)]),
                        "sub_id": set([cmv_sub.reddit_id for cmv_sub in 
                            self.db_session.query(SQLACMVSub)])
                        }
        assert len(self.history["sub_id"]) >= 1

    @can_fail
    def get_history_for(self, post_type):
        """
        """
        # Get posts
        post_generator = getattr(self.user, post_type)
        posts = post_generator.new(limit=None)

        posts_retrieved = 0
        post_prefix = post_type[:3]
        print("\tRetrieving {} for {}".format(post_type, self.stats["user_name"]))
        
        com_limit = 5 
        for post in posts:
            # Check if we already gathered the cmv_comment or submission
            if post.id not in self.history["{}_id".format(post_prefix)]:
                posts_retrieved += 1
                self.stats[post_type] += 1
                if post.subreddit == "changemyview":
                    gather_model = self.gather_model[post_prefix][1]

                    pos_deltas_received = gather_model(post, self.scraper).save_to_db()
                    self.stats["cmv_{}".format(post_type)] += 1

                    # Check if pos_deltas_received returned a number
                    if isinstance(pos_deltas_received, int):
                        self.stats["deltas_awarded"] += pos_deltas_received
                else:
                    self.gather_model[post_prefix][0](post, self.scraper).save_to_db()
                if post_prefix == "com":
                    if posts_retrieved == com_limit:
                        break


        if posts_retrieved in  [999, 1000]:
            print("\t\t999 or 1000 {} retrieved exactly,"
                  " attempting to retrive more for {}.".format(
                      post_type, self.stats["user_name"]))
            self.get_more_history_for(post_prefix, post_type,
                                      post_generator)
        elif posts_retrieved > 1000:
            print("\t\t{} {} retrieved, don't have to worry about comment limit".format(
                posts_retrieved, post_type))
        else:
            print("\t\t{} {} retrieved for {}".format(posts_retrieved,
                                                    post_type, self.stats["user_name"]))

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
                    self.stats[post_type] += 1
                    # Add id so we don't gather it again
                    self.history[post_prefix + "_id"].add(post.id)

                    # Gather the post
                    if post.subreddit == "changemyview":
                        self.stats["cmv_{}".format(post_type)] += 1
                        pos_deltas_received = self.gather_model[post_prefix][1](post,
                                self.scraper).save_to_db()

                        # Check if pos_deltas_received returned a number
                        # (will if comment, no if submission)
                        if isinstance(pos_deltas_received, int):
                            self.stats["deltas_awarded"] += pos_deltas_received
                    else:
                        self.gather_model[post_prefix][0](post, self.scraper).save_to_db()
                else:
                    same_posts_found += 1
 
        if new_posts_found == 3000:
            print("Maximum number (3000) of new {} found".format(post_type))
        else:
            print("\t{} new and {} same {} found".format(new_posts_found,
                same_posts_found, post_type))

    def save_to_db(self):
        """
        Writes the stats of the object to the SQL datebase
        """
        def save_new_info():
            """
            Wrapper for standard procedure, no integrity error
            """
            self.db_session.rollback()
            sqla_obj = self.sqla_mapping(**self.stats)
            self.db_session.add(sqla_obj)
            self.db_session.commit()
        try:
            save_new_info()
        except IntegrityError:
            self.db_session.rollback()
            pass

