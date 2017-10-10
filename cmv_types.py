"""
File that holds objects that handle the scraping of the related CMV data types of interest.
"""


import pdb
from utils import can_fail
import praw
from cmv_tables import CMVSubSchema, SubmissionSchema, CommentSchema



# Would like to have this inherit from praw"s submissions class but with the way
# I"m scraping the data I would have to tinker with a praw"s sublisting class
# and subreddit class.
class CMVSubmission:
    """
    A class of a /r/changemyview submission
    """
    sqla_mapping = CMVSubSchema

    @can_fail
    def __init__(self, sub_inst, db_session):
        self.submission = sub_inst
        self.db_session = db_session

        self.stats = {"reddit_id": None,
                      "subreddit": None,
                      "author": None,
                      "created_utc": None,
                      "title": None,
                      "content": None,
                      "score": None,
                      "edited": None,
                      "delta_from_author": False,
                      "direct_comments": 0,
                      "total_comments": 0,
                      "author_comments": 0,
                      "num_deltas_from_author": 0}
        self.unique_users = set()

        # Get author first
        try:
            self.stats["author"] = self.submission.author.name
        except AttributeError:
            self.stats["author"] = "[deleted]"

        # Gather info from submission itself
        self.stats["reddit_id"] = self.submission.id
        self.stats["subreddit"] = self.submission.subreddit_name_prefixed
        self.stats["content"] = self.submission.selftext
        self.stats["title"] = self.submission.title
        self.stats["created_utc"] = self.submission.created_utc
        self.stats["score"] = self.submission.score
        self.stats["edited"] = self.submission.edited
        self.unique_participants = len(self.unique_users)

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
                    com_author = com.author.name
                except AttributeError: # If author is None, then user is deleted
                    self.stats["has_deleted_user"] = True
                    com_author = "[deleted]"
                if com_author == self.stats["author"]:
                    self.stats["num_OP_comments"] += 1
                self.unique_users.add(com_author)
                self.parse_replies(com.replies)

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
                    self.stats["total_comments"] += 1
            except AttributeError: # If author is None, then user is deleted
                self.stats["total_comments"] += 1

            # Check for OP comments
            try:
                if str(reply.author) == self.author:
                    self.stats["num_OP_comments"] += 1
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
                if parent_com.author.name == self.stats["author"]:
                    self.stats["delta_from_author"] = True
                    self.stats["num_deltas_from_author"] += 1
    
    def save_to_db(self):
        """
        Writes the stats of the object to the SQL datebase
        """
        sqla_obj = self.sqla_mapping(**self.stats)
        self.db_session.add(sqla_obj)
        self.db_session.commit()


# TODO(jcm): Implement the inheritance from praw"s Redditor class, would be a 
# more effective use of OOP
class CMVSubAuthor:
    """
    Class for scraping the history of an author of /r/changemyview
    """
    def __init__(self, redditor_inst, session):
        """
        """
        self.user = redditor_inst
        self.user_name = redditor_inst.name
        self.db_session = session

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

            # Scrape data from submission or comment
            if post_prefix == "com":
                Comment(post, self.db_session).save_to_db()
            else:
                if post.subreddit == "r/changemyview":
                    CMVSubmission(post, self.db_session).save_to_db()
                else:
                    Submission(post, self.db_session).save_to_db()

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
        if post_prefix == "com":
            PostClass = Comment
        else:
            PostClass = Submission


        con_posts = post_generator.controversial(limit=None)
        hot_posts = post_generator.hot(limit=None)
        top_posts = post_generator.top(limit=None)

        new_posts_found, same_posts_found = 0, 0
        for post_types in zip(con_posts, hot_posts, top_posts):
            for post in post_types:
                gathered_posts = set(tup[0] for tup in self.db_session.query(PostClass.sqla_mapping.reddit_id).all())

                if post.id not in gathered_posts:
                    new_posts_found += 1
                    PostClass(post, self.db_session).save_to_db()
                else:
                    same_posts_found += 1

        if new_posts_found == 3000:
            print("Maximum number (3000) of new {} found".format(post_type))
        else:
            print("\t{} new and {} same {} found".format(new_posts_found,
                                                                         same_posts_found, post_type))


# TODO(jcm): Make CMVSubmission inherit from CMVAuthSubmission(?)
class Submission:
    """
    """
    sqla_mapping = SubmissionSchema
    STATS_TEMPLATE = {"created_utc": None,
                      "score": None,
                      "subreddit": None,
                      "content": None,
                      "direct_comments": 0,
                      "total_comments": 0,
                      "unique_participants": 0,
                      "has_deleted_user": False,
                      "title": None}
    @can_fail
    def __init__(self, submission_inst, session):
        """
        """
        self.submission = submission_inst
        self.stats = {"created_utc": None,
                      "score": None,
                      "subreddit": None,
                      "content": None,
                      "direct_comments": 0,
                      "total_comments": 0,
                      "has_deleted_user": False,
                      "title": None}
        self.db_session = session
        self.unique_users = set()

        # Stats that can be gathered right off the bat
        self.stats["reddit_id"] = self.submission.id
        self.stats["created_utc"] = self.submission.created_utc
        self.stats["score"] = self.submission.score
        self.stats["subreddit"] = self.submission.subreddit_name_prefixed
        self.stats["content"] = self.submission.selftext
        self.stats["title"] = self.submission.title

        self.parse_root_comments(self.submission.comments)
        self.unique_participants = len(self.unique_users)
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
                self.stats["total_comments"] += 1
                self.stats["direct_comments"] += 1
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
            self.stats["total_comments"] += 1
            try:
                reply_author = reply.author.name
            except AttributeError: # If author is None, then user is deleted
                self.stats["has_deleted_user"] = True
                reply_author = "[deleted]"
            self.unique_users.add(reply_author)

    def save_to_db(self):
        """
        """
        sqla_obj = self.sqla_mapping(**self.stats)
        self.db_session.add(sqla_obj)
        self.db_session.commit()

# STATS_TEMPLATE for date, score, subreddit. Could probably include a general
# method to update that dictionary in self.stats as well. Would also reduce
# redundancy in having 2 get_stats_series.
class Comment:
    """
    """
    sqla_mapping = CommentSchema

    @can_fail
    def __init__(self, comment_inst, session):
        """
        """
        self.comment = comment_inst
        self.stats = {"reddit_id": None,
                      "created_utc": None,
                      "score": None,
                      "subreddit": None,
                      "content": None,
                      "edited": None, 
                      "replies": 0,
                      "submission_id": None,
                      "parent_id": False}
        self.unique_users = set()
        self.db_session = session

        # Stats that can be gathered right away
        self.stats["reddit_id"] = self.comment.id
        self.stats["created_utc"] = self.comment.created_utc
        self.stats["score"] = self.comment.score
        self.stats["subreddit"] = (self.comment.submission.
                                  subreddit_name_prefixed)
        self.stats["content"] = self.comment.body
        self.stats["edited"] = self.comment.edited
        self.stats["submission_id"] = self.comment.submission.id
        self.stats["parent_id"] = self.comment.parent().id
        self.stats["is_direct_reply"] = (self.stats["parent_id"] == self.stats["submission_id"])

        self.parse_replies(comment_inst.replies)
        self.stats["unique_participants"] = len(self.unique_users)
        self.parsed = True

    @can_fail
    def parse_replies(self, reply_tree):
        """
        """
        reply_tree.replace_more(limit=None)

        for reply in reply_tree.list():
            self.stats["replies"] += 1
            try:
                reply_author = reply.author.name
            except AttributeError: # If author is None, then user is deleted
                self.stats["has_deleted_user"] = True
                reply_author = "[deleted]"
            self.unique_users.add(reply_author)

    def save_to_db(self):
        """
        """
        sqla_obj = self.sqla_mapping(**self.stats)
        self.db_session.add(sqla_obj)
        self.db_session.commit()
