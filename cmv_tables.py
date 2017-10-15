"""
File for initializing tables in cmv_related.db
"""

import pdb
from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, UnicodeText
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
STR_ENCODE = "utf8"

class RedditContentMixin:
    """
    Columns that ought to be common to any submission or comment on Reddit
    """
    # Pre Interaction Info
    score = Column(Integer, nullable=False)
    date_utc = Column(Integer, nullable=False)
    reddit_id = Column(String(7, convert_unicode=True), primary_key=True)
    author = Column(String(20, convert_unicode=True), nullable=False)
    subreddit = Column(String(20, convert_unicode=True), nullable=False)
    edited = Column(Boolean, nullable=False)

class SubmissionMixin(RedditContentMixin):
    """
    Columns that ought to be common to any submission on Reddit
    """
    title = Column(String(500, convert_unicode=True), nullable=False)
    content = Column(String(40000, convert_unicode=True), nullable=False)
    direct_comments = Column(Integer, nullable=False)
    total_comments = Column(Integer, nullable=False)
    author_comments = Column(Integer, nullable=False)
    unique_commentors = Column(Integer, nullable=False)

class SQLASub(SubmissionMixin, Base):
    """
    """
    __tablename__ = "Submission"
    reddit_id = Column(String(7, convert_unicode=True), primary_key=True)


class SQLACMVSub(SubmissionMixin, Base):
    __tablename__ = "CMV_Submission"

    # Post Interaction Info
    deltas_from_author = Column(Integer, nullable=False)
    deltas_from_other = Column(Integer, nullable=False)
    cmv_mod_comments = Column(Integer, nullable=False)

class CommentMixin(RedditContentMixin):
    content = Column(String(10000, convert_unicode=True), nullable=False)
    parent_submission_id =  Column(String(7, convert_unicode=True), nullable=False)
    replies = Column(Integer, nullable=False)
    author_children = Column(Integer, nullable=False)
    total_children = Column(Integer, nullable=False)
    unique_repliers = Column(Integer, nullable=False) 
    parent_comment_id = Column(String(7, convert_unicode=True))

class SQLACMVModComment(CommentMixin, Base):
    __tablename__ = "CMV_Mod_Comment"
    is_deltabot_com = Boolean()
    # If no parent_comment_id a sticky at top of post, otherwise
    # a reply to a user

class SQLAComment(CommentMixin, Base):
    __tablename__ = "Comment"

class SQLACMVComment(CommentMixin, Base):
    __tablename__ = "CMV_Comment"
    deltas_from_other = Column(Integer, nullable=False)
    deltas_from_OP = Column(Integer, nullable=False)

class RedditorMixin:
    user_name = Column(String(20, convert_unicode=True), primary_key=True)
    submissions = Column(Integer, nullable=False)
    comments = Column(Integer, nullable=False)

class SQLACMVSubAuthor(RedditorMixin, Base):
    __tablename__ = "CMV_Sub_Author"
    cmv_submissions = Column(Integer, nullable=False)
    cmv_comments = Column(Integer, nullable=False)
    deltas_awarded = Column(Integer, nullable=False)


def init_tables(engine):
    """
    File for initializing tables in cmv_related.db
    """
    # Create Table in db
    engine.execute("USE jmcclellanDB;") # select new db
    engine.execute("DROP TABLE IF EXISTS CMV_Submission,"
                                         "Submission,"
                                         "DeltaBot_Comment,"
                                         "CMV_Mod_Comment,"
                                         "CMV_Comment,"
                                         "Comment,"
                                         "CMV_Sub_Author"
                                         ";"
            )
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    pass

