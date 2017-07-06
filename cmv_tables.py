"""
File for initializing tables in cmv_related.db
"""

from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class RedditContentMixin(object):
    """
    Columns that ought to be common to any submission or comment on Reddit
    """
    # Pre Interaction Info
    reddit_id = Column(String, primary_key=True)
    author = Column(String)
    date_utc = Column(Integer)
    title = Column(String)
    content = Column(String)
    score = Column(Integer)

class SubmissionMixin(RedditContentMixin):
    """
    Columns that ought to be common to any submission on Reddit
    """
    direct_comments = Column(Integer)
    total_comments = Column(Integer)
    author_comments = Column(Integer)
    unique_participants = Column(Integer)
    subreddit = Column(String)

class CMVSub(SubmissionMixin, Base):
    __tablename__ = "CMV_Submissions"

    # Post Interaction Info
    delta_from_author = Column(Boolean)
    num_deltas_from_author = Column(Integer)

class AuthSub(SubmissionMixin, Base):
    __tablename__ = "CMV_Author_Submissions"



def init_tables(engine):
    """
    File for initializing tables in cmv_related.db
    """
    # Create Table in db
    Base.metadata.create_all(engine)

    # Add example
#    info_dict = {"reddit_id": "69",
#                 "author": "beefman",
#                 "date_utc": 1498256225,
#                 "title": "Beef > Pork?",
#                 "content": "Beef da best. Fight me irl",
#                 "delta_from_author": True,
#                 "direct_comments": 100,
#                 "total_comments": 150,
#                 "author_comments": 5,
#                 "unique_participants": 30}
#    ex_cmv_sub = CMVSub(**info_dict)

    # Add to session
#    session.add(ex_cmv_sub)
#    session.commit()

    # Bulk Inserts
    # session.bulk_save_objects([sub1, sub2])
    # session.commit()

# Queries
# session.query(CmvSub).all()

# Particular attributes
# print(session.query(CmvSub.reddit_id).first())


if __name__ == "__main__":
    pass

