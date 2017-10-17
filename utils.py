# File to hold general purpose utility functions for scraping cmv or otherwise.
#
#


import calendar, time
from datetime import datetime
import pickle
import pdb
from prawcore.exceptions import Forbidden, NotFound


def can_fail(praw_call, *args, **kwargs):
    """
    A decorator to handle praw calls that can encounter sever errors
    """
    # pdb.set_trace()
    def robust_praw_call(self, *args, **kwargs):
        """
        This is a function that takes the praw call and the class and makes sure
        that it can retry the server if an HTTP error is encountered.
        """
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
                # pdb.set_trace()
                print("AttributeError")
                print("\n\t{}".format(str(e)))
                print("\tTrying: {}".format(praw_call.__name__))
                call_successful = True
                # pdb.set_trace()
            except RuntimeError as e:
                print("RuntimeError")
                print("\n\t{}".format(str(e)))
                print("\tTrying: {}".format(praw_call.__name__))
                call_successful = True
                # pdb.set_trace()
        if "praw_call_result" not in locals():
            praw_call_result = None

        return praw_call_result

    return robust_praw_call

def to_utc_epoch(time_str, format="%a %b %d %H:%M:%S %Y %Z"):
    """
    Takes a time string and converts it to UTC epoch
    """
    parsed_time = datetime.strptime(time_str, format)
    return int(datetime.strftime(parsed_time, "%s"))

# Manually get mod information now, write automation and checker later
MOD_LIST = [("Snorrrlax", "Wed Jan 16 23:34:39 2013 UTC"),
            ("protagornast", "Sat Jan 19 05:51:24 2013 UTC"),
            ("TryUsingScience", "Thu Mar 7 18:21:23 2013 UTC"),
            ("IAmAN00bie", "Thu Apr 11 22:04:50 2013 UTC"),
            ("cwenham", "Sun Aug 18 18:51:21 2013 UTC"),
            ("convoces", "Sun Oct 20 16:12:10 2013 UTC"),
            ("Nepene", "Sun Oct 20 16:39:29 2013 UTC"),
            ("Grunt08", "Thu Feb 20 03:34:58 2014 UTC"),
            ("hacksoncode", "Thu Feb 20 22:04:49 2014 UTC"),
            ("garnteller", "Fri Mar 28 14:48:56 2014 UTC"),
            ("GnosticGnome", "Fri Jun 27 00:57:18 2014 UTC"),
            ("mehatch", "Tue Jan 27 23:59:43 2015 UTC"),
            ("bubi09", "Fri Feb 6 13:34:21 2015 UTC"),
            ("huadpe", "Sat Mar 28 16:15:00 2015 UTC"),
            ("IIIBlackhartIII", "Tue Sep 8 04:14:05 2015 UTC"),
            ("RustyRook", "Wed Jan 27 03:32:49 2016 UTC"),
            ("AutoModerator", "Tue Feb 2 22:48:06 2016 UTC"),
            ("qtx", "Wed Mar 9 13:29:30 2016 UTC"),
            ("FlyingFoxOfTheYard_", "Mon Sep 26 18:10:33 2016 UTC"),
            ("etquod", "Mon Sep 26 20:00:42 2016 UTC"),
            ("e36", "Sun Apr 2 21:44:58 2017 UTC"),
            ("whitef530 ", "Sun Apr 2 22:29:06 2017 UTC"),
            ("Ansuz07", "Sat Jul 15 23:27:12 2017 UTC"),
            ("neofederalist", "Mon Sep 11 18:38:43 2017 UTC"),
            ("Evil_Thresh", "Tue Sep 12 04:55:40 2017 UTC"),
            ("kochirakyosuke", "Wed Sep 13 19:42:03 2017 UTC"),
            ("howbigis1gb", "Mon Sep 25 16:57:25 2017 UTC"),
            ("ColdNotion", "Sat Oct 14 15:46:35 2017 UTC"),
            ("DeltaBot", "Thu Jan 9 23:37:43 2014 UTC")
           ]
MOD_KEY = {user_name: to_utc_epoch(date_str) for user_name, date_str in MOD_LIST}
