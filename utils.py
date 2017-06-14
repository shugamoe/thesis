# File to hold general purpose utility functions for scraping cmv or otherwise.
#
#


import time
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
