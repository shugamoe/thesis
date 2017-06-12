"""
File that processes the raw pickle files from the SRCC
"""

import os
import re
import subprocess
import pandas as pd
import feather
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from nltk.tag import StanfordNERTagger
from nltk.tag import StanfordPOSTagger
import pdb
import sklearn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans


# Stanford Parser Java stuff
stanfordVersion = "2016-10-31"
parserVersion = "3.7.0"
stanfordDir = "/home/jmcclellan/stanford-NLP"

modelName = 'englishPCFG.ser.gz'
nerClassifierPath = os.path.join(stanfordDir,'stanford-ner-{}'.format(stanfordVersion), 'classifiers/english.all.3class.distsim.crf.ser.gz')
nerJarPath = os.path.join(stanfordDir,'stanford-ner-{}'.format(stanfordVersion), 'stanford-ner.jar')
nerTagger = StanfordNERTagger(nerClassifierPath, nerJarPath)
postClassifierPath = os.path.join(stanfordDir, 'stanford-postagger-full-{}'.format(stanfordVersion), 'models/english-bidirectional-distsim.tagger')
postJarPath = os.path.join(stanfordDir,'stanford-postagger-full-{}'.format(stanfordVersion), 'stanford-postagger.jar')
postTagger = StanfordPOSTagger(postClassifierPath, postJarPath)
parserJarPath = os.path.join(stanfordDir, 'stanford-parser-full-{}'.format(stanfordVersion), 'stanford-parser.jar')
parserModelsPath = os.path.join(stanfordDir, 'stanford-parser-full-{}'.format(stanfordVersion), 'stanford-parser-{}-models.jar'.format(parserVersion))
modelPath = os.path.join(stanfordDir, 'stanford-parser-full-{}'.format(stanfordVersion), modelName)

SID = SentimentIntensityAnalyzer()
LINK_RE = re.compile(r"http\S*")


MOD_MES_RE = re.compile("\n_____\n\n.*")


def first_person_pronouns(word_tokens):
    """
    Retrieves first person pronouns (singular and plural) plus their fractions from content
    """
    tracking_dict = {"fps": 0,
                     "fpp": 0,
                     "fps_frac": 0,
                     "fpp_frac": 0,
                     "num_words": 0}
    fps = ["i", "me", "mine", "myself"]
    fpp = ["we", "our", "ours", "ourselves", "us"]
    index = 0
    for index, word in enumerate(word_tokens):
        if word in fps:
            tracking_dict["fps"] += 1
        elif word in fpp:
            tracking_dict["fpp"] += 1

    num_words = index + 1
    tracking_dict["fps_frac"] = tracking_dict["fps"] / num_words
    tracking_dict["fpp_frac"] = tracking_dict["fpp"] / num_words
    tracking_dict["num_words"] = num_words

    return pd.Series(tracking_dict)



def main(test=False):
    """
    Does transformations on cmv_auth_subs.pkl and cmv_subs.pkl
    """

    # Read in Data
    cmv_subs = pd.read_pickle("/home/jmcclellan/MACS30200proj/changemyview/cmv_subs.pkl")
    cmv_auth_subs = pd.read_pickle("/home/jmcclellan/MACS30200proj/changemyview/cmv_auth_subs.pkl")

    # CMV Subs Processing
    cmv_subs = cmv_subs[cmv_subs.title.str.contains(r"\[Podcast\]") == False]
    cmv_subs["content"] = cmv_subs.content.apply(
        lambda text: re.sub(MOD_MES_RE, "", text))
    print("Tokenizing CMV submission text")
    cmv_subs = cmv_subs.assign(**{stat: None for stat in 
                                      ["fps", "fpp", "fps_frac", "fpp_frac", "num_words"]})
    cmv_subs["tokenized_text"] = cmv_subs.content.apply(
        lambda text: word_tokenize(text))
    print("Retrieving First person pronouns")
    cmv_subs.loc[:, sorted(["fps", "fpp", "fps_frac", "fpp_frac", "num_words"])] = (
        cmv_subs.tokenized_text.apply(lambda w_tokens: first_person_pronouns(w_tokens)))
    cmv_subs["sentiment"] = cmv_subs.content.apply(lambda text:
                                                   SID.polarity_scores(text)["compound"])
    cmv_tf_vectorizer = TfidfVectorizer(max_df=0.5, max_features=1000, min_df=3, stop_words="english",
            norm="l2")
    cmv_tf_vects = cmv_tf_vectorizer.fit_transform(cmv_subs["content"])
    cmv_tf_km = KMeans(n_clusters=10, init="k-means++")
    cmv_tf_km.fit(cmv_tf_vects)
    cmv_subs["kmeans_topic"] = cmv_tf_km.labels_


    # CMV Author submissions processing
    cmv_auth_subs["content"] = cmv_auth_subs.content.apply(
        lambda text: re.sub(MOD_MES_RE, "", text))
    cmv_auth_subs = cmv_auth_subs[["href", "sub_id", "title", "author", "created_utc", "content",
                                   "score", "subreddit"]]
    cmv_auth_subs["content"] = cmv_auth_subs.content.apply(
        lambda text: re.sub(MOD_MES_RE, "", text))
    cmv_auth_subs["removed"] = cmv_auth_subs.content.str.contains(r"^\[removed\]$")
    cmv_auth_subs["empty"] = cmv_auth_subs.content.str.contains("^$")
    print("Tokenizing author submission text")
    cmv_auth_subs = cmv_auth_subs.assign(**{stat: None for stat in
                                            ["fps", "fpp", "fps_frac", "fpp_frac", "num_words"]})
    cmv_auth_subs["tokenized_text"] = cmv_auth_subs.content.apply(
        lambda text: word_tokenize(text))
    print("Retrieving First person pronouns")
    cmv_auth_subs.loc[:, sorted(["fps", "fpp", "fps_frac", "fpp_frac", "num_words"])] = (
        cmv_auth_subs.tokenized_text.apply(lambda w_tokens: first_person_pronouns(w_tokens)))
    if test:
        pdb.set_trace()
    cmv_auth_subs["sentiment"] = cmv_auth_subs.content.apply(lambda text:
                                                             SID.polarity_scores(text)["compound"])
    cmv_auth_subs["url_link"] = cmv_auth_subs.content.apply(lambda text:
                                                            len(re.findall(LINK_RE, text)))
    cmv_auth_subs["cmv_sub"] = cmv_auth_subs.subreddit.apply(
        lambda subred: True if subred == "r/changemyview" else False)

    if test:
        pdb.set_trace()

    cmv_subs = cmv_subs.apply(pd.to_numeric, errors="ignore")
    cmv_subs.drop("tokenized_text", axis=1, inplace=True)
    cmv_auth_subs = cmv_auth_subs.apply(pd.to_numeric, errors="ignore")
    cmv_auth_subs.drop("tokenized_text", axis=1, inplace=True)

    feather.write_dataframe(cmv_subs,
                            "/home/jmcclellan/MACS30200proj/changemyview/cmv_subs.feather")
    feather.write_dataframe(cmv_auth_subs,
                            "/home/jmcclellan/MACS30200proj/changemyview/cmv_auth_subs.feather")


    # For Dataviz project
    feather.write_dataframe(cmv_subs, "/home/jmcclellan/viz-shugamoe/cmv_subs.feather")
    feather.write_dataframe(cmv_auth_subs, "/home/jmcclellan/viz-shugamoe/cmv_auth_subs.feather")
    print("Feather files written")

    # Process Pickle files to R data
    print("Begin processing (in R) to make data suitable for modelling.")
    subprocess.check_call(["Rscript", 
                           "/home/jmcclellan/MACS30200proj/FinalPaper/process_data.R"], shell=False)
    print("R Data for logistic models created.")


if __name__ == "__main__":
    main()
