# R File for processing of Reddit Data into format ready for modelling

# Function for preparing some text data (Reddit Submission content)
prep_fun = function(x) {
  x %>% 
    # make text lower case
    str_to_lower %>% 
    # remove non-alphanumeric symbols
    str_replace_all("[^[:alnum:]]", " ") %>% 
    # collapse multiple spaces
    str_replace_all("\\s+", " ")
}

# Function to retrieve the nth ordered thing
nth_date <- function(x, order, decrease){
  u <- unique(x)
  sort(u, decreasing = decrease)[order]
}

utc_days_to_ms <- function(days){
  days * 86400
}


# Function to calculate "subreddit" impurity
calc_subred_gini <- function(user, df, days_between){
  require(tidyverse)
  require(ineq)
  require(glue)
  require(readr)
  
  auth_info_fp <- glue("pre_model_data/auth_gini/{user}_db_{days_between}.RData")
  if (!file.exists(auth_info_fp)){
    df <- df %>%
      filter(author == user)
    subreddits <- df$subreddit
    
    if (length(subreddits) == 0){
      return(-1)
    }
    
    work_tibble <- tibble(id = 1:length(subreddits), subreddit = subreddits)
    sr <- subreddits
    
    sr_probs <- work_tibble %>%
      group_by(subreddit) %>%
      dplyr::summarise(prob = n() / length(sr))
    ineq <- ineq(sr_probs$prob, type = "Gini")
    write_rds(list(user = user, ineq = ineq), auth_info_fp)
  } else {
    user_info <- read_rds(auth_info_fp)
    ineq <- user_info$ineq
  }
  ineq 
}

###
# Main Function. Processes data with given parameters
###
process_data <- function(lsa_topics = 100,
                         lda_topics = 7,
                         max_days_between = 730,
                         dl_data = F, raw = F
                         ){
  require(tidyverse)
  require(topicmodels)
  require(magrittr)
  require(text2vec)
  require(tidytext)
  require(glue)
  require(ineq)
  require(purrr)
  require(sentimentr)
  require(lubridate)
  
  out_fp <- glue("model_data/model_dat_db_{max_days_between}_ltv_{lsa_topics}_ldatv_{lda_topics}.rds")
  if (file.exists(out_fp)){
    print(as.character(glue("Skipping (exists) '{out_fp}'")))
    return()
  }
  
  tryCatch(
    dbcon <- src_mysql("jmcclellanDB", 
                       host="mpcs53001.cs.uchicago.edu",
                       username="jmcclellan",
                       password="udaeTh5b"
      ),
    error = print("Didn't connect to DB")
    )
  
  if (dl_data){
    print("Downloading Data")
    cmv_coms <- dbcon %>%
      tbl("CMV_Comment") %>%
      collect()
    
    
    cmv_subs <- dbcon %>%
      tbl("CMV_Submission") %>%
      collect()
    
    
    std_subs <- dbcon %>%
      tbl("Submission") %>%
      collect() 
    
    cmv_auths <- dbcon %>%
      tbl("CMV_Sub_Author") %>%
      collect()
    
  } else {
    print("Loading from raw data")
    cmv_coms <- read_rds("pre_model_data/db_data/cmv_coms_raw.rds")
    cmv_subs <- read_rds("pre_model_data/db_data/cmv_subs_raw.rds")
    std_subs <- read_rds("pre_model_data/db_data/std_subs_raw.rds")
    cmv_auths <- read_rds("pre_model_data/db_data/cmv_auths_raw.rds")
  }
  
  # Want Raw data or do some basic filtering? 
  if (!raw){
    cmv_coms <- cmv_coms %>%
      mutate(date = as.POSIXct(date_utc, origin = "1970-01-01")) %>%
      filter(content != "[removed]",
             content != "[deleted]",
             year(date) == 2016 # Want 2016 Comments only
             )
    
    cmv_subs <- cmv_subs %>%
      mutate(gave_delta = ifelse(deltas_from_author > 0, 1, 0),
             date = as.POSIXct(date_utc, origin = "1970-01-01")) %>%
      filter(content != "[removed]",
             year(date) == 2016 # Want 2016 Submissions Only
             )
    temp <- seq(from = 1, by = 1, length.out = nrow(cmv_subs %>%
                                                      filter(content != "[removed]")))
    cmv_subs <- cmv_subs %>%
      mutate(cmv_sub_ind = 1,
             content = unlist(strsplit(content, "\n\n>"))[temp]
             ) # ^ Remove CMV Moderator Message
    t <- length(unique(cmv_subs$content))
    
    std_subs <- std_subs %>% 
      mutate(date = as.POSIXct(date_utc, origin = "1970-01-01")) %>%
      filter(content != "[removed]",
             content != "[deleted]",
             year(date) <= 2016 # Want Submissions in or prior to 2016
             )
    raw_add <- ""
  } else {
    raw_add <- "_raw"
  }

  # Write to disk for future reference
  cmv_auths %>% 
    write_rds(glue("pre_model_data/db_data/cmv_auths{raw_add}.rds"))
  cmv_coms %>%
    write_rds(glue("pre_model_data/db_data/cmv_coms{raw_add}.rds"))
  cmv_subs %>%
    write_rds(glue("pre_model_data/db_data/cmv_subs{raw_add}.rds"))
  std_subs %>%
    write_rds(glue("pre_model_data/db_data/std_subs{raw_add}.rds"))
  
  
  
  
  ###
  # Joins, Filtering and Summarizing
  ###
  # CMV Sub authors, their earliest CMV Submission, and the number of CMV Subs
  sub_auths_min_date <- cmv_subs %>%
    group_by(author) %>%
    dplyr::summarise(last_cmv_date = nth_date(date_utc, 1, T),
              first_cmv_date = nth_date(date_utc, 1, F),
              second_cmv_date = nth_date(date_utc, 2, F),
              tot_cmv_subs = n())
  
  
  
  ### 
  # Get a "perplexity" score from "Warp" algo LDA topic model
  ###
  warp_topic_model_fp <- glue("pre_model_data/topic_model/warp_k_{lda_topics}.RData")
  if (!file.exists(warp_topic_model_fp)){
    tokens <- cmv_subs$content %>% 
      tolower %>% 
      word_tokenizer
    it <- itoken(tokens, progressbar = FALSE)
    v <- create_vocabulary(it) %>% 
      prune_vocabulary(term_count_min = 10, doc_proportion_max = 0.2)
    vectorizer <- vocab_vectorizer(v)
    cmv_dtm <- create_dtm(it, vectorizer, type = "dgTMatrix")
    
    cmv_lda <- LDA$new(n_topics = lda_topics, doc_topic_prior = 0.1, topic_word_prior = 0.01)
    doc_topic_distr <- cmv_lda$fit_transform(x = cmv_dtm, n_iter = 1000, 
                              convergence_tol = 0.001, n_check_convergence = 25, 
                              progressbar = FALSE)
    
    # Calculate "perplexity". I.e. topic number heuristic (low better)
    perp <- perplexity(cmv_dtm, 
               topic_word_distribution = cmv_lda$topic_word_distribution,
               doc_topic_distribution = doc_topic_distr)
    
    model_and_perp <- list(lda_mod = cmv_lda, perp = perp)
    write_rds(model_and_perp, warp_topic_model_fp)
  } else {
    cmv_lda <- read_rds(warp_topic_model_fp)$lda_mod
  }
  
  # Now have a topic model for scoring
  topic_model_fp <- glue("pre_model_data/topic_model/k_{lda_topics}.RData")
  if (!file.exists(topic_model_fp)){
    cmv_subs_col <- cmv_subs
    
    by_cmv_sub_word <- cmv_subs_col %>%
      select(reddit_id, content) %>%
      unnest_tokens(word, content)
    
    word_counts <- by_cmv_sub_word %>%
      anti_join(stop_words) %>%
      count(reddit_id, word, sort = TRUE) %>%
      ungroup()
    
    cmv_dtm <- word_counts %>%
      cast_dtm(reddit_id, word, n)
    
    cmv_lda <- LDA(cmv_dtm, k = lda_topics, control = list(seed = 1234))
    write_rds(cmv_lda, topic_model_fp)
  } else {
    cmv_lda <- read_rds(topic_model_fp)
  }

  # For the k topics, we create a table with reddit_id of the CMV post as the primary key
  # And the "gamma" score given by the model giving the "probability" that
  # document belongs to a certain topic
  cmv_lda_doc_top <- tidy(cmv_lda, matrix = "gamma") %>%
    mutate(topic = paste("topic_", topic, sep = "")) %>%
    spread(topic, gamma) %>%
    dplyr::rename(reddit_id = document)
  
  # Filter down to first cmv_subs for each author only, extract sentiment and wc
  first_cmv_subs <- inner_join(sub_auths_min_date, cmv_subs) %>%
    filter(date_utc == first_cmv_date) %>%
    dplyr::rename(first_cmv_op_coms = author_comments, 
                  first_cmv_total_coms = total_comments,
                  first_cmv_direct_comments = direct_comments,
                  first_cmv_edited = edited
                  )
  first_cmv_subs <- inner_join(first_cmv_subs, cmv_lda_doc_top)
  
  sent_wc_info <- sentiment_by(get_sentences(first_cmv_subs$content))
  first_cmv_subs <- first_cmv_subs %>%
    bind_cols(sent_wc_info %>% dplyr::select(word_count, ave_sentiment) %>%
                dplyr::rename(cmv_word_count = word_count, cmv_avg_sent = ave_sentiment)
              )
  rm(sent_wc_info)
  
  # Join with cmv submissions, calc gini coef
  std_subs_info <- inner_join(std_subs %>% mutate(is_cmv_sub = ifelse(subreddit == "r/changemyview", 1, 0)), 
                              sub_auths_min_date) %>%
    filter(date_utc < first_cmv_date,
           # Filter for submissions only younger than . . . 
           abs(first_cmv_date - date_utc) < utc_days_to_ms(max_days_between)) %>%
    inner_join(first_cmv_subs %>% 
                 select(author, starts_with("topic_"),
                        deltas_from_author, cmv_word_count, cmv_avg_sent,
                        starts_with("first_cmv_"),
                        -first_cmv_date
                        ),
               by = "author"
               ) %>%
    filter(author != "[deleted]")
  
  # auth_gini_fp <- glue("pre_model_data/gini_key_db_{max_days_between}.RData")
  # if (!file.exists(auth_gini_fp)){
  auth_gini <- sub_auths_min_date$author %>%
    map_dbl(.f = calc_subred_gini, df = std_subs_info, days_between = max_days_between)
    # write_rds(auth_gini, auth_gini_fp)
  # } else {
   # auth_gini <- read_rds(auth_gini_fp)
  # }
  auth_gini <- tibble(author = sub_auths_min_date$author, sr_gini = auth_gini)
  
  std_subs_info <- std_subs_info %>%
    left_join(auth_gini, by = "author")
  # Get average sentiment and word count for each submission
  sent_wc_info <- sentiment_by(get_sentences(std_subs_info$content))
  std_subs_info <- std_subs_info %>% 
    bind_cols(sent_wc_info %>% dplyr::select(word_count, ave_sentiment)) %>%
    group_by(author)
  rm(sent_wc_info)

  # browser()
  std_subs_info <- inner_join(std_subs_info %>%
    dplyr::summarise(
      # Stats from non-CMV submissions before first CMV Submission  
      prev_subs = n(),
      prev_unique_subs = length(unique(subreddit)),
      prev_avg_score = mean(score),
      prev_avg_edits = mean(edited),
      prev_avg_dcoms = mean(direct_comments),
      prev_avg_tcoms = mean(total_comments),
      prev_avg_acoms = mean(author_comments),
      prev_avg_score = mean(score),
      sub_text = prep_fun(paste(content, collapse=" ")),
      ps_avg_word_count = mean(word_count),
      ps_avg_sent = mean(ave_sentiment),
      ps_gini = mean(sr_gini),
      ps_available = sum(word_count != 0),
      ps_mean_date = mean(date_utc),
      
      
      # Stats from first CMV Submission
      first_cmv_op_coms = mean(first_cmv_op_coms),
      first_cmv_date = mean(first_cmv_date),
      first_cmv_deltas_from_OP = mean(deltas_from_author),
      first_cmv_wc = mean(cmv_word_count),
      first_cmv_sent = mean(cmv_avg_sent),
      first_cmv_op_coms = mean(first_cmv_op_coms), 
      first_cmv_total_coms = mean(first_cmv_total_coms),
      first_cmv_direct_comments = mean(first_cmv_direct_comments),
      first_cmv_edited = mean(first_cmv_edited)
      ),
    std_subs_info %>%
      dplyr::summarise_at(vars(starts_with("topic_")), mean), 
    by = "author"
    ) %>%
    inner_join(first_cmv_subs %>% select(content, author), by = "author") %>%
    dplyr::rename(first_cmv_text = content) %>%
    mutate(all_text = paste(sub_text, prep_fun(first_cmv_text)))
    # ^ Add in CMV Text info now (had repeats depending on num subs b4)
  
  ###
  # Create similarity score between author's previous body of work and first
  # CMV Submission
  ###
  tfidf = LSA$new(n_topics = lsa_topics)
  
  # http://text2vec.org/similarity.html
  vectorizer <- prep_fun(std_subs_info$all_text) %>%
    itoken(., progressbar=F) %>%
    create_vocabulary() %>%
    prune_vocabulary(doc_proportion_max = 0.1, term_count_min = 5) %>%
    vocab_vectorizer()
  
  # Remove all_text, it's huge!
  std_subs_info <- std_subs_info %>%
    dplyr::select(-contains("all_text"))
  
  sub_dtm <- std_subs_info$sub_text %>% itoken(., progressbar=F) %>%
    create_dtm(vectorizer) %>%
    fit_transform(tfidf)
  
  first_cmv_dtm <- std_subs_info$first_cmv_text %>% itoken(., progressbar=F) %>%
    create_dtm(vectorizer) %>%
    fit_transform(tfidf)
  
  par_scores <- psim2(sub_dtm, first_cmv_dtm, method = "cosine", norm = "l2")
  std_subs_info <- std_subs_info %>%
    mutate(sim_scores = par_scores)
  rm(tfidf, vectorizer, sub_dtm, first_cmv_dtm, par_scores)
  
  ###
  # Retrieve first person pronoun ratios
  ###
  auth_sub_text_words <- std_subs_info %>%
    select(author, sub_text) %>%
    unnest_tokens(word, sub_text) %>%
    mutate(first_person_singular = ifelse(word %in% c("i", "me"), 1, 0),
           first_person_plural = ifelse(word %in% c("we", "us"), 1, 0))
  
  auth_sub_text_info <- auth_sub_text_words %>%
    group_by(author) %>%
    dplyr::summarise(sub_n_words = n(),
              sub_tot_first_person_singular = sum(first_person_singular),
              sub_tot_first_person_plural = sum(first_person_plural),
              sub_frac_first_person_singular = sub_tot_first_person_singular / sub_n_words,
              sub_frac_first_person_plural = sub_tot_first_person_plural / sub_n_words
              )
  
  auth_cmv_text_words <- std_subs_info %>%
    select(author, first_cmv_text) %>%
    unnest_tokens(word, first_cmv_text) %>%
    mutate(first_person_singular = ifelse(word %in% c("i", "me"), 1, 0),
           first_person_plural = ifelse(word %in% c("we", "us"), 1, 0))
  
  auth_cmv_text_info <- auth_cmv_text_words %>%
    group_by(author) %>%
    dplyr::summarise(cmv_n_words = n(),
              cmv_tot_first_person_singular = sum(first_person_singular),
              cmv_tot_first_person_plural = sum(first_person_plural),
              cmv_frac_first_person_singular = cmv_tot_first_person_singular / cmv_n_words,
              cmv_frac_first_person_plural = cmv_tot_first_person_plural / cmv_n_words
              )
  
  std_subs_info <- std_subs_info %>% inner_join(auth_sub_text_info) %>%
    inner_join(auth_cmv_text_info)
  
  # Attempt to retrieve previous CMV Comment info
  cmv_coms_info <- inner_join(cmv_coms, sub_auths_min_date) %>%
    # Don't count CMV comments posted on their own posts (Impossible to get a delta from yourself!)
    inner_join(cmv_subs %>% select(author, reddit_id, date_utc, direct_comments, content)
               %>% rename(sub_auth = author, sub_id = reddit_id, sub_date = date_utc), 
               by=c("parent_submission_id" = "sub_id")) %>%
    filter(sub_auth != author) %>%
    filter(date_utc < first_cmv_date) %>%
    # filter(!((author %in% subject_authors) & (sub_auth %in% subject_authors))) %>%
    mutate(cmv_com_ind = 1,
           direct_reply_ind = ifelse(is.na(parent_comment_id), 1, 0),
           
           # Create variable showing the lag time in minutes (originally ms) between
           # the CMV comment and the original post
           lag_time = abs((date_utc - sub_date) / (1000 * 60))
           ) %>% 
    group_by(author) %>%
    dplyr::summarise(
              prev_cmv_coms = sum(cmv_com_ind),
              prev_cmv_direct_coms = sum(direct_reply_ind),
              prev_cmv_avg_OP_deltas = mean(deltas_from_OP),
              prev_cmv_avg_other_deltas = mean(deltas_from_other),
              prev_cmv_sub_avg_direct_coms = mean(direct_comments),
              prev_cmv_com_avg_lag = mean(lag_time)
    )
  
  std_subs_info <- std_subs_info %>%
    left_join(cmv_coms_info) %>%
    dplyr::select(-contains("text")) %>%
    mutate(prev_cmv_participation = ifelse(prev_cmv_coms > 0, 1, 0))
  std_subs_info[is.na(std_subs_info)] <- 0 # Replace all NAs with 0
  # rm(list=setdiff(ls(), "std_subs_info"))
  # browser()
  
  
  # Return model ready info
  write_rds(std_subs_info, out_fp)
  std_subs_info
}

all_process_data <- function(){
  require(purrr)
  source("master_vars.R")
  
  as.list(expand.grid(lsa_topics = LSA_TOPICS, lda_topics = LDA_TOPICS,
                      max_days_between = DAYS_BETWEEN)) %>%
    pwalk(process_data)
}

###
# Post hoc, fixes
###

fix_dat <- function(){
  require(tidyverse)
  require(purrr)
  source("explore_data.R")
  
  
  model_data_files <- list.files("model_data/", pattern = "^model_dat_db")
  days_between <- unique(
    as.integer(str_extract(model_data_files, "[1234567890]+")))
  
  cmv_subs <- read_rds("pre_model_data/db_data/cmv_subs.rds")
  sub_auths_min_date <- cmv_subs %>%
    dplyr::group_by(author) %>%
    dplyr::summarise(last_cmv_date = nth_date(date_utc, 1, T),
              first_cmv_date = nth_date(date_utc, 1, F),
              second_cmv_date = nth_date(date_utc, 2, F),
              tot_cmv_subs = n())
  
  cmv_subs <- read_rds("pre_model_data/db_data/cmv_subs_raw.rds") %>%
    left_join(sub_auths_min_date, by = "author")
  
  add_rm_cmv_subs <- function(cmv_subs_raw, db){
    require(glue)
    rm_cmv_info <- cmv_subs_raw %>%
      dplyr::filter((first_cmv_date - date_utc) < db * 86400) %>%
      dplyr::mutate(rm = ifelse(content == "[removed]", 1, 0)) %>%
      dplyr::group_by(author) %>%
      dplyr::summarise(rm_cmv_subs = sum(rm))
    
    pat <- glue("model_dat_db_{db}")
    relevant_files <- list.files("model_data/", pattern = pat)
    
    for (f in relevant_files){
      file_path <- glue("model_data/{f}")
      read_rds(file_path) %>%
        inner_join(rm_cmv_info) %>%
        dplyr::mutate(gave_delta = ifelse(first_cmv_deltas_from_OP > 0, 1, 0)) %>%
        left_join(MOD_TIB, by = c("author" = "mod_name")) %>%
        write_rds(file_path)
    }
  } 
  
  days_between %>%
    walk(~ add_rm_cmv_subs(cmv_subs_raw = cmv_subs, db = .))
}

if (!interactive()) {
  all_process_data()
  # quickfixes
  fix_dat()
}