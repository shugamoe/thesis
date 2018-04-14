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


###
# Main Function
###
process_data <- function(dl_data = F, raw = F, lsa_topics = 100,
                         lda_topics = 7,
                         max_days_between = Inf
                         ){
  require(tidyverse)
  require(topicmodels)
  require(magrittr)
  require(text2vec)
  require(tidytext)
  require(glue)
  require(ineq)
  require(purrr)
  
  DL_DATA <- dl_data # Load data from MySQL or from disk?
  dbcon <- src_mysql("jmcclellanDB", 
                     host="mpcs53001.cs.uchicago.edu",
                     username="jmcclellan",
                     password="uderpTh5b"
  )
  if (DL_DATA){
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
    cmv_coms <- read_rds("pre_model_data/cmv_coms_raw.rds")
    cmv_subs <- read_rds("pre_model_data/cmv_subs_raw.rds")
    std_subs <- read_rds("pre_model_data/std_subs_raw.rds")
    cmv_auths <- read_rds("pre_model_data/cmv_auths_raw.rds")
  }
  
  # Want Raw data or do some basic filtering? 
  if (!raw){
    cmv_coms <- cmv_coms %>%
      filter(content != "[removed]",
             content != "[deleted]")
    
    cmv_subs <- cmv_subs %>%
      mutate(gave_delta = ifelse(deltas_from_author > 0, 1, 0)) %>%
      filter(content != "[removed]") %>%
      mutate(cmv_sub_ind = 1)
    
    std_subs <- std_subs %>% 
    filter(content != "[removed]",
           content != "[deleted]")
    raw_add <- "_raw"
  } else {
    raw_add <- ""
  }

  # Write to disk for future reference
  print("Writing dat")
  cmv_auths %>% 
    write_rds(glue("pre_model_data/cmv_auths{raw_add}.rds"))
  cmv_coms %>%
    write_rds(glue("pre_model_data/cmv_coms{raw_add}.rds"))
  cmv_subs %>%
    write_rds(glue("pre_model_data/cmv_subs{raw_add}.rds"))
  std_subs %>%
    write_rds(glue("pre_model_data/std_subs{raw_add}.rds"))
  
  
  ###
  # Joins, Filtering and Summarizing
  ###
  # CMV Sub authors, their earliest CMV Submission, and the number of CMV Subs
  sub_auths_min_date <- cmv_subs %>%
    group_by(author) %>%
    summarise(last_cmv_date = nth_date(date_utc, 1, T),
              first_cmv_date = nth_date(date_utc, 1, F),
              tot_cmv_subs = n())
  
  
  
  ### 
  # Score CMV Submissions with LDA Topic Model
  ###
  topic_model_fp <- glue("pre_model_data/topic_model_k_{lda_topics}.RData")
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
    rename(reddit_id = document)
  
  # Join with cmv submissions
  first_cmv_subs <- inner_join(sub_auths_min_date, cmv_subs) %>%
    filter(date_utc == first_cmv_date)
  
  first_cmv_subs <- inner_join(first_cmv_subs, cmv_lda_doc_top)
  
  std_subs_info <- inner_join(std_subs %>% mutate(is_cmv_sub = ifelse(subreddit == "r/changemyview", 1, 0)), 
                              sub_auths_min_date) %>%
    filter(date_utc < first_cmv_date,
           # Filter for submissions only younger than . . . 
           abs(first_cmv_date - date_utc) < utc_days_to_ms(max_days_between)) %>%
    inner_join(by = "author", first_cmv_subs %>% select(author, content, starts_with("topic_"))) %>%
    filter(author != "[deleted]") %>%
    group_by(author)
  
  std_subs_info <- inner_join(std_subs_info %>%
    summarise(
      # Stats from non-CMV submission before first CMV Submission  
      prev_subs = n(),
      prev_unique_subs = length(unique(subreddit)),
      prev_avg_score = mean(score),
      prev_avg_edits = mean(edited),
      prev_avg_dcoms = mean(direct_comments),
      prev_avg_tcoms = mean(total_comments),
      prev_avg_acoms = mean(author_comments),
      prev_avg_score = mean(score),
      all_text = prep_fun(paste(content.x, content.y, collapse=" ")),
      sub_text = prep_fun(paste(content.x, collapse=" ")),
      first_cmv_text = prep_fun(paste(content.y, collapse = " ")),
      first_cmv_date = mean(first_cmv_date)
    ),
    std_subs_info %>%
      summarise_at(vars(starts_with("topic_")), mean), 
    by = "author"
    )
  
  # browser()
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
    summarise(sub_n_words = n(),
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
  
  auth_cmv_text_info <- auth_sub_text_words %>%
    group_by(author) %>%
    summarise(cmv_n_words = n(),
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
    summarise(
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
  std_subs_info
}

create_multiple_model_data <- function(lsa_topic_vals, days_between_vals,
                                       lda_topic_vals){
  require(readr)
  require(glue)
  for (ltv in lsa_topic_vals){
    for (dbv in days_between_vals){
      for (ldatv in lda_topic_vals){
        file_path <- glue("model_data/model_dat_db_{dbv}_ltv_{ltv}_ldatv_{ldatv}.rds")
        
        # LSA makes it take forever, don't remake a file if it's already there.
        if (!file.exists(file_path)){
          process_data(lsa_topics = ltv, max_days_between = dbv,
                       lda_topics = ldatv) %>%
            write_rds(file_path)
        }
      }
    }
  }
}

create_multiple_model_data(c(50, 100, 150, 200), c(365, 60, 30, 90, 730), c(4, 5, 6, 7, 8, 9, 10))