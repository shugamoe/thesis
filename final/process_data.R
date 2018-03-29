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

process_data <- function(dl_data = F, raw = F){
  require(tidyverse)
  require(magrittr)
  require(text2vec)
  require(glue)
  require(ineq)
  
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
    cmv_coms <- read_rds("cmv_coms_raw.rds")
    cmv_subs <- read_rds("cmv_subs_raw.rds")
    std_subs <- read_rds("std_subs_raw.rds")
    cmv_auths <- read_rds("cmv_auths_raw.rds")
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
    write_rds(glue("cmv_auths{raw_add}.rds"))
  cmv_coms %>%
    write_rds(glue("cmv_coms{raw_add}.rds"))
  cmv_subs %>%
    write_rds(glue("cmv_subs{raw_add}.rds"))
  std_subs %>%
    write_rds(glue("std_subs{raw_add}.rds"))
  
  
  ###
  # Joins, Filtering and Summarizing
  ###
  # CMV Sub authors, their earliest CMV Submission, and the number of CMV Subs
  sub_auths_min_date <- cmv_subs %>%
    group_by(author) %>%
    summarise(last_cmv_date = nth_date(date_utc, 1, T),
              first_cmv_date = nth_date(date_utc, 1, F),
              tot_cmv_subs = n())
  
  first_cmv_subs <- inner_join(sub_auths_min_date, cmv_subs) %>%
    filter(date_utc == first_cmv_date)
  
  std_subs_info <- inner_join(std_subs %>% mutate(is_cmv_sub = ifelse(subreddit == "r/changemyview", 1, 0)), 
                              sub_auths_min_date) %>%
    filter(date_utc < first_cmv_date) %>%
    inner_join(by = "author", first_cmv_subs %>% select(author, content)) %>%
    filter(author != "[deleted]") %>%
    group_by(author) %>%
    summarise(
      # Stats from non-CMV submission before first CMV Submission  
      prev_subs = n(),
      cmv_sub_prop = sum(is_cmv_sub) / prev_subs, 
      prev_unique_subs = length(unique(subreddit)),
      prev_avg_score = mean(score),
      prev_avg_edits = mean(edited),
      prev_avg_dcoms = mean(direct_comments),
      prev_avg_tcoms = mean(total_comments),
      prev_avg_acoms = mean(author_comments),
      prev_avg_unique_parts = mean(unique_commentors),
      prev_avg_score = mean(score),
      all_text = prep_fun(paste(content.x, content.y, collapse=" ")),
      sub_text = prep_fun(paste(content.x, collapse=" ")),
      first_cmv_text = prep_fun(paste(content.y, collapse = " "))
    )
  
  
}