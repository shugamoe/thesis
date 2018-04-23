# Data Exploration for Thesis

source("process_data.R")
source("train_model.R")

MOD_TIB = list(c("Snorrrlax", "Wed Jan 16 23:34:39 2013"),
              c("protagornast", "Sat Jan 19 05:51:24 2013"),
              c("TryUsingScience", "Thu Mar 7 18:21:23 2013"),
              c("IAmAN00bie", "Thu Apr 11 22:04:50 2013"),
              c("cwenham", "Sun Aug 18 18:51:21 2013"),
              c("convoces", "Sun Oct 20 16:12:10 2013"),
              c("Nepene", "Sun Oct 20 16:39:29 2013"),
              c("Grunt08", "Thu Feb 20 03:34:58 2014"),
              c("hacksoncode", "Thu Feb 20 22:04:49 2014"),
              c("garnteller", "Fri Mar 28 14:48:56 2014"),
              c("GnosticGnome", "Fri Jun 27 00:57:18 2014"),
              c("mehatch", "Tue Jan 27 23:59:43 2015"),
              c("bubi09", "Fri Feb 6 13:34:21 2015"),
              c("huadpe", "Sat Mar 28 16:15:00 2015"),
              c("IIIBlackhartIII", "Tue Sep 8 04:14:05 2015"),
              c("RustyRook", "Wed Jan 27 03:32:49 2016"),
              c("AutoModerator", "Tue Feb 2 22:48:06 2016"),
              c("qtx", "Wed Mar 9 13:29:30 2016"),
              c("FlyingFoxOfTheYard_", "Mon Sep 26 18:10:33 2016"),
              c("etquod", "Mon Sep 26 20:00:42 2016"),
              c("e36", "Sun Apr 2 21:44:58 2017"),
              c("whitef530 ", "Sun Apr 2 22:29:06 2017"),
              c("Ansuz07", "Sat Jul 15 23:27:12 2017"),
              c("neofederalist", "Mon Sep 11 18:38:43 2017"),
              c("Evil_Thresh", "Tue Sep 12 04:55:40 2017"),
              c("kochirakyosuke", "Wed Sep 13 19:42:03 2017"),
              c("howbigis1gb", "Mon Sep 25 16:57:25 2017"),
              c("ColdNotion", "Sat Oct 14 15:46:35 2017"),
              c("DeltaBot", "Thu Jan 9 23:37:43 2014")
              )

fix_mod_tib <- function(){
  require(lubridate)
  require(tidyverse)
  require(purrr)
  
  d_form <- c("a b d H:M:S Y") 
  
  extract <- function(vec){
   tibble(mod_name = vec[1], modded_by = parse_date_time(vec[2], orders = d_form,
                                                     tz ="UTC"),
          is_mod = T) 
  }
  
  (final <- MOD_TIB %>%
    map_dfr(~ extract(.)))
}

MOD_TIB <- fix_mod_tib()

get_k_lda <- function(lda_topics = 7){
  require(readr)
  require(topicmodels)
  
  (cmv_lda <- read_rds(
    glue("pre_model_data/topic_model/warp_k_{lda_topics}.RData"))$lda_mod)
}

get_cmv_subs <- function(lsa_topics = 100, lda_topics = 7,
                         max_days_between = 730){
  require(tidyverse)
  require(readr)
  require(lubridate)
  require(topicmodels)
  require(glue)
  require(tidytext)
  
    
  cmv_subs <- read_rds("pre_model_data/db_data/cmv_subs_raw.rds") %>%
      mutate(posix = as.POSIXct(date_utc, origin = "1970-01-01"),
             date = as_date(posix),
             dtime = as_datetime(date),
             tod = hour(posix) + minute(posix) / 60,
             fill = 1,
             opinion_change = ifelse(deltas_from_author > 0, T, F)
             ) %>%
      filter(year(date) == 2016)
  
  sub_auths_min_date <- cmv_subs %>%
    group_by(author) %>%
    dplyr::summarise(last_cmv_date = nth_date(date_utc, 1, T),
              first_cmv_date = nth_date(date_utc, 1, F),
              tot_cmv_subs = n())
  
  cmv_subs <- cmv_subs %>%
    inner_join(sub_auths_min_date %>% select(author, first_cmv_date)) %>%
    dplyr::mutate(auth_first = ifelse(date_utc == first_cmv_date, T, F))
  
  
  # Join with model ready data for even more stats
  source("train_model.R")
  model_dat <- read_data(lsa_topics, lda_topics, max_days_between)
  
  cmv_subs <- cmv_subs %>%
    inner_join(model_dat %>% select(-first_cmv_date), by = "author")
  
  cmv_subs
}

explore_data <- function(lsa_topics = 100, lda_topics = 7,
                              max_days_between = 730){
  require(tidyverse)
  require(ggplot2)
  require(lubridate)
  require(scales)
  require(glue)
  
  out_fp <- glue("exploration_objects/db_{max_days_between}_ltv_{lsa_topics}_ldatv_{lda_topics}.rds") 
  if (file.exists(out_fp)){
    print(as.character(glue("Skipping (exists) '{out_fp}'")))
    return()
  }
  
  theme_set(theme_minimal())
  cmv_subs <- get_cmv_subs(lsa_topics, lda_topics, max_days_between)
  
  model_dat <- read_data(lsa_topics, lda_topics, max_days_between) %>%
    mutate(first_cmv_date = as.POSIXct(first_cmv_date, origin = "1970-01-01"),
           ps_mean_date = as.POSIXct(ps_mean_date, origin = "1970-01-01")
           ) %>%
    revamp_cols(max_days_between) %>%
    dplyr::select(starts_with("("))
    
  all <- list()
  plots <- list()
  tables <- list()
  
  stargazer_output_fp <- glue("figs/stargazer_tabs/db_{max_days_between}_ltv_{lsa_topics}_ldatv_{lda_topics}.tex")
  stargazer(as.data.frame(model_dat), 
            title = "Summary Statistics",
            header = FALSE, type = "latex", 
            out = stargazer_output_fp,
            summary.stat =  c("mean", "sd", "min", "max")) 
  
  # Extract top 10 words from given # of lda topics
  warp_k_lda <- get_k_lda(lda_topics)
  tables$top_words <- warp_k_lda$get_top_words()
  tables$stargazer_fp <- as.character(stargazer_output_fp)
  
  net_change <- cmv_subs %>%
    group_by(date) %>%
    dplyr::summarise(n = n(),
                     change = sum(opinion_change),
                     net_change = change - (n - change)
                     ) %>%
    mutate(negative)
  
  net_change_first <- cmv_subs %>%
    filter(auth_first == T) %>%
    group_by(date) %>%
    dplyr::summarise(n = n(),
                     change = sum(opinion_change),
                     net_change = change - (n - change)
                     )
  
  # Plot of Net Opinion Change Overall
  plots$all_net_change <- net_change %>%
    ggplot(aes(x = date, y = net_change)) +
      geom_line()
  
  # Plot of Net Opinion Change Overall
  plots$first_net_change <- net_change %>%
    ggplot(aes(x = date, y = net_change)) +
      geom_line()
  
  # Plot of all CMV Submission Activity over Time, by opinion change
  plots$all_activity <- cmv_subs %>%
    ggplot(aes(x = date, group = opinion_change, fill = opinion_change)) +
      geom_density(position = "stack", aes(y = ..count..), bw = 1)
  
  # Plot of all CMV Submission Activity, by whether it was author's first sub
  plots$first_activity <- cmv_subs %>%
    ggplot(aes(x = date, group = auth_first, fill = auth_first)) +
      geom_density(position = "stack", aes(y = ..count..), bw = 1)
  
  
  # Plot of Activity given time of day
  plots$day_activity <- cmv_subs %>%
    ggplot(aes(x = tod, group = opinion_change, fill = opinion_change)) +
      geom_density(position = "stack", aes(y = ..count..), bw = 30/60)
  
  first_only <- cmv_subs %>%
    filter(auth_first == T) %>%
    filter_at(vars(ends_with(".x")), any_vars(is.na(.)))
  
  # Previous Sub density
  plots$prev_subs_density <- first_only %>%
    ggplot(aes(x = prev_subs)) + 
      geom_density(aes(y = ..count..))
  
  
  
  # # Moderator activity
  # plots$mod_activity <- cmv_subs %>%
  #   ggplot(aes(x = date, group = is_mod, fill = is_mod)) + 
  #     geom_density(position = "stack", aes(y = ..count..), bw = 30/60)
  
  # # Gather topic numbers and scores
  # cmv_subs <- cmv_subs %>%
  #   gather(key = topic, value = topic_score, starts_with("topic_"))
  # 
  # Topic scores over time
  # plots$topic_activity <- cmv_subs %>%
  #   ggplot(aes(x = date, y = topic_score, group = topic, color = topic)) +
  #     geom_line()
  
  all$plots <- plots
  all$tables <- tables
  all
}

find_cmv_date_cutoffs <- function(){
  require(tidyverse)
  require(lubridate)
  
  cmv_subs <- get_cmv_subs() %>%
    group_by(date) %>%
    dplyr::summarise(posts = n(),
              fill = mean(fill))
  
  fdate <- as_date(min(cmv_subs$date))
  ldate <- as_date(max(cmv_subs$date))
  
  cutoff_key <- tibble(date = seq(fdate, ldate, by = "days"))
  
  cutoff_key <- cutoff_key %>%
    left_join(cmv_subs %>% select(date, fill, posts))
  
  cutoff_key
}

all_explore_data <- function(){
  require(purrr)
  source("master_vars.R")
  
  as.list(expand.grid(lsa_topics = LSA_TOPICS, lda_topics = LDA_TOPICS,
                      max_days_between = DAYS_BETWEEN)) %>%
    pwalk(explore_data)
}
 
if (interactive()){
  cmv_subs <- get_cmv_subs()
  plots <- explore_data()
  k_lda <- get_k_lda()
  
  coff_key <- find_cmv_date_cutoffs()
}