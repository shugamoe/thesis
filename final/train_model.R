# Running logistic regression with 2016 changemyview submissions

library(caret)
library(purrr)
library(plyr)
library(tidyverse)
library(pROC)
library(doMC)
library(modelr)
library(stringr)
library(tidytext)
library(perm)


source("master_vars.R")
###
# Tool Functions
###
read_data <- function(lsa_topics = CHOICE_LSA, lda_topics = CHOICE_LDA,
                      max_days_between = CHOICE_DB, tag = ""){
  require(glue)
  require(readr)
  data_fp <- glue("model_data/model_dat_db_{max_days_between}_ltv_{lsa_topics}_ldatv_{lda_topics}{tag}.rds")
  if (file.exists(data_fp)){
    return(read_rds(data_fp))
  } else {
    source("process_data.R")
    process_data(lsa_topics = lsa_topics, lda_topics = lda_topics,
                 max_days_between = max_days_between, tag = tag)
    return(read_data(lsa_topics, lda_topics, max_days_between, tag))
  }
}

revamp_cols <- function(model_dat, days_between = CHOICE_DB){
  require(dplyr)
  require(glue)
  require(lubridate)
  
  # Dynamic variable names
  subs_in_days_bw_name <- if (days_between >= 365){
    years <- days_between / 365
    glue("(AH) # Submissions within {years} years before 1st CMV Post")  
  } else {
    glue("(AH) # Submissions within {days_between} days before 1st CMV Post") 
  }
  rm_cmv_bw_name <- if (days_between >= 365){
    years <- days_between / 365
    glue("(AH) Removed CMV Subs {years} years before 1st CMV Post")  
  } else {
    glue("(AH) Removed CMV Subs {days_between} days before 1st CMV Post") 
  }
  avail_bw_name <- if (days_between >= 365){
    years <- days_between / 365
    glue("(AH) Submissions with Content {years} years before 1st CMV Post")  
  } else {
    glue("(AH) Submissions with Content {days_between} days before 1st CMV Post") 
  }
  
  add_topic_name <- function(name){
    library(stringr)
    library(glue)
    
    num <- str_extract(name, "[1234567890]+")
    
    new_name <- as.character(glue("(Pre Debate) Topic {num} Score"))
  }
  
  model_dat <- model_dat %>%
    mutate(gave_delta = factor(gave_delta, labels = c("Stable", "Changed")),
           hour = hour(as.POSIXct(first_cmv_date, origin = "1970-01-01"))
           ) %>%
    dplyr::mutate(
      `(Post Debate) # OP Comments` = first_cmv_op_coms,
      `(Post Debate) # Direct Comments` = first_cmv_direct_comments,
      `(Post Debate) # Total Comments` = first_cmv_total_coms,
      `(Post Debate) Opinion Change?` = gave_delta, # Dependent Var
      `(Pre Debate) CMV Submission Date`  = first_cmv_date, 
      `(Pre Debate) CMV Submission Hour` = hour,
      `(Pre Debate) Total Plural FP Pronouns`  = cmv_tot_first_person_plural, 
      `(Pre Debate) Fraction Plural FP Pronouns` = cmv_frac_first_person_plural ,
      `(Pre Debate) Total Singular FP Pronouns`  = cmv_tot_first_person_singular,
      `(Pre Debate) Fraction Singular FP Pronouns` = cmv_frac_first_person_singular,
      `(Pre Debate) # Words` = first_cmv_wc, # Base
      `(Pre Debate) Sentiment` = first_cmv_sent,
      `(Pre Debate) Similarity Score` = sim_scores,
      # `(Pre Debate) # Previous CMV Comments` = prev_cmv_coms,
      # `(Pre Debate) Mean Previous Deltas Received` = prev_cmv_avg_OP_deltas,
      # `(Pre Debate) Mean Previous CMV Comment Lag (Minutes)` = prev_cmv_com_avg_lag,
      !!subs_in_days_bw_name := prev_subs,
      `(AH) Edits Per Previous Submission` = prev_avg_edits,
      `(AH) # Unique Subreddits Posted In` = prev_unique_subs,
      `(AH) Mean Submission Score` = prev_avg_score,
      `(AH) Subreddit Gini Index` = ps_gini,
      !!rm_cmv_bw_name := rm_cmv_subs,
      `(AH) Mean Previous Submission Sentiment` = ps_avg_sent,
      `(AH) Mean Plural First Person Pronouns` = sub_tot_first_person_plural,
      `(AH) Fraction Plural First Person Pronouns` = sub_frac_first_person_plural,
      `(AH) Mean Singular First Person Pronouns` = sub_tot_first_person_singular,
      `(AH) Fraction Singular First Person Pronouns` = sub_frac_first_person_singular,
      !!avail_bw_name := ps_available,
      `(AH) Mean # of Words` = ps_avg_word_count,
      `(AH) Average Previous Submission Date` = ps_mean_date
    ) %>%
    dplyr::rename_at(dplyr::vars(starts_with("topic_")), add_topic_name) %>%
    dplyr::filter(is.na(is_mod.x)) # Removes moderator submissions
    model_dat <- model_dat[,order(colnames(model_dat))]
    (model_dat)
}



###
# Function for training model given certain data
###
train_model <- function(lsa_topics = CHOICE_LSA, lda_topics = CHOICE_LDA,
                         max_days_between = CHOICE_DB, num_folds = K, num_repeats = REPEATS,
                        tag = ""){
  require(tidyverse)
  require(readr)
  require(caret)
  require(glue)
  require(doMC)
  
  out_fp <- glue("model_results/db_{max_days_between}_ltv_{lsa_topics}_ldatv_{lda_topics}_k_{num_folds}_rep_{num_repeats}{tag}.rds")
  if (file.exists(out_fp)){
    print(as.character(glue("Skipping (exists) '{out_fp}'")))
    return()
  }
  
  model_dat <- revamp_cols(read_data(lsa_topics, lda_topics, max_days_between, tag = tag),
                           days_between = max_days_between)
  
  registerDoMC(3)
  
  # Train and Test Splits
  train_index <- createDataPartition(model_dat$first_cmv_deltas_from_OP, p = 1,
                                     list = FALSE,
                                     times = 1)
  model_dat.train <- model_dat[train_index, ]
  model_dat.test <- model_dat[-train_index, ]
  
  
  # Define formulas
  # Base
  all_names <- names(model_dat)
  dep_var <- "`(Post Debate) Opinion Change?`"
  form.base <- formula(glue("{dep_var} ~ `(Pre Debate) CMV Submission Date`"))
  
  # Past
  vars.past <- names(model_dat)[startsWith(names(model_dat), "(Pre Debate)") | 
                                  startsWith(names(model_dat), "(AH)")] %>%
    paste0("`", ., "`") %>%
    paste0(collapse = " + ")
  form.past <- formula(glue("{dep_var} ~ {vars.past}"))
  
  # Full
  vars.full <- names(model_dat)[!startsWith(names(model_dat), "(Post Debate) Opinion Change?") 
                                & startsWith(names(model_dat), "(")] %>%
    paste0("`", ., "`") %>%
    paste0(collapse = " + ")
  form.full <- formula(glue("`(Post Debate) Opinion Change?` ~ {vars.full}"))
  
  
  
  # Train first model version (base model)
  print("Training Base Model. . . ")
  set.seed(69)
  ctrl.base <- trainControl(method = "repeatedcv", number = num_folds, 
                            repeats = num_repeats,
                            summaryFunction = twoClassSummary,
                            returnResamp = "final",
                            classProbs  = TRUE, search = "random")
  model.base <- train(form.base,
                      data = model_dat.train,
                      method = "glm",
                      metric = "ROC",
                      preProcess = c("center", "scale"),
                      trControl = ctrl.base)
  
  # Train the past only model
  print("Training Past Information Model. . . ")
  set.seed(69)
  model.past <- train(form.past,
                      data = model_dat.train,
                      method = "glm",
                      metric = "ROC",
                      preProcess = c("center", "scale"),
                      trControl = ctrl.base)
  
  # Train full model
  print("Training Full Information Model. . .")
  set.seed(69)
  model.full <- train(form.full,
                      data = model_dat.train,
                      method = "glm",
                      metric = "ROC",
                      preProcess = c("center", "scale"),
                      trControl = ctrl.base)
  
  results_dat <- list(
    model.base = model.base,
    model.past = model.past,
    model.full = model.full
  )
  
  
  # Write results to summary file
  # sum_fp <- glue("model_results/roc_summary.rds")
  # # Gather all the information on folds and model types
  # new_tib <- as_tibble(resamples(results_dat)$values) %>%
  #   select(-Resample) %>%
  #   mutate(lsa_topics = lsa_topics,
  #          lda_topics = lda_topics,
  #          max_days_between = max_days_between,
  #          k = num_folds,
  #          num_repeats = num_repeats
  #          )
  # if (!file.exists(sum_fp)){
  #   out_tib <- new_tib
  # } else {
  #   out_tib <- read_rds(sum_fp) %>% # Combine current and new tib (info)
  #     bind_rows(new_tib)
  # }
  # write_rds(out_tib, sum_fp)
  
  
  # Write the raw results as well
  write_rds(results_dat, out_fp)
  results_dat
}



all_train_models <- function(lsa_topics = LSA_TOPICS, lda_topics = LDA_TOPICS,
                             max_days_between = DAYS_BETWEEN, tag = ""
                             ){
  require(purrr)
  source("master_vars.R")
  
  input_list <- as.list(expand.grid(lsa_topics = lsa_topics, lda_topics = lda_topics,
                      max_days_between = max_days_between, tag = tag, stringsAsFactors = F))
  input_list$num_folds <- rep(K, length(input_list[[1]]))
  input_list$num_repeats <- rep(REPEATS, length(input_list[[1]]))
  
  input_list %>%
    pwalk(train_model)
  
  # Find non-overlapping 95% bootstrap confidence intervals for combinations of
  # LSA topics, LDA topics, and days between
  
  # roc_ci <- read_rds("model_results/roc_summary.rds") %>%
  #   group_by(lsa_topics, lda_topics, max_days_between, k, num_repeats) %>%
  #   dplyr::summarise(
  #             base_bci = quantile(`model.base~ROC`, .025),
  #             base_tci = quantile(`model.base~ROC`, .975),
  #             past_bci = quantile(`model.past~ROC`, .025),
  #             past_tci = quantile(`model.past~ROC`, .975),
  #             full_bci = quantile(`model.full~ROC`, .025),
  #             full_tci = quantile(`model.full~ROC`, .975)
  #             )
  # write_rds(roc_ci, "model_results/roc_intervals.rds")
  # 
  # remove_ci_overlap <- function(){
  #   dub_roc_ci <- roc_ci %>%
  #     inner_join(ungroup(roc_ci) %>% select(matches("(ci$)|(^k$)|(num_repeats)")),
  #                by = c("k", "num_repeats"), suffix = c("", ".check")) %>%
  #     filter(
  #       # base_bci > base_tci.check,
  #       # past_bci > past_tci.check,
  #       full_bci >= full_tci.check
  #       )
  #   
  # } 
}
