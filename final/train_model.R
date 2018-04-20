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


###
# Tool Functions
###
read_data <- function(lsa_topics = 100, lda_topics = 7,
                      max_days_between = 730){
  require(glue)
  require(readr)
  data_fp <- glue("model_data/model_dat_db_{max_days_between}_ltv_{lsa_topics}_ldatv_{lda_topics}.rds")
  if (file.exists(data_fp)){
    return(read_rds(data_fp))
  } else {
    source("process_data.R")
    process_data(lsa_topics = lsa_topics, lda_topics = lda_topics,
                 max_days_between = max_days_between)
    return(read_data(lsa_topics, lda_topics, max_days_between))
  }
}

revamp_cols <- function(model_dat, days_between = 730){
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
  
  model_dat %>%
    mutate(gave_delta = factor(gave_delta, labels = c("Stable", "Changed"))) %>%
    dplyr::rename(
      `(Post Debate) # OP Comments` = first_cmv_op_coms,
      `(Post Debate) # Direct Comments` = first_cmv_direct_comments,
      `(Post Debate) # Total Comments` = first_cmv_total_coms,
      `(Post Debate) Opinion Change?` = gave_delta, # Dependent Var
      `(Pre Debate) CMV Submission Date`  = first_cmv_date, 
      `(Pre Debate) Total Plural FP Pronouns`  = cmv_tot_first_person_plural, 
      `(Pre Debate) Fraction Plural FP Pronouns` = cmv_frac_first_person_plural ,
      `(Pre Debate) Total Singular FP Pronouns`  = cmv_tot_first_person_singular,
      `(Pre Debate) Fraction Singular FP Pronouns` = cmv_frac_first_person_singular,
      `(Pre Debate) # Words` = first_cmv_wc, # Base
      `(Pre Debate) Sentiment` = first_cmv_sent,
      `(Pre Debate) Similarity Score` = sim_scores,
      `(Pre Debate) # Previous CMV Comments` = prev_cmv_coms,
      `(Pre Debate) Mean Previous Deltas Received` = prev_cmv_avg_OP_deltas,
      `(Pre Debate) Mean Previous CMV Comment Lag (Minutes)` = prev_cmv_com_avg_lag,
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
    dplyr::rename_at(dplyr::vars(starts_with("topic_")), add_topic_name)
}



###
# Function for training model given certain data
###
train_models <- function(lsa_topics = 100, lda_topics = 7,
                         max_days_between = 730, num_folds = 5, num_repeats = 1){
  require(tidyverse)
  require(readr)
  require(caret)
  require(glue)
  require(doMC)
  
  model_dat <- revamp_cols(read_data(lsa_topics, lda_topics, max_days_between),
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
  
  print("Writing results to model_results/")
  write_rds(results_dat, glue("model_results/results_db_{max_days_between}_ltv_{lsa_topics}_ldatv{lda_topics}_k_{num_folds}_rep_{num_repeats}.rds"))
  results_dat
}

###
# Older stuff (Spring 2017)
###

theme_set(theme_minimal())

# Data preProcessing
ALL_DAT <- readRDS("MACS30200proj/FinalPaper/cmv_processed_dat.rds") %>%
  filter(!str_detect(title, "Fresh Topic Friday")) 
CMV_DAT <- readRDS("MACS30200proj/FinalPaper/cmv_processed_dat.rds") %>%
  distinct(author, .keep_all = TRUE) %>% # One author only
  filter(!str_detect(title, "Fresh Topic Friday")) %>% # Take out meta posts
  # filter(num_OP_comments >= 1 & num_user_comments >= 10) %>% # Filtering procedure of Tan et al.
  mutate(OP_gave_delta = factor(OP_gave_delta, labels = c("stable", "changed")),
         created_utc = as.numeric(created_utc)) %>%
  dplyr::select(-c(author, num_deltas_from_OP, num_user_comments, href, date,
                   has_priors, id, content, num_OP_comments, num_root_comments,
                   title, mean_dl_subs)) %>% # No variation in deleted submissions
  # Last Submission statistics removal
  dplyr::select(-c(ls_score, ls_fps, ls_created_utc, ls_fpp_frac, ls_removed, time_since_ls,
                   ls_fps_frac, ls_cmv_sub, ls_fpp, ls_sentiment, ls_empty, ls_fpp)) %>%
  # dplyr::select(-c(created_utc, ls_created_utc)) %>% # time removal
  dplyr::select(-starts_with("sd_"), -starts_with("max_"), -starts_with("min_")) %>%
  drop_na() %>%
  mutate(created_utc = created_utc / 86400) %>%
  dplyr::rename(`(O) Creation Time` = created_utc,
                `(O) Opinion Change?` = OP_gave_delta,
                `(O) # Words` = num_words,
                `(O) Topic #` = kmeans_topic,
                `(O) Sentiment` = sentiment,
                `(O) Singular First Person Pronouns` = fps,
                `(O) Fraction Singular First Person Pronouns` = fps_frac,
                `(O) Plural First Person Pronouns` = fpp,
                `(O) Fraction Plural First Person Pronouns` = fpp_frac,
                `(AH) # All Prior Submissions` = num_prior_subs,
                `(AH) # Submissions with Available Content` = num_valid_prior_subs,
                `(AH) Daily Submission Frequency` = mean_daily_sub_freq,
                `(AH) Mean Number of Words` = mean_num_words,
                `(AH) Mean Submission Score` = mean_sub_score,
                `(AH) Subreddit Gini Index` = gini_index,
                `(AH) Fraction Empty Submissions` = mean_empty_subs,
                `(AH) Fraction Removed Submissions` = mean_rm_subs,
                `(AH) Mean Submission Sentiment` = mean_sub_sentiment,
                `(AH) Number of CMV Submissions` = num_cmv_subs,
                `(AH) Fraction of CMV Submissions` = frac_cmv_subs,
                `(AH) Mean Plural First Person Pronouns` = mean_fpp,
                `(AH) Fraction Plural First Person Pronouns` = mean_fpp_frac,
                `(AH) Mean Singular First Person Pronouns` = mean_fps,
                `(AH) Fraction Singular First Person Pronouns` = mean_fps_frac
  ) %>%
  as.data.frame()
# for (i in 1:length(CMV_DAT)){
#   print(i)
#   CMV_DAT[[i]] <- as.data.frame(CMV_DAT[[i]] %>%
#     select(-c(author, num_deltas_from_OP, num_user_comments, href, date,
#               has_priors, id, content, num_OP_comments, num_root_comments,
#               title, sentiment))
#   )
# }


CMV_DAT_POST <- readRDS("MACS30200proj/FinalPaper/cmv_processed_dat.rds") %>%
  distinct(author, .keep_all = TRUE) %>% # One author only
  filter(!str_detect(title, "Fresh Topic Friday")) %>% # Take out meta posts
  # filter(num_OP_comments >= 1 & num_user_comments >= 10) %>% # Filtering procedure of Tan et al.
  mutate(OP_gave_delta = factor(OP_gave_delta, labels = c("stable", "changed")),
         created_utc = as.numeric(created_utc)) %>%
  dplyr::select(-c(author, num_deltas_from_OP, href, date,
                   has_priors, id, content,
                   title, mean_dl_subs)) %>% # No variation in deleted submissions
  # Last Submission statistics removal
  dplyr::select(-c(ls_score, ls_fps, ls_created_utc, ls_fpp_frac, ls_removed, time_since_ls,
                   ls_fps_frac, ls_cmv_sub, ls_fpp, ls_sentiment, ls_empty, ls_fpp)) %>%
  # dplyr::select(-c(created_utc, ls_created_utc)) %>% # time removal
  dplyr::select(-starts_with("sd_"), -starts_with("max_"), -starts_with("min_")) %>%
  drop_na() %>%
  mutate(created_utc = created_utc / 86400) %>%
  dplyr::rename(`(O) Creation Time` = created_utc,
                `(O) Opinion Change?` = OP_gave_delta,
                `(O) # Words` = num_words,
                `(O) Topic #` = kmeans_topic,
                `(O) Sentiment` = sentiment,
                `(O) Singular First Person Pronouns` = fps,
                `(O) Fraction Singular First Person Pronouns` = fps_frac,
                `(O) Plural First Person Pronouns` = fpp,
                `(O) Fraction Plural First Person Pronouns` = fpp_frac,
                `(Post) # OP Comments` = num_OP_comments,
                `(Post) # Direct Comments` = num_root_comments,
                `(Post) # Total Comments` = num_user_comments,
                `(AH) # All Prior Submissions` = num_prior_subs,
                `(AH) # Submissions with Available Content` = num_valid_prior_subs,
                `(AH) Daily Submission Frequency` = mean_daily_sub_freq,
                `(AH) Mean Number of Words` = mean_num_words,
                `(AH) Mean Submission Score` = mean_sub_score,
                `(AH) Subreddit Gini Index` = gini_index,
                `(AH) Fraction Empty Submissions` = mean_empty_subs,
                `(AH) Fraction Removed Submissions` = mean_rm_subs,
                `(AH) Mean Submission Sentiment` = mean_sub_sentiment,
                `(AH) Number of CMV Submissions` = num_cmv_subs,
                `(AH) Fraction of CMV Submissions` = frac_cmv_subs,
                `(AH) Mean Plural First Person Pronouns` = mean_fpp,
                `(AH) Fraction Plural First Person Pronouns` = mean_fpp_frac,
                `(AH) Mean Singular First Person Pronouns` = mean_fps,
                `(AH) Fraction Singular First Person Pronouns` = mean_fps_frac
  ) %>%
  as.data.frame()

saveRDS(CMV_DAT_POST, "~/MACS30200proj/Poster/final_data.rds")
saveRDS(CMV_DAT_POST, "~/MACS30200proj/FinalPaper/final_data.rds")

num_folds <- 10
num_repeats <- 20

train_index <- createDataPartition(CMV_DAT$`(Post Debate) Opinion Change?`, p = 1,
                                   list = FALSE,
                                   times = 1)
CMV_DAT_TRAIN <- CMV_DAT[train_index, ]
CMV_DAT_TEST <- CMV_DAT[-train_index, ]

POST_TRAIN <- CMV_DAT_POST[train_index, ]
POST_TEST <- CMV_DAT_POST[-train_index, ]

train_x <- CMV_DAT_TRAIN[names(CMV_DAT) != "OP_gave_delta"]
train_y <- CMV_DAT_TRAIN$OP_gave_delta


Grid1 <- expand.grid(list(nIter = seq(50, 300, 10)))

ctrl1 <- trainControl(method = "repeatedcv", number = num_folds, repeats = num_repeats,
                      summaryFunction = twoClassSummary,
                      returnResamp = "all",
                      classProbs  = TRUE, search = "random")


set.seed(69)
model1 <- train(`(Post Debate) Opinion Change?`~ .,
                data = CMV_DAT_TRAIN,
                method = "LogitBoost",
                preProcess = c("center", "scale"),
                trControl = ctrl1,
                tuneGrid = Grid1,
                metric = "ROC")

# gbmGrid <-  expand.grid(interaction.depth = c(1, 5, 9), 
#                         n.trees = (1:30) * 50, 
#                         shrinkage = 0.1,
#                         n.minobsinnode = 20)

model_gbm <- train(`(Post Debate) Opinion Change?` ~ .,
                   data = CMV_DAT_TRAIN,
                   method = "gbm",
                   preProcess = c("center", "scale"),
                   trControl = ctrl1)
# tuneGrid = gbmGrid)

ctrl2 <- trainControl(method = "repeatedcv", number = num_folds, repeats = num_repeats,
                      summaryFunction = twoClassSummary,
                      returnResamp = "all",
                      classProbs  = TRUE, search = "random")

set.seed(69)
# model2_no_int <- train(`(O) Opinion Change?` ~ .,
#                 data = CMV_DAT_TRAIN,
#                 method = "glm",
#                 metric = "ROC",
#                 preProcess = c("center", "scale"),
#                 trControl = ctrl2)



# Grid3 <- expand.grid(mtry = seq(10, 300, 10))
# ctrl3 <- trainControl(method = "repeatedcv", number = 5, repeats = 5,
#                       summaryFunction = twoClassSummary,
#                       classProbs = TRUE, search = "random")
# 
# model3 <- train(train_x, train_y,
#                 method = "rf",
#                 metric = "ROC",
#                 tuneGrid = Grid3, 
#                 trControl = ctrl3)

Grid4 <- expand.grid(list(cp = "aic", lambda = seq(.0001, .1, .001)))

model_auc <- function(model, test_dat = CMV_DAT_TEST){
  dat_with_preds <- test_dat %>%
    add_predictions(model) %>%
    mutate(`(O) Opinion Change?` = ifelse(`(O) Opinion Change?`== "stable", 0, 1),
           pred = ifelse(pred == "stable", 0, 1))
  
  list(dat = dat_with_preds, auc = auc(dat_with_preds$`(O) Opinion Change?`,
                                       dat_with_preds$pred))
}

graph_roc <- function(model, test_dat = CMV_DAT_TEST, title = FALSE){
  graph_dat <- model_auc(model, test_dat)  
  
  if (title){
    main_lab = title
  } else{
    main_lab = sprintf("%s | AUC = %.3f", model$method, graph_dat[["auc"]])
  }
  plot(roc(graph_dat[["dat"]]$`(O) Opinion Change?`, graph_dat[["dat"]]$pred),
       xlim = c(1, 0),
       main = main_lab)
}
set.seed(69)
model2 <- train(`(O) Opinion Change?` ~ . + `(AH) # All Prior Submissions`:`(AH) Mean Submission Score` 
                + `(AH) # All Prior Submissions`:`(AH) Fraction Removed Submissions` + 
                  `(AH) # All Prior Submissions`:`(AH) Mean Singular First Person Pronouns` + 
                  `(AH) # All Prior Submissions`:`(AH) Mean Plural First Person Pronouns`,
                data = CMV_DAT_TRAIN,
                method = "glm",
                metric = "ROC",
                preProcess = c("center", "scale"),
                trControl = ctrl2)

set.seed(69)
model2base <- train(`(O) Opinion Change?` ~ 1 + `(O) # Words`,
                    data = CMV_DAT_TRAIN,
                    method = "glm",
                    metric = "ROC",
                    preProcess = c("center", "scale"),
                    trControl = ctrl2)

set.seed(69)
model_post <- train(`(O) Opinion Change?` ~ . + `(AH) # All Prior Submissions`:`(AH) Mean Submission Score` 
                    + `(AH) # All Prior Submissions`:`(AH) Fraction Removed Submissions` + 
                      `(AH) # All Prior Submissions`:`(AH) Mean Singular First Person Pronouns` +  
                      `(AH) # All Prior Submissions`:`(AH) Mean Plural First Person Pronouns`,
                    data = POST_TRAIN,
                    method = "glm",
                    metric = "ROC",
                    preProcess = c("center", "scale"),
                    trControl = ctrl2)

set.seed(69)
model_tonly <- train(`(O) Opinion Change?` ~ `(O) Creation Time`,
                     data = CMV_DAT_TRAIN,
                     method = "glm",
                     metric = "ROC",
                     preProcess = c("center", "scale"),
                     trControl = ctrl2)

set.seed(69)
model2_no_time <- train(`(O) Opinion Change?` ~ . + `(AH) # All Prior Submissions`:`(AH) Mean Submission Score` 
                        + `(AH) # All Prior Submissions`:`(AH) Fraction Removed Submissions` + 
                          `(AH) # All Prior Submissions`:`(AH) Mean Singular First Person Pronouns` + 
                          `(AH) # All Prior Submissions`:`(AH) Mean Plural First Person Pronouns`,
                        data = CMV_DAT_TRAIN %>% select(-`(O) Creation Time`),
                        method = "glm",
                        metric = "ROC",
                        preProcess = c("center", "scale"),
                        trControl = ctrl2)

# glm_dat <- auc_conf_int("glm")
# blr_dat <- auc_conf_int("LogitBoost")

# graph_roc(model1)
results_dat <- list(
  model2 = model2,
  # model2_no_int = model2_no_int,
  model2_no_time = model2_no_time,
  model2base = model2base,
  model_post = model_post,
  model_tonly = model_tonly
)

# Resample ROC results can be found in model$resample

saveRDS(results_dat, "~/MACS30200proj/FinalPaper/results.rds")

tibble(x = 1:(num_repeats * num_folds), y = model2$resample$ROC) %>%
  ggplot(aes(x, y)) +
  geom_line() +
  geom_smooth()

# Trash
# ================================================================================
# ================================================================================
# ================================================================================
# ================================================================================

# graph_roc(model2)
# graph_roc(model2base)
# graph_roc(model2_no_int)
# graph_roc(model_tonly)
# graph_roc(model_post, POST_TEST)
# graph_roc(model3)

# delta_accuracy <- CMV_DAT_TEST %>%
#   add_predictions(model) %>%
#   mutate(OP_gave_delta = ifelse(OP_gave_delta == "stable", 0, 1),
#          pred = ifelse(pred == "stable", 0, 1))
# 
# auc_delta <- auc(delta_accuracy$OP_gave_delta, delta_accuracy$pred)
# 
# plot(roc(delta_accuracy$OP_gave_delta, delta_accuracy$pred), xlim = c(1, 0),
#      main = sprintf("ROC Curve for Initial Logistic Model | AUC = %.3f", auc_delta))

auc_conf_int <- function(method, ints = 1:100){
  models_n_auc <- list(models = list(),
                       auc = c())
  for (num in ints){
    set.seed(69)
    iter_train_index <- createDataPartition(CMV_DAT$OP_gave_delta, p = .8, 
                                            list = FALSE, 
                                            times = 1)
    iter_train <- CMV_DAT[iter_train_index, ]
    iter_test <- CMV_DAT[-iter_train_index, ]
    
    iter_ctrl <- trainControl(method = "repeatedcv", number = 5, repeats = 5,
                              summaryFunction = twoClassSummary,
                              classProbs  = TRUE, search = "random")
    
    if (method == "LogitBoost"){
      iter_grid <- expand.grid(list(nIter = seq(50, 300, 10)))
      
      
      iter_model <- train(OP_gave_delta ~ .,
                          data = iter_train,
                          method = "LogitBoost",
                          trControl = iter_ctrl,
                          tuneGrid = iter_grid,
                          metric = "ROC")
    } else if (method == "glm"){
      iter_model <- train(OP_gave_delta ~ .,
                          data = iter_train, 
                          method = "glm",
                          trControl = iter_ctrl,
                          metric = "ROC")   
    }
    iter_test <- iter_test %>%
      add_predictions(iter_model) %>%
      mutate(OP_gave_delta = ifelse(OP_gave_delta == "stable", 0, 1),
             pred = ifelse(pred == "stable", 0, 1))
    models_n_auc[["auc"]] <- append(models_n_auc[["auc"]], 
                                    auc(iter_test$OP_gave_delta, iter_test$pred))
    models_n_auc[["models"]] <- append(models_n_auc[["models"]],
                                       iter_model)
  }
  models_n_auc
}