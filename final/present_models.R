#count Analyze the results of the trained models
#
#

library(tidyverse)
library(ggplot2)
library(caret)
library(coefplot)
source("master_vars.R")

read_models <- function(lsa_topics = CHOICE_LSA, lda_topics = CHOICE_LDA, max_days_between = CHOICE_DB,
                        num_folds = K, num_repeats = REPEATS, tag = ""
                        ){
  require(readr)
  require(glue)
  models_fp <- glue("model_results/db_{max_days_between}_ltv_{lsa_topics}_ldatv_{lda_topics}_k_{num_folds}_rep_{num_repeats}{tag}.rds")
  
  (read_rds(models_fp)) 
}

present_models <- function(lsa_topics = CHOICE_LSA, lda_topics = CHOICE_LDA, max_days_between = CHOICE_DB,
                           num_folds = K, num_repeats = REPEATS, tag = "", overwrite = F){
  require(tidyverse)
  require(coefplot)
  require(caret)
  require(glue)
  
  out_fp <- glue("figs/result_plots/db_{max_days_between}_ltv_{lsa_topics}_ldatv_{lda_topics}_k_{num_folds}_rep_{num_repeats}{tag}.rds")
  if (file.exists(out_fp) & !overwrite){
    print(as.character(glue("Skipping creation of '{out_fp}' (exists)")))
    return(read_rds(out_fp))
  }
  result_plots <- list()
  
  model_list <- read_models(lsa_topics, lda_topics, max_days_between, tag = tag)
  
  
  result_plots$roc <- dotplot(resamples(model_list), # metric = c("ROC", "Sens", "Spec"),
                              scales = list(x = list(relation = "free",
                                                     rot = 90
                              )),
                              main = "Model Evaluation Metrics")
  result_plots$coef_plot <- multiplot(model_list, intercept = FALSE, only = FALSE,
            xlab = "Log Odds Value",
            single = FALSE,
            sort = "magnitude",
            # horizontal = TRUE,
            # pointSize = 4,
            scales = "fixed") + 
    # scale_y_discrete(labels = rev(coef_names)) +
    labs(title = "Coefficient Plot",
         subtitle = "95% Confidence Level") + 
    ylab(label = "") +
    theme(legend.position = "none") + 
    theme(axis.text.y = element_text(size = 18),
          legend.text = element_text(size = 18),
          legend.title = element_text(size = 18),
          strip.text.x = element_text(size = 14),
          axis.title.x = element_text(size = 17),
          axis.text.x  = element_text(size = 13, angle = 90)
    ) +
    theme(plot.title = element_text(size = 20, hjust = 0.5),
          plot.subtitle = element_text(hjust = 0.5))
  bad_names <- levels(result_plots$coef_plot$data$Coefficient)
  
  good_names <- str_replace( 
      str_replace(
        str_replace(bad_names, "``", ""), "`\\\\`", ""), 
      "\\\\", ""
  )

 levels(result_plots$coef_plot$data$Coefficient) <- good_names
 write_rds(result_plots, out_fp)
 result_plots 
}

tfunc <- function(){
  require(coefplot)
  require(caret)
  coefplot(resamples(tres))
}

get_perplexity <- function(model = F){
  require(readr)
  require(dplyr)
  require(purrr)
  require(glue)
  require(stringr)
  
  warp_lda_paths <- list.files("pre_model_data/topic_model/", pattern = "^warp")
  num_topics <- as.integer(str_extract(warp_lda_paths, "[1234567890]+"))
  extract_perps <- function(fp){
    require(readr)
      
    ffp <- as.character(glue("pre_model_data/topic_model/{fp}"))
    if (model){
      (read_rds(ffp)$lda_mod) 
    } else {
      (as.integer(read_rds(ffp)$perp))
    }
  }

  if (model){
    perps <- warp_lda_paths %>%
      map(~ extract_perps(.))
  } else {
    perps <- warp_lda_paths %>%
      map_dbl(~ extract_perps(.))
  }
  
  tibble(k = num_topics, perps = perps)
}

display_perp <- function(dat_return = T){
  require(tidyverse)
  dat <- get_perplexity() %>%
    arrange(k)
  display_dat <- dat[-nrow(dat),] - dat[-1,]
  
  theme_set(theme_minimal())
  
  display_dat %>%
    ggplot(aes(x = as.factor(k + dat$k[-1] + 1), y = perps)) + 
      geom_line(aes(group = 1)) +
      labs(x = "CMV LDA Topics")
}

all_present_models <- function(){
  require(purrr)
  source("master_vars.R")
  
  input_list <- as.list(expand.grid(lsa_topics = LSA_TOPICS, lda_topics = LDA_TOPICS,
                      max_days_between = DAYS_BETWEEN))
  input_list$num_folds <- rep(K, length(input_list[[1]]))
  input_list$num_repeats <- rep(REPEATS, length(input_list[[1]]))
  
  input_list %>%
    pwalk(present_models)
}

# Function that lets you compare the CI values for a certain variable over all
# the "days between" utilized.
comp_cohort_cis <- function(tag = TAG){
  require(purrr)
  require(magrittr)
  require(tidyverse)
  require(stringr)
  
  cohort_num <- as.integer(str_extract(tag, "[\\d]{1,}"))
  ci_vals <- CH_DB %>% map(~present_models(max_days_between = ., tag = TAG) %$%
                                coef_plot %$%
                                data %>%
                                 dplyr::filter(Model == "model.full",
                                              ) %>%
                                 dplyr::select(Coefficient, contains("Inner"))
                                )
  add_db <- function(df, db){
    df %>%
      mutate(db = db,
             cohort_num = cohort_num
             )
  }
  ci_vals <- map2(ci_vals, CH_DB, add_db) %>%
    bind_rows()
  # Undo fancy naming scheme
  cutoff <- unique(ci_vals$Coefficient[str_detect(ci_vals$Coefficient, "Submissions with Content")])
  num_subs <- unique(ci_vals$Coefficient[str_detect(ci_vals$Coefficient, "Submissions within")])
  removed <-  unique(ci_vals$Coefficient[str_detect(ci_vals$Coefficient, "Removed CMV Subs")])
  
  ci_vals <- ci_vals %>%
    mutate(Coefficient = ifelse(Coefficient %in% cutoff, "(AH) # Submissions with Content before Cutoff",
                                ifelse(Coefficient %in% num_subs, "(AH) # Submissions within Cutoff before 1st CMV Post",
                                       ifelse(Coefficient %in% removed, "(AH) # Removed CMV Subs within Cutoff before 1st CMV Post", Coefficient)
                                )
                         )
    ) %>%
    filter(grepl("^\\(AH\\)", Coefficient)) %>%
    mutate(sig = ifelse((LowInner < 0) & (0 < HighInner), "red", "green"))
  
  (ci_vals)
}

comp_cohort_rocs <- function(tag = TAG){
  cohort_num <- as.integer(str_extract(tag, "[\\d]{1,}"))
  
  results_dat <- CH_DB %>% map(~read_models(max_days_between = ..1, tag = tag))
  results_dat <- results_dat %>% map2(CH_DB, ~ as_tibble(resamples(..1)$values) %>%
                                 select(-Resample) %>%
                                 mutate(max_days_between = ..2,
                                        cohort_num = cohort_num
                                        )
                                ) %>%
    bind_rows()
  roc_ci <- results_dat %>%
    group_by(max_days_between) %>%
    dplyr::summarise(
              base_bci = quantile(`model.base~ROC`, .025),
              base_tci = quantile(`model.base~ROC`, .975),
              past_bci = quantile(`model.past~ROC`, .025),
              past_tci = quantile(`model.past~ROC`, .975),
              full_bci = quantile(`model.full~ROC`, .025),
              full_tci = quantile(`model.full~ROC`, .975)
              )
  browser()
}

# 
# theme_set(theme_minimal())
# 
# # source("~/MACS30200proj/FinalPaper/train_model.R")
# 
# coef_names <- c(
#          "(Post Debate) # Total Comments",
#          "(Post Debate) # OP Comments" ,
#          "(Post Debate) # Direct Comments" ,
#          "(Pre Debate) # Words",
#          "(Pre Debate) Singular First Person Pronouns" ,
#          "(Pre Debate) Sentiment" ,
#          "(Pre Debate) Plural First Person Pronouns" ,
#          "(Pre Debate) Fraction Singular First Person Pronouns" ,
#          "(Pre Debate) Fraction Plural First Person Pronouns",
#          "(Pre Debate) Creation Time", 
#          "(AH) Subreddit Gini Index" ,
#          "(AH) # Submissions with Available Content",
#          "(AH) # CMV Submissions" ,
#          "(AH) Mean Submission Sentiment" ,
#          "(AH) Mean Submission Score" ,
#          "(AH) Mean Singular First Person Pronouns" ,
#          "(AH) Mean Plural First Person Pronouns" ,
#          "(AH) Mean # of Words",
#          "(AH) Fraction Singular First Person Pronouns",
#          "(AH) Fraction Removed Submissions" ,
#          "(AH) Fraction Plural First Person Pronouns" ,
#          "(AH) Fraction of CMV Submissions" ,
#          "(AH) Fraction Empty Submissions" ,
#          "(AH) Daily Submission Frequency" ,
#          "(AH) Total Submission Score",
#          "(AH) Total Singular First Person Pronouns",
#          "(AH) Total Plural First Person Pronouns",
#          "(AH) Total Removed Submissions",
#          "(AH) # All Prior Submissions"
# )
# 
# model_list <- readRDS("~/MACS30200proj/FinalPaper/results.rds")
# new_model_names <- c("Standard (Pre Debate + AH)",
#                      "Standard - Creation Time",
#                      "# Words Only",
#                      "Standard + Post Debate",
#                      "Creation Time Only")
# 
# names(model_list) <- new_model_names
# 
# results <- resamples(model_list)
# 
# graph <- dotplot(results, # metric = c("ROC", "Sens", "Spec"),
#                  scales = list(x = list(relation = "free",
#                                         rot = 90
#                  )),
#                  main = "Model Evaluation Metrics")
# 
# coef_plot <- multiplot(model_list, intercept = FALSE, only = FALSE,
#                        xlab = "Log Odds Value",
#                        single = FALSE,
#                        sort = "alphabetical",
#                        # horizontal = TRUE,
#                        # pointSize = 4,
#                        scales = "fixed") + 
#                # scale_y_discrete(labels = rev(coef_names)) +
#                labs(title = "Coefficient Plot",
#                     subtitle = "95% Confidence Level") + 
#                ylab(label = "") +
#                theme(legend.position = "none") + 
#              theme(axis.text.y = element_text(size = 18),
#                    legend.text = element_text(size = 18),
#                    legend.title = element_text(size = 18),
#                    strip.text.x = element_text(size = 14),
#                    axis.title.x = element_text(size = 17),
#                    axis.text.x  = element_text(size = 13, angle = 90)
#                    ) +
#               theme(plot.title = element_text(size = 20, hjust = 0.5),
#                   plot.subtitle = element_text(hjust = 0.5))
# 
#      
# graph
# coef_plot
# 
