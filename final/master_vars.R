# Master Variables 

LSA_TOPICS <- c(100, 50, 150, 200)
LDA_TOPICS <- c(7, 4, 5, 6, 8, 9, 10)
DAYS_BETWEEN <- c(730, 30, 60, 90, 180, 365)
K <- 10
REPEATS <- 20

CHOICE_LSA <- 150
CHOICE_LDA <- 9
CHOICE_DB <- 365

COHORT <- 30
TAG <- as.character(glue::glue("_{COHORT}"))
CH_DB <- c(30, 60, 90, 180, 270, 365, 545, 720)