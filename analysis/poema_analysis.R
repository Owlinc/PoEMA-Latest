# Libraries
library(ggpubr)
library(tidyverse)
library(psych)
library(rstatix)
library(car)

# Reading data
previous_data = read.csv("previous_study.csv", header=T, na = "")
work_data = read.csv("current_study.csv", header=T, na = "")

# Formatting string to numeric
work_data$SUS <- as.numeric(gsub(",", ".", work_data$SUS))
work_data$CR <- as.numeric(gsub(",", ".", work_data$CR))
work_data$Weight <- as.numeric(gsub(",", ".", work_data$Weight))
previous_data$CR <- as.numeric(gsub(",", ".", previous_data$CR))

# Alpha Cronbach's scale
computeDiligence <- select(work_data, 15:18)
computeSUS <- select(work_data, 19:28)
computeBurden <- select(work_data, 29:31)
alpha(computeDiligence)
alpha(computeSUS)
alpha(computeBurden)

# Mean
mean(work_data$Burden, na.rm = FALSE)
mean(work_data$SUS, na.rm = FALSE)
mean(work_data$SR_Diligence, na.rm = FALSE)

# SD
sd(work_data$Burden, na.rm = FALSE)
sd(work_data$SUS, na.rm = FALSE)
sd(work_data$SR_Diligence, na.rm = FALSE)

# Levene Test (checking for variance homogenity)
combined_scores <- c(work_data$CR, previous_data$CR)
group <- factor(c(rep("current", length(work_data$CR)), rep("previous", length(previous_data$CR))))
leven_test_df <- data.frame(CR = combined_scores, group = group)
leven_test_df$CR <- as.numeric(gsub(",", ".", leven_test_df$CR))
leveneTest(CR ~ group, data = leven_test_df)

# Shapiro test (checking ofr linear distribution)
shapiro_test(previous_data, CR)
shapiro_test(work_data, CR)

# Welch's t-test
t.test(work_data$CR, as.double(previous_data$CR), alternative = "two.sided", var.equal = FALSE)

# Linear regression model
cr_ols = lm(CR ~ Burden + SUS, data = work_data, weights = work_data$Weight)
summary(cr_ols)

