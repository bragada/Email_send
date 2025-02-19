packages <- c("base64enc", "httr", "jsonlite", "janitor", "tidyverse", "aws.s3", "arrow")

missing_packages <- packages[!(packages %in% installed.packages()[, "Package"])]

if (length(missing_packages) > 0) {
  install.packages(missing_packages, repos = "https://cloud.r-project.org/")
}
