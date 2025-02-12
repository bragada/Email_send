if (!requireNamespace("tidyverse", quietly = TRUE)) install.packages("rmarkdown")
if (!requireNamespace("rmarkdown", quietly = TRUE)) install.packages("rmarkdown")
if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")

library(tidyverse)
library(rmarkdown)


rmarkdown::pandoc_version()
render_relatorio <- rmarkdown::render(
 input = "script_relatorio.Rmd",
 output_file = "program_conecta_campinas.pdf"
)

put_object(
    file = "program_conecta_campinas.pdf",
    object = "program_conecta_campinas.pdf",
    bucket = "automacao-conecta",
    region = "sa-east-1"
  )

info_relatorio <- file.info("program_conecta_campinas.pdf")

if(as.Date(info_relatorio$mtime,tz = "America/Sao_Paulo") == Sys.Date()){
    print("s")
  }else {
print("n")}
