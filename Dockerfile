FROM rocker/r-ver:latest

# Instalar pacotes essenciais do R
RUN R -e "install.packages(c('tidyverse', 'rmarkdown', 'aws.s3'), repos='http://cran.rstudio.com/')"

# Copiar o script para dentro do contêiner
WORKDIR /app
COPY script_relatorio.Rmd /app/
COPY email_prefeitura.R /app/

# Definir o comando de execução do script
CMD ["Rscript", "email_prefeitura.R"]
