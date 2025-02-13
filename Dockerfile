FROM rocker/r-ver:latest

# Instalar pacotes essenciais do R
RUN R -e "install.packages(c('tidyverse', 'rmarkdown', 'aws.s3'), repos='http://cran.rstudio.com/')"

# Definir o diretório de trabalho dentro do container
WORKDIR /app

# Copiar os arquivos necessários para dentro do container
COPY email_prefeitura.R /app/
COPY script_relatorio.Rmd /app/

# Comando de execução do script
CMD ["Rscript", "email_prefeitura.R"]
