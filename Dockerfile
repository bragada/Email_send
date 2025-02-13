# Usa a imagem otimizada para R
FROM rocker/tidyverse:latest

# Define o diretório de trabalho
WORKDIR /app

# Instala pacotes adicionais necessários
RUN R -e "install.packages(c('aws.s3', 'rmarkdown',janitor, 'httr', 'jsonlite', 'arrow', 'keyring', 'tinytex'))"

# Copia todos os arquivos do repositório para o container
COPY . /app

# Comando padrão: executa todos os scripts R em sequência
CMD Rscript -e "source('email_prefeitura.R')"
