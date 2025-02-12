# Use uma imagem base com Ubuntu
FROM mcr.microsoft.com/vscode/devcontainers/base:ubuntu

# Adicionar a chave do CRAN e o repositório para versões mais recentes do R
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
    add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"

# Atualizar e instalar a versão mais recente do R
RUN apt update && apt install -y r-base
