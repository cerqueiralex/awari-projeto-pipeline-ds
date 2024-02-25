# Projeto IMDB - Pipeline ETL completo de engenharia de dados

Desenvolvimento um projeto integral de um pipeline de engenharia de dados abrangente, proporcionando uma solução ponta a ponta para extrair dados do IMDB e disponibilizá-los para cientistas e analistas de dados. Este projeto utiliza uma série de tecnologias robustas e modernas para garantir eficiência, escalabilidade e flexibilidade.

Essa arquitetura proporciona um ambiente sólido e modular, permitindo escalabilidade vertical e horizontal conforme necessário. O uso de tecnologias populares no ecossistema de dados garante a confiabilidade e eficiência do pipeline de ponta a ponta.

## Tecnologias utilizadas no projeto
* Python (Pandas, boto3, os, requests, io)
* Apache Spark
* Airflow
* MinIO
* PostgreSQL
* Jupyter Lab
* Docker
* Git

## Definição do Problema:

O projeto consiste em fazer a ingestão dos dados disponíveis em um bucket do MinIO. Os datasets a serem utilizados estão disponíveis em: https://datasets.imdbws.com.

Vamos supor que atuamos em uma empresa e nossos cientistas de dados precisam ter acesso a toda essa informação através do JupyterLab, e necessitam que seja atualizada diariamente. Os dados que iremos usar podem ser acessados em: https://datasets.imdbws.com/

Precisamos ter um histórico de quando os arquivos foram atualizados, para que possamos validar o passado. Também precisaremos enviar as informações para um banco de dados Postgres, para que elas possam ser visualizadas em uma ferramenta de Bl, que, no nosso caso, será o Metabase.

<img src="https://i.imgur.com/B0ceA7i.png">

### 1. Airflow: Ingestão

Para o Airflow, é criada uma DAG para cada arquivo disponível no IMDB com 3 Tasks cada, a primeira é responsável por:

1. fazer o download do arquivo correspondente a cada dia em subpastas nomeadas no formato YYYY-MM-DD (ano/mês/dia), dentro da pasta downloaded;
2. descompactar e salvar o arquivo baixado na pasta datalake, sobrescrevendo o último arquivo.

### 2. Airflow: processamento

Desenvolvida uma segunda DAG que será disparada sempre que houver novos arquivos. Os arquivos são convertidos de CSV para Parquet e salvos em:

imdb/processed/{UMA PASTA POR TIPO DE ARQUIVO}

### 3. Airflow: disponibilização

A terceira DAG que será disparada sempre que houver novos arquivos. Tendo novos arquivos, seus dados são inseridos em tabelas no banco de dados Postgres.

## Estrutura Docker (docker-compose/docker-compose.yml)

* MinIO: 4 containers Docker executando instâncias do servidor MinIO. Utilizando um proxy reverso do Nginx e balanceamento de carga, você poderá acessá-los pela porta 9000.
* MinIO: Aplicativo RELEASE.2023-02-22T18-23-45Z
* Postgres: Imagem na versão 12
* Postgres: Imagem postgres:15.2-alpine3.17
* Airflow: Scheduler
* Airflow: Webserver
* Airflow: init
* Metabase: Aplicativo do Metabase v0.45.3

