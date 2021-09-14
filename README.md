# Ferramenta de administração do Boletim DOU Acredito

Este é um aplicativo web feito no Streamlit utilizado para realizar a tarefa de monitoramento,
execução e postagem do Boletim DOU Acredito. As ferramentas também podem ser executadas como
scripts diretamente no terminal.

## Descrição dos scripts individuais

### Formatter for DOU section 2 posts on whatsapp

This script loads selected acts from section 2 of _Diário Oficial da União_, stored in
Google BigQuery tables (credentials are required and not provided here), and format them
to be published on whatsapp or similar apps.

## Notas

* Para ativar o ambiente virtual python do projeto, execute:

      source ./env/DOUadm/bin/activate

## Written by:

Henrique S. Xavier - [@hsxavier](https://github.com/hsxavier) -  on 2020-07-03.

