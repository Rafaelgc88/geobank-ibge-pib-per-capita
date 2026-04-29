# GeoBank IBGE PIB per capita

Este repositório gera um CSV leve com o PIB per capita oficial dos municípios
do Nordeste, para consumo pelo Google Apps Script do projeto GeoBank.

O pipeline baixa a base oficial do IBGE, extrai o campo oficial de PIB per
capita e publica apenas as colunas necessárias:

```csv
cod_ibge,ano,pib_per_capita
```

## Fonte oficial

Base oficial do IBGE PIB dos Municípios 2010-2023:

https://ftp.ibge.gov.br/Pib_Municipios/2022_2023/base/base_de_dados_2010_2023_txt.zip

Campo usado:

`Produto Interno Bruto per capita, a preços correntes (R$ 1,00)`

Não há cálculo manual de PIB dividido por população. O valor publicado no CSV
vem diretamente do campo oficial do IBGE.

## Arquivos publicados

CSV:

https://raw.githubusercontent.com/Rafaelgc88/geobank-ibge-pib-per-capita/main/data/ibge_pib_per_capita_nordeste.csv

Metadata:

https://raw.githubusercontent.com/Rafaelgc88/geobank-ibge-pib-per-capita/main/data/ibge_pib_per_capita_metadata.json

## Atualização

O workflow `Atualizar PIB per capita IBGE` roda mensalmente e também pode ser
acionado manualmente:

1. Abra a aba `Actions` no GitHub.
2. Selecione `Atualizar PIB per capita IBGE`.
3. Clique em `Run workflow`.

O workflow usa Python 3.11, executa `scripts/extrair_pib_per_capita.py` e faz
commit automático dos arquivos em `data/` quando houver alteração.

## Consumo pelo Apps Script

O Apps Script deve baixar o CSV raw, fazer o parse das linhas e usar
`cod_ibge` como chave de município. A coluna `pib_per_capita` deve ser tratada
como o valor oficial do IBGE, preservando a representação publicada no CSV.

Exemplo de fluxo:

1. Buscar a URL raw do CSV com `UrlFetchApp.fetch`.
2. Ler o conteúdo com `getContentText("UTF-8")`.
3. Separar as linhas como CSV.
4. Montar um mapa `{ cod_ibge: { ano, pib_per_capita } }`.
5. Cruzar esse mapa com os municípios já usados pelo GeoBank.

## Execução local

```bash
python3 scripts/extrair_pib_per_capita.py
```

O script usa apenas biblioteca padrão do Python.
