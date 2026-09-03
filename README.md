## Tech Challenge Data Analytics FIAP, 2026 | Fase 3

## Integrantes

- Bruno Querobin Francisco – RM371779

- Henrique dos Reis Machado - RM372837

- Jennifer Eduarda Vieira Daleffi – RM374013

- Leonardo de Carvalho Melo – RM370240 

- Renan Simões de Farias – RM371818 

<h3 align="center"> State of Data Brasil 💻 </h3>

<table align="center">
  <tr>
    <td align="center" style="padding: 10px;">
      <img src="https://github.com/user-attachments/assets/549919ba-db08-44b3-8209-ace69288f2a9" width="120" height="120" style="border-radius: 10px; object-fit: cover;" /><br>
      <sub><b>Bruno</b></sub><br>
      <a href="https://github.com/BrunoQF" target="_blank"><img width="20" height="20" src="https://img.icons8.com/ios-filled/50/github.png" alt="github"/></a>
    </td><td align="center" style="padding: 10px;">
      <img src="https://github.com/user-attachments/assets/71df3046-85db-49c9-a109-67e733f521ab" width="120" height="120" style="border-radius: 10px; object-fit: cover;" /><br>
      <sub><b>Henrique</b></sub><br>
      <a href="https://github.com/IkkiMachado/" target="_blank"><img width="20" height="20" src="https://img.icons8.com/ios-filled/50/github.png" alt="github"/></a>
    </td><td align="center" style="padding: 10px;">
      <img src="https://github.com/user-attachments/assets/10cc8aba-a63b-4cc9-9c61-745cfa32fad9" width="120" height="120" style="border-radius: 10px; object-fit: cover;" /><br>
      <sub><b>Jennifer</b></sub><br>
      <a href="https://github.com/JenniferDaleffi/" target="_blank"><img width="20" height="20" src="https://img.icons8.com/ios-filled/50/github.png" alt="github"/></a>
    </td><td align="center" style="padding: 10px;">
      <img src="https://github.com/user-attachments/assets/be1754d2-6580-46e6-820b-b816f83188b8" width="120" height="120" style="border-radius: 10px; object-fit: cover;" /><br>
      <sub><b>Leonardo</b></sub><br>
      <a href="https://github.com/LeoMelo95/" target="_blank"><img width="20" height="20" src="https://img.icons8.com/ios-filled/50/github.png" alt="github"/></a>
    </td><td align="center" style="padding: 10px;">
      <img src="https://github.com/user-attachments/assets/d47f8585-e513-45bb-9e8b-5451113feeb8" width="120" height="120" style="border-radius: 10px; object-fit: cover;" /><br>
      <sub><b>Renan</b></sub><br>
      <a href="https://github.com/ElRekan/" target="_blank"><img width="20" height="20" src="https://img.icons8.com/ios-filled/50/github.png" alt="github"/></a>
    </td>
  </tr>
</table>

## 📌 Tema: State of Data Brasil

Projeto desenvolvido para o Tech Challenge da FIAP – Fase 3, com foco em
Engenharia de Dados, Big Data, Analytics e processamento em Cloud AWS.

O projeto utiliza as três últimas pesquisas disponíveis do State of Data Brasil,
realizadas pela comunidade Data Hackers em parceria com a Bain, para analisar
o cenário do mercado brasileiro de profissionais de Dados, Analytics e
Inteligência Artificial.

---

## 🎯 Objetivo

Construir uma solução completa de Engenharia de Dados e Analytics em ambiente
AWS, desde a ingestão dos dados brutos até sua disponibilização para consultas
analíticas e geração de insights.

A análise busca compreender:

- Perfil dos profissionais de Dados no Brasil;
- Formação acadêmica;
- Experiência profissional;
- Remuneração;
- Senioridade;
- Tecnologias utilizadas;
- Utilização de Inteligência Artificial;
- Modelo de trabalho;
- Diferenças entre regiões;
- Diversidade de gênero;
- Tendências do mercado de Dados e Analytics.

---

# 🏗️ Arquitetura da Solução AWS

A solução foi estruturada utilizando uma arquitetura de Data Lake na AWS,
organizada em camadas Bronze, Silver e Gold.

O fluxo principal da solução é:

**Dados brutos → Ingestão → Processamento → Dados tratados → Catalogação →
Consultas analíticas → Insights**

## Diagrama da Arquitetura

<img width="1512" height="772" alt="Image" src="https://github.com/user-attachments/assets/95839a87-8bfb-40c4-a92f-bb109afe63b8" />

# ☁️ Serviços AWS utilizados

| Serviço | Função |
|---|---|
| Amazon S3 | Armazenamento das camadas Bronze, Silver e Gold |
| AWS Glue | ETL, transformação e processamento dos dados |
| Apache Spark / PySpark | Processamento distribuído |
| AWS Glue Data Catalog | Catalogação dos dados, schemas e tabelas |
| Amazon Athena | Consultas SQL e análises |
| AWS Academy Lab | Ambiente utilizado para desenvolvimento da solução |

---

# 🗂️ Organização dos dados

## 🥉 Bronze — Dados Brutos

A camada **Bronze** é responsável pelo armazenamento dos dados brutos, mantendo os arquivos em seu formato original após a ingestão.

As bases utilizadas correspondem às três últimas pesquisas disponíveis do **State of Data Brasil**.

### Estrutura

```text
bronze/
├── 2023/
├── 2024/
└── 2025-2026/
```

---

## 🥈 Silver — Dados Tratados

A camada **Silver** contém os dados após os processos de tratamento e transformação realizados durante o pipeline de dados.

Nesta etapa são realizados processos como:

- Limpeza dos dados;
- Padronização das informações;
- Tratamento de valores nulos;
- Transformação de colunas;
- Definição e correção de tipos de dados;
- Conversão dos dados para o formato **Parquet**.

### Estrutura

```text
silver/
├── state_of_data_2023/
├── state_of_data_2024/
└── state_of_data_2025_2026/
```

---

## 🥇 Gold — Dados Analíticos

A camada **Gold** contém os dados preparados e organizados para o consumo analítico, consultas e geração de insights.

Nesta camada são disponibilizadas informações utilizadas nas análises do projeto, como:

- Remuneração;
- Senioridade;
- Tecnologias;
- Inteligência Artificial;
- Região;
- Modelo de trabalho;
- Formação;
- Perfil profissional.

### Estrutura

```text
gold/
├── remuneracao/
├── senioridade/
├── tecnologias/
├── inteligencia_artificial/
├── regiao/
└── modelo_trabalho/
```
