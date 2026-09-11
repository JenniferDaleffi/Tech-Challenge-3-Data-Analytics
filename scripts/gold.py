import sys

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ============================================================
# 1. INICIALIZAÇÃO
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ============================================================
# 2. CAMINHOS S3
# ============================================================

BASE = "s3://lab-300271615875"

caminho_silver = (
    f"{BASE}/Silver/State_of_Data_Harmonizado/"
)

caminho_gold = (
    f"{BASE}/Gold/"
)


# ============================================================
# 3. LEITURA DA SILVER
# ============================================================

df = spark.read.parquet(caminho_silver)

print("======================================")
print("CAMADA SILVER CARREGADA")
print("======================================")

print("TOTAL DE REGISTROS:", df.count())

df.groupBy(
    "edicao_pesquisa"
).count().orderBy(
    "edicao_pesquisa"
).show()


# ============================================================
# 4. GOLD - RESUMO GERAL
# ============================================================

gold_resumo = (
    df
    .groupBy("edicao_pesquisa")
    .agg(
        F.count("*").alias("total_respondentes"),
        F.count("cargo_atual").alias("respondentes_com_cargo"),
        F.count("nivel_profissional").alias(
            "respondentes_com_senioridade"
        ),
        F.count("faixa_salarial").alias(
            "respondentes_com_salario"
        )
    )
)

(
    gold_resumo
    .write
    .mode("overwrite")
    .parquet(
        caminho_gold + "resumo_geral/"
    )
)


# ============================================================
# 5. GOLD - ESTRUTURA DO MERCADO / CARGOS
# ============================================================

gold_cargos = (
    df
    .filter(
        F.col("cargo_atual").isNotNull()
    )
    .groupBy(
        "edicao_pesquisa",
        "cargo_atual",
        "nivel_profissional"
    )
    .agg(
        F.count("*").alias("quantidade_profissionais")
    )
)

janela_cargos = Window.partitionBy(
    "edicao_pesquisa"
)

gold_cargos = (
    gold_cargos
    .withColumn(
        "total_edicao",
        F.sum(
            "quantidade_profissionais"
        ).over(janela_cargos)
    )
    .withColumn(
        "percentual",
        F.round(
            F.col("quantidade_profissionais")
            / F.col("total_edicao")
            * 100,
            2
        )
    )
)

(
    gold_cargos
    .write
    .mode("overwrite")
    .partitionBy("edicao_pesquisa")
    .parquet(
        caminho_gold + "mercado_cargos/"
    )
)


# ============================================================
# 6. TRATAMENTO DAS FAIXAS SALARIAIS
# ============================================================

# Existem dois registros anômalos nas bases originais:
#
# "de R$ 101/mês a R$ 2.000/mês"
#
# "de R$ 25.001/mês a R$ 3000/mês"
#
# Aqui eles são padronizados para as faixas equivalentes
# existentes nas pesquisas.

df_salario = (
    df
    .withColumn(
        "faixa_salarial_tratada",

        F.when(
            F.col("faixa_salarial")
            == "de R$ 101/mês a R$ 2.000/mês",

            "de R$ 1.001/mês a R$ 2.000/mês"
        )

        .when(
            F.col("faixa_salarial")
            == "de R$ 25.001/mês a R$ 3000/mês",

            "de R$ 25.001/mês a R$ 30.000/mês"
        )

        .otherwise(
            F.col("faixa_salarial")
        )
    )
)


# ============================================================
# 7. ORDEM DAS FAIXAS SALARIAIS
# ============================================================

df_salario = (
    df_salario
    .withColumn(

        "ordem_faixa_salarial",

        F.when(
            F.col("faixa_salarial_tratada")
            == "Menos de R$ 1.000/mês",
            1
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "de R$ 1.001/mês a R$ 2.000/mês",
            2
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "de R$ 2.001/mês a R$ 3.000/mês",
            3
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "de R$ 3.001/mês a R$ 4.000/mês",
            4
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "de R$ 4.001/mês a R$ 6.000/mês",
            5
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "de R$ 6.001/mês a R$ 8.000/mês",
            6
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "de R$ 8.001/mês a R$ 12.000/mês",
            7
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "de R$ 12.001/mês a R$ 16.000/mês",
            8
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "de R$ 16.001/mês a R$ 20.000/mês",
            9
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "de R$ 20.001/mês a R$ 25.000/mês",
            10
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "de R$ 25.001/mês a R$ 30.000/mês",
            11
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "de R$ 30.001/mês a R$ 40.000/mês",
            12
        )

        .when(
            F.col("faixa_salarial_tratada")
            == "Acima de R$ 40.001/mês",
            13
        )
    )
)


# ============================================================
# GOLD - REMUNERAÇÃO POR CARGO E SENIORIDADE
# ============================================================

gold_remuneracao = (
    df_salario

    .filter(
        F.col("faixa_salarial_tratada").isNotNull()
        &
        F.col("nivel_profissional").isNotNull()
        &
        F.col("cargo_atual").isNotNull()
    )

    .groupBy(
        "edicao_pesquisa",
        "cargo_atual",
        "nivel_profissional",
        "faixa_salarial_tratada",
        "ordem_faixa_salarial"
    )

    .agg(
        F.count("*").alias(
            "quantidade_profissionais"
        )
    )
)


# Total por cargo + senioridade
janela_salario = Window.partitionBy(
    "edicao_pesquisa",
    "cargo_atual",
    "nivel_profissional"
)


gold_remuneracao = (
    gold_remuneracao

    .withColumn(
        "total_cargo_senioridade",
        F.sum(
            "quantidade_profissionais"
        ).over(janela_salario)
    )

    .withColumn(
        "percentual_cargo_senioridade",
        F.round(
            (
                F.col("quantidade_profissionais")
                /
                F.col("total_cargo_senioridade")
            ) * 100,
            2
        )
    )
)


(
    gold_remuneracao
    .write
    .mode("overwrite")
    .partitionBy("edicao_pesquisa")
    .parquet(
        caminho_gold
        + "remuneracao_senioridade/"
    )
)


# ============================================================
# 9. GOLD - DIVERSIDADE DE GÊNERO
# ============================================================

gold_genero = (
    df
    .filter(
        F.col("genero").isNotNull()
    )
    .groupBy(
        "edicao_pesquisa",
        "genero"
    )
    .agg(
        F.count("*").alias("quantidade_profissionais")
    )
)


janela_genero = Window.partitionBy(
    "edicao_pesquisa"
)


gold_genero = (
    gold_genero

    .withColumn(
        "total_respostas",
        F.sum(
            "quantidade_profissionais"
        ).over(janela_genero)
    )

    .withColumn(
        "percentual",
        F.round(
            F.col("quantidade_profissionais")
            / F.col("total_respostas")
            * 100,
            2
        )
    )
)


(
    gold_genero
    .write
    .mode("overwrite")
    .partitionBy("edicao_pesquisa")
    .parquet(
        caminho_gold
        + "diversidade_genero/"
    )
)


# ============================================================
# 10. GOLD - REGIÕES
# ============================================================

gold_regiao = (
    df
    .filter(
        F.col("regiao").isNotNull()
    )
    .groupBy(
        "edicao_pesquisa",
        "regiao"
    )
    .agg(
        F.count("*").alias("quantidade_profissionais")
    )
)


janela_regiao = Window.partitionBy(
    "edicao_pesquisa"
)


gold_regiao = (
    gold_regiao

    .withColumn(
        "total_respostas",
        F.sum(
            "quantidade_profissionais"
        ).over(janela_regiao)
    )

    .withColumn(
        "percentual",
        F.round(
            F.col("quantidade_profissionais")
            / F.col("total_respostas")
            * 100,
            2
        )
    )
)


(
    gold_regiao
    .write
    .mode("overwrite")
    .partitionBy("edicao_pesquisa")
    .parquet(
        caminho_gold + "regioes/"
    )
)


# ============================================================
# 11. PADRONIZAÇÃO DO MODELO DE TRABALHO
# ============================================================

df_trabalho = (
    df
    .withColumn(

        "modelo_trabalho_resumido",

        F.when(
            F.col("modelo_trabalho")
            == "Modelo 100% remoto",
            "Remoto"
        )

        .when(
            F.col("modelo_trabalho")
            == "Modelo 100% presencial",
            "Presencial"
        )

        .when(
            F.col("modelo_trabalho")
            .contains("híbrido flexível"),
            "Híbrido flexível"
        )

        .when(
            F.col("modelo_trabalho")
            .contains("híbrido com dias fixos"),
            "Híbrido fixo"
        )

        .otherwise(
            F.col("modelo_trabalho")
        )
    )
)


# ============================================================
# 12. GOLD - MODELO DE TRABALHO
# ============================================================

gold_trabalho = (
    df_trabalho

    .filter(
        F.col(
            "modelo_trabalho_resumido"
        ).isNotNull()
    )

    .groupBy(
        "edicao_pesquisa",
        "modelo_trabalho_resumido"
    )

    .agg(
        F.count("*").alias(
            "quantidade_profissionais"
        )
    )
)


janela_trabalho = Window.partitionBy(
    "edicao_pesquisa"
)


gold_trabalho = (
    gold_trabalho

    .withColumn(
        "total_respostas",
        F.sum(
            "quantidade_profissionais"
        ).over(janela_trabalho)
    )

    .withColumn(
        "percentual",
        F.round(
            F.col("quantidade_profissionais")
            / F.col("total_respostas")
            * 100,
            2
        )
    )
)


(
    gold_trabalho
    .write
    .mode("overwrite")
    .partitionBy("edicao_pesquisa")
    .parquet(
        caminho_gold
        + "modelo_trabalho/"
    )
)


# ============================================================
# 13. FUNÇÃO PARA TECNOLOGIAS
# ============================================================

def criar_gold_tecnologia(
    dataframe,
    coluna,
    nome_tecnologia
):

    return (
        dataframe

        .filter(
            F.col(coluna).isNotNull()
        )

        .groupBy(
            "edicao_pesquisa"
        )

        .agg(

            F.sum(
                F.col(coluna)
            ).alias(
                "quantidade_usuarios"
            ),

            F.count(
                F.col(coluna)
            ).alias(
                "respondentes_validos"
            )
        )

        .withColumn(
            "tecnologia",
            F.lit(nome_tecnologia)
        )

        .withColumn(
            "percentual_adocao",

            F.round(
                F.col("quantidade_usuarios")
                /
                F.col("respondentes_validos")
                * 100,
                2
            )
        )

        .select(
            "edicao_pesquisa",
            "tecnologia",
            "quantidade_usuarios",
            "respondentes_validos",
            "percentual_adocao"
        )
    )


# ============================================================
# 14. GOLD - TECNOLOGIAS
# ============================================================

gold_sql = criar_gold_tecnologia(
    df,
    "usa_sql",
    "SQL"
)

gold_python = criar_gold_tecnologia(
    df,
    "usa_python",
    "Python"
)

gold_aws = criar_gold_tecnologia(
    df,
    "usa_aws",
    "AWS"
)

gold_powerbi = criar_gold_tecnologia(
    df,
    "usa_powerbi",
    "Power BI"
)


gold_tecnologias = (
    gold_sql
    .unionByName(gold_python)
    .unionByName(gold_aws)
    .unionByName(gold_powerbi)
)


(
    gold_tecnologias
    .write
    .mode("overwrite")
    .partitionBy("edicao_pesquisa")
    .parquet(
        caminho_gold + "tecnologias/"
    )
)


# ============================================================
# 15. TRATAMENTO DE IA
# ============================================================

texto_ia = F.lower(
    F.coalesce(
        F.col("usa_ia_trabalho"),
        F.lit("")
    )
)


df_ia = (
    df
    .withColumn(

        "adota_ia",

        F.when(
            F.col("usa_ia_trabalho").isNull(),
            None
        )

        # Primeiro identificamos respostas positivas.
        # Isso é importante porque existem respostas
        # com múltiplas alternativas.

        .when(
            texto_ia.rlike(
                "utilizo apenas|"
                "utilizo soluções|"
                "uso soluções"
            ),
            1
        )

        .when(
            texto_ia.contains(
                "não utilizo nenhum tipo"
            ),
            0
        )

        .otherwise(1)
    )
)


# ============================================================
# 16. GOLD - ADOÇÃO DE IA
# ============================================================

gold_ia = (
    df_ia

    .filter(
        F.col("adota_ia").isNotNull()
    )

    .groupBy(
        "edicao_pesquisa"
    )

    .agg(

        F.sum(
            "adota_ia"
        ).alias(
            "usuarios_ia"
        ),

        F.count(
            "adota_ia"
        ).alias(
            "respondentes_validos"
        )
    )

    .withColumn(
        "nao_usuarios_ia",

        F.col("respondentes_validos")
        -
        F.col("usuarios_ia")
    )

    .withColumn(
        "percentual_adocao_ia",

        F.round(
            F.col("usuarios_ia")
            /
            F.col("respondentes_validos")
            * 100,
            2
        )
    )
)


(
    gold_ia
    .write
    .mode("overwrite")
    .parquet(
        caminho_gold + "adocao_ia/"
    )
)


# ============================================================
# 17. VALIDAÇÃO
# ============================================================

print("======================================")
print("GOLD - RESUMO")
print("======================================")

gold_resumo.show(
    truncate=False
)


print("======================================")
print("GOLD - GÊNERO")
print("======================================")

gold_genero.orderBy(
    "edicao_pesquisa",
    F.desc("percentual")
).show(
    truncate=False
)


print("======================================")
print("GOLD - REGIÕES")
print("======================================")

gold_regiao.orderBy(
    "edicao_pesquisa",
    F.desc("percentual")
).show(
    truncate=False
)


print("======================================")
print("GOLD - TECNOLOGIAS")
print("======================================")

gold_tecnologias.orderBy(
    "edicao_pesquisa",
    F.desc("percentual_adocao")
).show(
    truncate=False
)


print("======================================")
print("GOLD - IA")
print("======================================")

gold_ia.orderBy(
    "edicao_pesquisa"
).show(
    truncate=False
)


print("======================================")
print("CAMADA GOLD CRIADA COM SUCESSO")
print("======================================")


# ============================================================
# 18. FINALIZAÇÃO
# ============================================================

job.commit()