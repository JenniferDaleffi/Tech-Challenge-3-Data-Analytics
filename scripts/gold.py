import sys

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ============================================================
# 1. INICIALIZAÇÃO DO AWS GLUE
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ============================================================
# 2. CAMINHOS DO S3
# ============================================================

BASE = "s3://lab-300271615875"

CAMINHO_SILVER = (
    f"{BASE}/Silver/State_of_Data_Harmonizado/"
)

CAMINHO_GOLD = (
    f"{BASE}/Gold/"
)


# ============================================================
# 3. LEITURA DA CAMADA SILVER
# ============================================================

df = spark.read.parquet(CAMINHO_SILVER)

print("============================================")
print("CAMADA SILVER CARREGADA")
print("============================================")

total_silver = df.count()

print("TOTAL DE REGISTROS:", total_silver)

(
    df
    .groupBy("edicao_pesquisa")
    .count()
    .orderBy("edicao_pesquisa")
    .show(truncate=False)
)


# ============================================================
# 4. FUNÇÃO AUXILIAR PARA GRAVAÇÃO
# ============================================================

def gravar_gold(dataframe, pasta, particionar=True):

    writer = (
        dataframe
        .write
        .mode("overwrite")
    )

    if particionar and "edicao_pesquisa" in dataframe.columns:
        writer = writer.partitionBy("edicao_pesquisa")

    writer.parquet(
        CAMINHO_GOLD + pasta
    )

    print(
        f"GOLD gravada: {CAMINHO_GOLD}{pasta}"
    )


# ============================================================
# 5. GOLD - RESUMO GERAL
# ============================================================

gold_resumo = (
    df
    .groupBy("edicao_pesquisa")
    .agg(
        F.count("*").alias(
            "total_respondentes"
        ),

        F.count("cargo_atual").alias(
            "respondentes_com_cargo"
        ),

        F.count("nivel_profissional").alias(
            "respondentes_com_senioridade"
        ),

        F.count("faixa_salarial").alias(
            "respondentes_com_salario"
        ),

        F.count("usa_ia_trabalho").alias(
            "respondentes_bloco_ia"
        )
    )
)

gravar_gold(
    gold_resumo,
    "resumo_geral/",
    particionar=False
)


# ============================================================
# 6. GOLD - ESTRUTURA DO MERCADO / CARGOS
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
        F.count("*").alias(
            "quantidade_profissionais"
        )
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
            (
                F.col("quantidade_profissionais")
                /
                F.col("total_edicao")
            ) * 100,
            2
        )
    )
)

gravar_gold(
    gold_cargos,
    "mercado_cargos/"
)


# ============================================================
# 7. TRATAMENTO DAS FAIXAS SALARIAIS
# ============================================================

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
# 8. ORDEM DAS FAIXAS SALARIAIS
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

        .otherwise(99)
    )
)


# ============================================================
# 9. GOLD - REMUNERAÇÃO POR CARGO E SENIORIDADE
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

gravar_gold(
    gold_remuneracao,
    "remuneracao_senioridade/"
)


# ============================================================
# 10. GOLD - DIVERSIDADE DE GÊNERO
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
        F.count("*").alias(
            "quantidade_profissionais"
        )
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
            (
                F.col("quantidade_profissionais")
                /
                F.col("total_respostas")
            ) * 100,
            2
        )
    )
)

gravar_gold(
    gold_genero,
    "diversidade_genero/"
)


# ============================================================
# 11. GOLD - REGIÕES
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
        F.count("*").alias(
            "quantidade_profissionais"
        )
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
            (
                F.col("quantidade_profissionais")
                /
                F.col("total_respostas")
            ) * 100,
            2
        )
    )
)

gravar_gold(
    gold_regiao,
    "regioes/"
)


# ============================================================
# 12. PADRONIZAÇÃO DO MODELO DE TRABALHO
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
            F.lower(
                F.col("modelo_trabalho")
            ).contains(
                "híbrido flexível"
            ),
            "Híbrido flexível"
        )

        .when(
            F.lower(
                F.col("modelo_trabalho")
            ).contains(
                "híbrido com dias fixos"
            ),
            "Híbrido fixo"
        )

        .otherwise(
            F.col("modelo_trabalho")
        )
    )
)


# ============================================================
# 13. GOLD - MODELO DE TRABALHO
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
            (
                F.col("quantidade_profissionais")
                /
                F.col("total_respostas")
            ) * 100,
            2
        )
    )
)

gravar_gold(
    gold_trabalho,
    "modelo_trabalho/"
)


# ============================================================
# 14. FUNÇÃO AUXILIAR - TECNOLOGIAS
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
                (
                    F.col("quantidade_usuarios")
                    /
                    F.col("respondentes_validos")
                ) * 100,
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
# 15. GOLD - TECNOLOGIAS
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

gravar_gold(
    gold_tecnologias,
    "tecnologias/"
)


# ============================================================
# 16. GOLD - ADOÇÃO DE IA
# ============================================================

gold_adocao_ia = (
    df
    .filter(
        F.col(
            "ia_adota_produtividade"
        ).isNotNull()
    )
    .groupBy(
        "edicao_pesquisa"
    )
    .agg(
        F.sum(
            "ia_adota_produtividade"
        ).alias(
            "usuarios_ia"
        ),

        F.count(
            "ia_adota_produtividade"
        ).alias(
            "respondentes_validos"
        )
    )
    .withColumn(
        "nao_usuarios_ia",
        F.col(
            "respondentes_validos"
        )
        -
        F.col(
            "usuarios_ia"
        )
    )
    .withColumn(
        "percentual_adocao_ia",
        F.round(
            (
                F.col("usuarios_ia")
                /
                F.col("respondentes_validos")
            ) * 100,
            2
        )
    )
)

gravar_gold(
    gold_adocao_ia,
    "adocao_ia/"
)


# ============================================================
# 17. FUNÇÃO AUXILIAR PARA MÉTRICAS BINÁRIAS DE IA
# ============================================================

def criar_metrica_binaria_ia(
    dataframe,
    coluna,
    metrica,
    categoria
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
                "quantidade"
            ),

            F.count(
                F.col(coluna)
            ).alias(
                "respondentes_validos"
            )
        )
        .withColumn(
            "metrica",
            F.lit(metrica)
        )
        .withColumn(
            "categoria",
            F.lit(categoria)
        )
        .withColumn(
            "percentual",
            F.round(
                (
                    F.col("quantidade")
                    /
                    F.col("respondentes_validos")
                ) * 100,
                2
            )
        )
        .select(
            "edicao_pesquisa",
            "categoria",
            "metrica",
            "quantidade",
            "respondentes_validos",
            "percentual"
        )
    )


# ============================================================
# 18. GOLD - IA PRODUTIVIDADE / CUSTO
# ============================================================

ia_prod_gratuita = criar_metrica_binaria_ia(
    df,
    "ia_prod_gratuita",
    "Uso de solução gratuita",
    "Produtividade e custo"
)

ia_prod_paga_proprio = criar_metrica_binaria_ia(
    df,
    "ia_prod_paga_proprio",
    "Profissional paga a solução",
    "Produtividade e custo"
)

ia_prod_paga_empresa = criar_metrica_binaria_ia(
    df,
    "ia_prod_paga_empresa",
    "Empresa paga a solução",
    "Produtividade e custo"
)

ia_prod_copilot = criar_metrica_binaria_ia(
    df,
    "ia_prod_copilot",
    "Uso de Copilot",
    "Produtividade e custo"
)

ia_prod_nao_usa = criar_metrica_binaria_ia(
    df,
    "ia_prod_nao_usa",
    "Não usa IA para produtividade",
    "Produtividade e custo"
)

gold_ia_produtividade_custo = (
    ia_prod_gratuita
    .unionByName(ia_prod_paga_proprio)
    .unionByName(ia_prod_paga_empresa)
    .unionByName(ia_prod_copilot)
    .unionByName(ia_prod_nao_usa)
)

gravar_gold(
    gold_ia_produtividade_custo,
    "ia_produtividade_custo/"
)


# ============================================================
# 19. GOLD - IA PRIORIDADE ESTRATÉGICA
# ============================================================

gold_ia_prioridade = (
    df
    .filter(
        F.col(
            "ia_prioridade_empresa"
        ).isNotNull()
    )
    .groupBy(
        "edicao_pesquisa",
        "ia_prioridade_empresa"
    )
    .agg(
        F.count("*").alias(
            "quantidade"
        )
    )
)

janela_prioridade = Window.partitionBy(
    "edicao_pesquisa"
)

gold_ia_prioridade = (
    gold_ia_prioridade
    .withColumn(
        "respondentes_validos",
        F.sum(
            "quantidade"
        ).over(janela_prioridade)
    )
    .withColumn(
        "percentual",
        F.round(
            (
                F.col("quantidade")
                /
                F.col("respondentes_validos")
            ) * 100,
            2
        )
    )
)

gravar_gold(
    gold_ia_prioridade,
    "ia_prioridade/"
)


# ============================================================
# 20. GOLD - IA APLICAÇÕES / CASOS DE USO
# ============================================================

# Visão dos profissionais
aplic_prof_descentralizado = criar_metrica_binaria_ia(
    df,
    "ia_prof_uso_descentralizado",
    "Uso independente e descentralizado",
    "Aplicações - Profissionais"
)

aplic_prof_centralizado = criar_metrica_binaria_ia(
    df,
    "ia_prof_uso_centralizado",
    "Uso centralizado pela empresa",
    "Aplicações - Profissionais"
)

aplic_prof_copilot = criar_metrica_binaria_ia(
    df,
    "ia_prof_copilot",
    "Copilots para desenvolvedores",
    "Aplicações - Profissionais"
)

aplic_prof_produto_externo = criar_metrica_binaria_ia(
    df,
    "ia_prof_produto_externo",
    "Melhoria de produtos para clientes",
    "Aplicações - Profissionais"
)

aplic_prof_produto_interno = criar_metrica_binaria_ia(
    df,
    "ia_prof_produto_interno",
    "Melhoria de produtos internos",
    "Aplicações - Profissionais"
)

aplic_prof_principal_negocio = criar_metrica_binaria_ia(
    df,
    "ia_prof_principal_negocio",
    "IA como principal frente do negócio",
    "Aplicações - Profissionais"
)


# Visão dos gestores
aplic_gestor_descentralizado = criar_metrica_binaria_ia(
    df,
    "ia_gestor_uso_descentralizado",
    "Uso independente e descentralizado",
    "Aplicações - Gestores"
)

aplic_gestor_centralizado = criar_metrica_binaria_ia(
    df,
    "ia_gestor_uso_centralizado",
    "Uso centralizado pela empresa",
    "Aplicações - Gestores"
)

aplic_gestor_copilot = criar_metrica_binaria_ia(
    df,
    "ia_gestor_copilot",
    "Copilots para desenvolvedores",
    "Aplicações - Gestores"
)

aplic_gestor_produto_externo = criar_metrica_binaria_ia(
    df,
    "ia_gestor_produto_externo",
    "Melhoria de produtos para clientes",
    "Aplicações - Gestores"
)

aplic_gestor_produto_interno = criar_metrica_binaria_ia(
    df,
    "ia_gestor_produto_interno",
    "Melhoria de produtos internos",
    "Aplicações - Gestores"
)

aplic_gestor_principal_negocio = criar_metrica_binaria_ia(
    df,
    "ia_gestor_principal_negocio",
    "IA como principal frente do negócio",
    "Aplicações - Gestores"
)


gold_ia_aplicacoes = (
    aplic_prof_descentralizado
    .unionByName(aplic_prof_centralizado)
    .unionByName(aplic_prof_copilot)
    .unionByName(aplic_prof_produto_externo)
    .unionByName(aplic_prof_produto_interno)
    .unionByName(aplic_prof_principal_negocio)
    .unionByName(aplic_gestor_descentralizado)
    .unionByName(aplic_gestor_centralizado)
    .unionByName(aplic_gestor_copilot)
    .unionByName(aplic_gestor_produto_externo)
    .unionByName(aplic_gestor_produto_interno)
    .unionByName(aplic_gestor_principal_negocio)
)

gravar_gold(
    gold_ia_aplicacoes,
    "ia_aplicacoes/"
)


# ============================================================
# 21. GOLD - IA BARREIRAS
# ============================================================

barreira_casos_uso = criar_metrica_binaria_ia(
    df,
    "ia_barreira_casos_uso",
    "Falta de compreensão dos casos de uso",
    "Barreiras"
)

barreira_confiabilidade = criar_metrica_binaria_ia(
    df,
    "ia_barreira_confiabilidade",
    "Falta de confiabilidade / alucinações",
    "Barreiras"
)

barreira_regulamentacao = criar_metrica_binaria_ia(
    df,
    "ia_barreira_regulamentacao",
    "Incerteza regulatória",
    "Barreiras"
)

barreira_seguranca = criar_metrica_binaria_ia(
    df,
    "ia_barreira_seguranca",
    "Segurança e privacidade de dados",
    "Barreiras"
)

barreira_roi = criar_metrica_binaria_ia(
    df,
    "ia_barreira_roi",
    "ROI ainda não comprovado",
    "Barreiras"
)

barreira_dados = criar_metrica_binaria_ia(
    df,
    "ia_barreira_dados",
    "Dados da empresa não preparados",
    "Barreiras"
)

barreira_expertise = criar_metrica_binaria_ia(
    df,
    "ia_barreira_expertise",
    "Falta de expertise ou recursos",
    "Barreiras"
)

barreira_lideranca = criar_metrica_binaria_ia(
    df,
    "ia_barreira_lideranca",
    "Alta direção não vê valor/prioridade",
    "Barreiras"
)

barreira_pi = criar_metrica_binaria_ia(
    df,
    "ia_barreira_propriedade_intelectual",
    "Propriedade intelectual",
    "Barreiras"
)


gold_ia_barreiras = (
    barreira_casos_uso
    .unionByName(barreira_confiabilidade)
    .unionByName(barreira_regulamentacao)
    .unionByName(barreira_seguranca)
    .unionByName(barreira_roi)
    .unionByName(barreira_dados)
    .unionByName(barreira_expertise)
    .unionByName(barreira_lideranca)
    .unionByName(barreira_pi)
)

gravar_gold(
    gold_ia_barreiras,
    "ia_barreiras/"
)


# ============================================================
# 22. GOLD - IA IMPACTO NO NEGÓCIO
# ============================================================

# A pergunta específica de resultados/impacto só existe em 2025.
gold_ia_impacto = (
    df
    .filter(
        F.col(
            "ia_resultado_empresa"
        ).isNotNull()
    )
    .groupBy(
        "edicao_pesquisa",
        "ia_resultado_empresa"
    )
    .agg(
        F.count("*").alias(
            "quantidade"
        )
    )
)

janela_impacto = Window.partitionBy(
    "edicao_pesquisa"
)

gold_ia_impacto = (
    gold_ia_impacto
    .withColumn(
        "respondentes_validos",
        F.sum(
            "quantidade"
        ).over(janela_impacto)
    )
    .withColumn(
        "percentual",
        F.round(
            (
                F.col("quantidade")
                /
                F.col("respondentes_validos")
            ) * 100,
            2
        )
    )
)

gravar_gold(
    gold_ia_impacto,
    "ia_impacto_negocio/"
)


# ============================================================
# 23. GOLD - INDICADORES EXECUTIVOS DE IA
# ============================================================

# Esta tabela facilita a criação de cartões no Power BI.
# Cada linha corresponde a um ano.

gold_ia_executivo = (
    df
    .groupBy(
        "edicao_pesquisa"
    )
    .agg(

        # Adoção
        F.sum(
            "ia_adota_produtividade"
        ).alias(
            "usuarios_ia"
        ),

        F.count(
            "ia_adota_produtividade"
        ).alias(
            "respondentes_validos_adocao"
        ),

        # Custo / produtividade
        F.sum(
            "ia_prod_paga_empresa"
        ).alias(
            "empresa_paga_ia"
        ),

        F.count(
            "ia_prod_paga_empresa"
        ).alias(
            "respondentes_validos_empresa_paga"
        ),

        F.sum(
            "ia_prod_paga_proprio"
        ).alias(
            "profissional_paga_ia"
        ),

        F.count(
            "ia_prod_paga_proprio"
        ).alias(
            "respondentes_validos_profissional_paga"
        ),

        F.sum(
            "ia_prod_gratuita"
        ).alias(
            "usa_ia_gratuita"
        ),

        F.count(
            "ia_prod_gratuita"
        ).alias(
            "respondentes_validos_ia_gratuita"
        ),

        F.sum(
            "ia_prod_copilot"
        ).alias(
            "usa_copilot"
        ),

        F.count(
            "ia_prod_copilot"
        ).alias(
            "respondentes_validos_copilot"
        ),

        # Barreiras
        F.sum(
            "ia_barreira_roi"
        ).alias(
            "roi_nao_comprovado"
        ),

        F.count(
            "ia_barreira_roi"
        ).alias(
            "respondentes_validos_roi"
        ),

        F.sum(
            "ia_barreira_expertise"
        ).alias(
            "falta_expertise_recursos"
        ),

        F.count(
            "ia_barreira_expertise"
        ).alias(
            "respondentes_validos_expertise"
        ),

        F.sum(
            "ia_barreira_dados"
        ).alias(
            "dados_nao_preparados"
        ),

        F.count(
            "ia_barreira_dados"
        ).alias(
            "respondentes_validos_dados"
        ),

        F.sum(
            "ia_barreira_seguranca"
        ).alias(
            "seguranca_privacidade"
        ),

        F.count(
            "ia_barreira_seguranca"
        ).alias(
            "respondentes_validos_seguranca"
        )
    )
)


gold_ia_executivo = (
    gold_ia_executivo

    .withColumn(
        "percentual_adocao_ia",
        F.round(
            (
                F.col("usuarios_ia")
                /
                F.col("respondentes_validos_adocao")
            ) * 100,
            2
        )
    )

    .withColumn(
        "percentual_empresa_paga_ia",
        F.round(
            (
                F.col("empresa_paga_ia")
                /
                F.col("respondentes_validos_empresa_paga")
            ) * 100,
            2
        )
    )

    .withColumn(
        "percentual_profissional_paga_ia",
        F.round(
            (
                F.col("profissional_paga_ia")
                /
                F.col("respondentes_validos_profissional_paga")
            ) * 100,
            2
        )
    )

    .withColumn(
        "percentual_ia_gratuita",
        F.round(
            (
                F.col("usa_ia_gratuita")
                /
                F.col("respondentes_validos_ia_gratuita")
            ) * 100,
            2
        )
    )

    .withColumn(
        "percentual_copilot",
        F.round(
            (
                F.col("usa_copilot")
                /
                F.col("respondentes_validos_copilot")
            ) * 100,
            2
        )
    )

    .withColumn(
        "percentual_roi_nao_comprovado",
        F.round(
            (
                F.col("roi_nao_comprovado")
                /
                F.col("respondentes_validos_roi")
            ) * 100,
            2
        )
    )

    .withColumn(
        "percentual_falta_expertise_recursos",
        F.round(
            (
                F.col("falta_expertise_recursos")
                /
                F.col("respondentes_validos_expertise")
            ) * 100,
            2
        )
    )

    .withColumn(
        "percentual_dados_nao_preparados",
        F.round(
            (
                F.col("dados_nao_preparados")
                /
                F.col("respondentes_validos_dados")
            ) * 100,
            2
        )
    )

    .withColumn(
        "percentual_seguranca_privacidade",
        F.round(
            (
                F.col("seguranca_privacidade")
                /
                F.col("respondentes_validos_seguranca")
            ) * 100,
            2
        )
    )
)

gravar_gold(
    gold_ia_executivo,
    "ia_executivo/",
    particionar=False
)


# ============================================================
# 24. VALIDAÇÕES FINAIS
# ============================================================

print("============================================")
print("GOLD - RESUMO GERAL")
print("============================================")

gold_resumo.orderBy(
    "edicao_pesquisa"
).show(
    truncate=False
)


print("============================================")
print("GOLD - ADOÇÃO DE IA")
print("============================================")

gold_adocao_ia.orderBy(
    "edicao_pesquisa"
).show(
    truncate=False
)


print("============================================")
print("GOLD - IA PRODUTIVIDADE / CUSTO")
print("============================================")

gold_ia_produtividade_custo.orderBy(
    "edicao_pesquisa",
    F.desc("percentual")
).show(
    100,
    truncate=False
)


print("============================================")
print("GOLD - IA PRIORIDADE")
print("============================================")

gold_ia_prioridade.orderBy(
    "edicao_pesquisa",
    F.desc("percentual")
).show(
    100,
    truncate=False
)


print("============================================")
print("GOLD - IA BARREIRAS")
print("============================================")

gold_ia_barreiras.orderBy(
    "edicao_pesquisa",
    F.desc("percentual")
).show(
    100,
    truncate=False
)


print("============================================")
print("GOLD - IA IMPACTO NO NEGÓCIO")
print("============================================")

gold_ia_impacto.orderBy(
    "edicao_pesquisa",
    F.desc("percentual")
).show(
    100,
    truncate=False
)


print("============================================")
print("GOLD - IA EXECUTIVO")
print("============================================")

gold_ia_executivo.orderBy(
    "edicao_pesquisa"
).show(
    truncate=False
)


print("============================================")
print("CAMADA GOLD CRIADA COM SUCESSO")
print("============================================")


# ============================================================
# 25. FINALIZAÇÃO DO JOB
# ============================================================

job.commit()