import sys

from pyspark.context import SparkContext
from pyspark.sql import functions as F

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

job.init(
    args["JOB_NAME"],
    args
)


# ============================================================
# 2. CAMINHOS DO S3
# ============================================================

BASE = "s3://lab-300271615875"


# -----------------------------
# BRONZE
# -----------------------------

CAMINHO_2023 = (
    f"{BASE}/Bronze/Data_2023_2024/*.csv"
)

CAMINHO_2024 = (
    f"{BASE}/Bronze/Data_2024_2025/*.csv"
)

CAMINHO_2025 = (
    f"{BASE}/Bronze/Data_2025_2026/*.csv"
)


# -----------------------------
# SILVER
# -----------------------------

CAMINHO_SILVER = (
    f"{BASE}/Silver/State_of_Data_Harmonizado/"
)


# ============================================================
# 3. FUNÇÃO DE LEITURA DOS CSV
# ============================================================

def ler_csv(caminho):

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .csv(caminho)
    )

    return df


# ============================================================
# 4. LEITURA DAS BASES BRONZE
# ============================================================

print("============================================")
print("INICIANDO LEITURA DAS BASES")
print("============================================")


df_2023 = ler_csv(
    CAMINHO_2023
)

df_2024 = ler_csv(
    CAMINHO_2024
)

df_2025 = ler_csv(
    CAMINHO_2025
)


# ============================================================
# 5. VALIDAÇÃO INICIAL
# ============================================================

qtd_2023 = df_2023.count()

qtd_2024 = df_2024.count()

qtd_2025 = df_2025.count()


print("============================================")
print("REGISTROS ENCONTRADOS")
print("============================================")

print(
    "BASE 2023-2024:",
    qtd_2023
)

print(
    "BASE 2024-2025:",
    qtd_2024
)

print(
    "BASE 2025-2026:",
    qtd_2025
)


# ============================================================
# 6. MAPEAMENTO DA BASE 2023
# ============================================================

mapa_2023 = {

    "id":
        "('P0', 'id')",

    "idade":
        "('P1_a ', 'Idade')",

    "faixa_idade":
        "('P1_a_1 ', 'Faixa idade')",

    "genero":
        "('P1_b ', 'Genero')",

    "cor_raca_etnia":
        "('P1_c ', 'Cor/raca/etnia')",

    "pcd":
        "('P1_d ', 'PCD')",

    "uf":
        "('P1_i_1 ', 'uf onde mora')",

    "regiao":
        "('P1_i_2 ', 'Regiao onde mora')",

    "nivel_ensino":
        "('P1_l ', 'Nivel de Ensino')",

    "area_formacao":
        "('P1_m ', 'Área de Formação')",

    "situacao_trabalho":
        "('P2_a ', 'Qual sua situação atual de trabalho?')",

    "setor":
        "('P2_b ', 'Setor')",

    "cargo_atual":
        "('P2_f ', 'Cargo Atual')",

    "nivel_profissional":
        "('P2_g ', 'Nivel')",

    "faixa_salarial":
        "('P2_h ', 'Faixa salarial')",

    "tempo_experiencia_dados":
        "('P2_i ', 'Quanto tempo de experiência na área de dados você tem?')",

    "modelo_trabalho":
        "('P2_r ', 'Atualmente qual a sua forma de trabalho?')",

    "linguagem_preferida":
        "('P4_f ', 'Entre as linguagens listadas abaixo, qual é a sua preferida?')",

    "cloud_preferida":
        "('P4_i ', 'Cloud preferida')",

    "ia_prioridade_empresa":
        "('P3_e ', 'AI Generativa é uma prioridade em sua empresa?')",

    "usa_ia_trabalho":
        "('P4_m ', 'Utiliza ChatGPT ou LLMs no trabalho?')",

    "usa_sql":
        "('P4_d_1 ', 'SQL')",

    "usa_python":
        "('P4_d_3 ', 'Python')",

    "usa_aws":
        "('P4_h_2 ', 'Amazon Web Services (AWS)')",

    "usa_powerbi":
        "('P4_j_1 ', 'Microsoft PowerBI')"
}


# ============================================================
# 7. MAPEAMENTO DA BASE 2024
# ============================================================

mapa_2024 = {

    "id":
        "0.a_token",

    "idade":
        "1.a_idade",

    "faixa_idade":
        "1.a.1_faixa_idade",

    "genero":
        "1.b_genero",

    "cor_raca_etnia":
        "1.c_cor/raca/etnia",

    "pcd":
        "1.d_pcd",

    "uf":
        "1.i.1_uf_onde_mora",

    "regiao":
        "1.i.2_regiao_onde_mora",

    "nivel_ensino":
        "1.l_nivel_de_ensino",

    "area_formacao":
        "1.m_área_de_formação",

    "situacao_trabalho":
        "2.a_situação_de_trabalho",

    "setor":
        "2.b_setor",

    "cargo_atual":
        "2.f_cargo_atual",

    "nivel_profissional":
        "2.g_nivel",

    "faixa_salarial":
        "2.h_faixa_salarial",

    "tempo_experiencia_dados":
        "2.i_tempo_de_experiencia_em_dados",

    "modelo_trabalho":
        "2.r_modelo_de_trabalho_atual",

    "linguagem_preferida":
        "4.f_linguagem_preferida",

    "cloud_preferida":
        "4.i_cloud_preferida",

    "ia_prioridade_empresa":
        "3.e_ai_generativa_e_llm_é_uma_prioridade?",

    "usa_ia_trabalho":
        "4.m_usa_chatgpt_ou_copilot_no_trabalho?",

    "usa_sql":
        "4.d.1_SQL",

    "usa_python":
        "4.d.3_Python",

    "usa_aws":
        "4.h.1_Amazon Web Services (AWS)",

    "usa_powerbi":
        "4.j.1_Microsoft PowerBI"
}


# ============================================================
# 8. MAPEAMENTO DA BASE 2025-2026
# ============================================================

mapa_2025 = {

    "id":
        "0.a_token",

    "idade":
        "1.a_idade",

    "faixa_idade":
        "1.a.1_faixa_idade",

    "genero":
        "1.b_genero",

    "cor_raca_etnia":
        "1.c_cor/raca/etnia",

    "pcd":
        "1.d_pcd",

    "uf":
        "1.i.1_uf_onde_mora",

    "regiao":
        "1.i.2_regiao_onde_mora",

    "nivel_ensino":
        "1.l_nivel_de_ensino",

    "area_formacao":
        "1.m_área_de_formação",

    "situacao_trabalho":
        "2.a_situação_de_trabalho",

    "setor":
        "2.b_setor",

    "cargo_atual":
        "2.f_cargo_atual",

    "nivel_profissional":
        "2.g_nivel",

    "faixa_salarial":
        "2.h_faixa_salarial",

    "tempo_experiencia_dados":
        "2.i_tempo_de_experiencia_em_dados",

    "modelo_trabalho":
        "2.q_modelo_de_trabalho_atual",

    "linguagem_preferida":
        "4.c_linguagem_preferida",

    "cloud_preferida":
        "4.f_cloud_preferida",

    "ia_prioridade_empresa":
        "3.e_ai_generativa_e_llm_é_uma_prioridade?",

    "usa_ia_trabalho":
        "4.j_usa_chatgpt_ou_copilot_no_trabalho?",

    "usa_sql":
        "4.c.1_SQL",

    "usa_python":
        "4.c.3_Python",

    "usa_aws":
        "4.e.1_Amazon Web Services (AWS)",

    "usa_powerbi":
        "4.g.1_Microsoft PowerBI"
}


# ============================================================
# 9. FUNÇÃO PARA VALIDAR AS COLUNAS
# ============================================================

def validar_colunas(
    df,
    mapa,
    nome_base
):

    colunas_existentes = set(
        df.columns
    )

    colunas_faltantes = []

    for nome_original in mapa.values():

        if nome_original not in colunas_existentes:

            colunas_faltantes.append(
                nome_original
            )

    if len(colunas_faltantes) > 0:

        print("============================================")
        print(
            f"ERRO - COLUNAS AUSENTES NA BASE {nome_base}"
        )
        print("============================================")

        for coluna in colunas_faltantes:

            print(
                "COLUNA NÃO ENCONTRADA:",
                coluna
            )

        raise Exception(
            f"A base {nome_base} possui colunas ausentes."
        )

    print(
        f"Validação da base {nome_base}: OK"
    )


# ============================================================
# 10. VALIDAR MAPEAMENTOS
# ============================================================

validar_colunas(
    df_2023,
    mapa_2023,
    "2023-2024"
)

validar_colunas(
    df_2024,
    mapa_2024,
    "2024-2025"
)

validar_colunas(
    df_2025,
    mapa_2025,
    "2025-2026"
)


# ============================================================
# 11. FUNÇÃO DE PADRONIZAÇÃO
# ============================================================

def padronizar(
    df,
    mapa,
    edicao
):

    colunas = []

    for nome_final, nome_original in mapa.items():

        coluna = (
            F.col(
                f"`{nome_original}`"
            )
            .alias(
                nome_final
            )
        )

        colunas.append(
            coluna
        )


    resultado = df.select(
        *colunas
    )


    # Adiciona o ano/edição da pesquisa

    resultado = (
        resultado
        .withColumn(
            "edicao_pesquisa",
            F.lit(edicao)
        )
    )


    return resultado


# ============================================================
# 12. PADRONIZAR AS TRÊS BASES
# ============================================================

silver_2023 = padronizar(
    df_2023,
    mapa_2023,
    "2023-2024"
)

silver_2024 = padronizar(
    df_2024,
    mapa_2024,
    "2024-2025"
)

silver_2025 = padronizar(
    df_2025,
    mapa_2025,
    "2025-2026"
)


# ============================================================
# 13. FUNÇÃO PARA LIMPEZA DE TEXTOS
# ============================================================

def limpar_textos(df):

    for campo, tipo in df.dtypes:

        if tipo == "string":

            df = df.withColumn(
                campo,
                F.when(
                    F.trim(
                        F.col(campo)
                    ) == "",
                    None
                )
                .otherwise(
                    F.trim(
                        F.col(campo)
                    )
                )
            )

    return df


silver_2023 = limpar_textos(
    silver_2023
)

silver_2024 = limpar_textos(
    silver_2024
)

silver_2025 = limpar_textos(
    silver_2025
)


# ============================================================
# 14. TRATAMENTO DA IDADE
# ============================================================

def tratar_idade(df):

    return (
        df
        .withColumn(
            "idade",
            F.col("idade")
            .cast("double")
            .cast("int")
        )
    )


silver_2023 = tratar_idade(
    silver_2023
)

silver_2024 = tratar_idade(
    silver_2024
)

silver_2025 = tratar_idade(
    silver_2025
)


# ============================================================
# 15. TRATAMENTO DOS INDICADORES DE TECNOLOGIA
# ============================================================

INDICADORES = [
    "usa_sql",
    "usa_python",
    "usa_aws",
    "usa_powerbi"
]


def tratar_indicadores(df):

    for coluna in INDICADORES:

        df = (
            df
            .withColumn(
                coluna,
                F.when(
                    F.col(coluna).isin(
                        "1",
                        "1.0"
                    ),
                    F.lit(1)
                )
                .when(
                    F.col(coluna).isin(
                        "0",
                        "0.0"
                    ),
                    F.lit(0)
                )
                .otherwise(
                    F.col(coluna)
                    .cast("int")
                )
            )
        )

    return df


silver_2023 = tratar_indicadores(
    silver_2023
)

silver_2024 = tratar_indicadores(
    silver_2024
)

silver_2025 = tratar_indicadores(
    silver_2025
)


# ============================================================
# 16. UNIÃO DAS TRÊS PESQUISAS
# ============================================================

silver = (
    silver_2023
    .unionByName(
        silver_2024
    )
    .unionByName(
        silver_2025
    )
)


# ============================================================
# 17. VALIDAÇÃO DO RESULTADO
# ============================================================

print("============================================")
print("CAMADA SILVER")
print("============================================")


total_silver = silver.count()


print(
    "TOTAL DE REGISTROS:",
    total_silver
)


print("============================================")
print("REGISTROS POR EDIÇÃO")
print("============================================")


(
    silver
    .groupBy(
        "edicao_pesquisa"
    )
    .count()
    .orderBy(
        "edicao_pesquisa"
    )
    .show(
        truncate=False
    )
)


# ============================================================
# 18. VISUALIZAÇÃO DE ALGUNS REGISTROS
# ============================================================

print("============================================")
print("AMOSTRA DOS DADOS TRATADOS")
print("============================================")


(
    silver
    .select(
        "edicao_pesquisa",
        "idade",
        "genero",
        "uf",
        "regiao",
        "cargo_atual",
        "nivel_profissional",
        "faixa_salarial",
        "modelo_trabalho",
        "linguagem_preferida",
        "cloud_preferida",
        "usa_ia_trabalho"
    )
    .show(
        20,
        truncate=False
    )
)


# ============================================================
# 19. VERIFICAÇÃO DE NULOS IMPORTANTES
# ============================================================

print("============================================")
print("VERIFICAÇÃO DE CAMPOS NULOS")
print("============================================")


silver.select(

    F.sum(
        F.col("idade").isNull().cast("int")
    ).alias(
        "idade_nulos"
    ),

    F.sum(
        F.col("genero").isNull().cast("int")
    ).alias(
        "genero_nulos"
    ),

    F.sum(
        F.col("cargo_atual").isNull().cast("int")
    ).alias(
        "cargo_nulos"
    ),

    F.sum(
        F.col("nivel_profissional").isNull().cast("int")
    ).alias(
        "nivel_nulos"
    ),

    F.sum(
        F.col("faixa_salarial").isNull().cast("int")
    ).alias(
        "salario_nulos"
    )

).show()


# ============================================================
# 20. GRAVAÇÃO DA CAMADA SILVER EM PARQUET
# ============================================================

print("============================================")
print("INICIANDO GRAVAÇÃO DA SILVER")
print("============================================")


(
    silver
    .write
    .mode(
        "overwrite"
    )
    .partitionBy(
        "edicao_pesquisa"
    )
    .parquet(
        CAMINHO_SILVER
    )
)


# ============================================================
# 21. CONFIRMAÇÃO
# ============================================================

print("============================================")
print("PROCESSAMENTO CONCLUÍDO")
print("============================================")

print(
    "CAMINHO SILVER:"
)

print(
    CAMINHO_SILVER
)

print(
    "TOTAL DE REGISTROS:",
    total_silver
)


# ============================================================
# 22. FINALIZAÇÃO DO GLUE JOB
# ============================================================

job.commit()