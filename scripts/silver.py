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
job.init(args["JOB_NAME"], args)


# ============================================================
# 2. CAMINHOS DO S3
# ============================================================

BASE = "s3://lab-300271615875"

# Pastas corrigidas: cada pasta corresponde a um único ano.
CAMINHO_2023 = f"{BASE}/Bronze/Data_2023/*.csv"
CAMINHO_2024 = f"{BASE}/Bronze/Data_2024/*.csv"
CAMINHO_2025 = f"{BASE}/Bronze/Data_2025/*.csv"

CAMINHO_SILVER = (
    f"{BASE}/Silver/State_of_Data_Harmonizado/"
)


# ============================================================
# 3. FUNÇÃO DE LEITURA DOS CSV
# ============================================================

def ler_csv(caminho):
    return (
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


# ============================================================
# 4. LEITURA DAS BASES BRONZE
# ============================================================

print("============================================")
print("INICIANDO LEITURA DAS BASES")
print("============================================")

df_2023 = ler_csv(CAMINHO_2023)
df_2024 = ler_csv(CAMINHO_2024)
df_2025 = ler_csv(CAMINHO_2025)


# ============================================================
# 5. VALIDAÇÃO INICIAL
# ============================================================

qtd_2023 = df_2023.count()
qtd_2024 = df_2024.count()
qtd_2025 = df_2025.count()

print("============================================")
print("REGISTROS ENCONTRADOS")
print("============================================")
print("BASE 2023:", qtd_2023)
print("BASE 2024:", qtd_2024)
print("BASE 2025:", qtd_2025)


# ============================================================
# 6. MAPEAMENTO DA BASE 2023
# ============================================================

mapa_2023 = {

    # --------------------------------------------------------
    # PERFIL
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # TRABALHO / MERCADO
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # TECNOLOGIAS
    # --------------------------------------------------------
    "linguagem_preferida":
        "('P4_f ', 'Entre as linguagens listadas abaixo, qual é a sua preferida?')",

    "cloud_preferida":
        "('P4_i ', 'Cloud preferida')",

    "usa_sql":
        "('P4_d_1 ', 'SQL')",

    "usa_python":
        "('P4_d_3 ', 'Python')",

    "usa_aws":
        "('P4_h_2 ', 'Amazon Web Services (AWS)')",

    "usa_powerbi":
        "('P4_j_1 ', 'Microsoft PowerBI')",

    # --------------------------------------------------------
    # IA - PRIORIDADE
    # --------------------------------------------------------
    "ia_prioridade_empresa":
        "('P3_e ', 'AI Generativa é uma prioridade em sua empresa?')",

    # --------------------------------------------------------
    # IA - USO NA VISÃO DE GESTORES
    # --------------------------------------------------------
    "ia_uso_gestores":
        "('P3_f ', 'Tipos de uso de AI Generativa e LLMs na empresa')",

    "ia_gestor_uso_descentralizado":
        "('P3_f_1 ', 'Colaboradores usando AI generativa de forma independente e descentralizada')",

    "ia_gestor_uso_centralizado":
        "('P3_f_2 ', 'Direcionamento centralizado do uso de AI generativa')",

    "ia_gestor_copilot":
        "('P3_f_3 ', 'Desenvolvedores utilizando Copilots')",

    "ia_gestor_produto_externo":
        "('P3_f_4 ', 'AI Generativa e LLMs para melhorar produtos externos')",

    "ia_gestor_produto_interno":
        "('P3_f_5 ', 'AI Generativa e LLMs para melhorar produtos internos para os colaboradores')",

    "ia_gestor_principal_negocio":
        "('P3_f_6 ', 'IA Generativa e LLMs como principal frente do negócio')",

    "ia_gestor_nao_prioridade":
        "('P3_f_7 ', 'IA Generativa e LLMs não é prioridade')",

    "ia_gestor_nao_sabe":
        "('P3_f_8 ', 'Não sei opinar sobre o uso de IA Generativa e LLMs na empresa')",

    # Essa pergunta específica só existe em 2025.
    "ia_resultado_empresa":
        None,

    # --------------------------------------------------------
    # IA - BARREIRAS
    # --------------------------------------------------------
    "ia_barreiras_empresa":
        "('P3_g ', 'Motivos que levam a empresa a não usar AI Genrativa e LLMs')",

    "ia_barreira_casos_uso":
        "('P3_g_1 ', 'Falta de compreensão dos casos de uso')",

    "ia_barreira_confiabilidade":
        "('P3_g_2 ', 'Falta de confiabilidade das saídas (alucinação dos modelos)')",

    "ia_barreira_regulamentacao":
        "('P3_g_3 ', 'Incerteza em relação a regulamentação')",

    "ia_barreira_seguranca":
        "('P3_g_4 ', 'Preocupações com segurança e privacidade de dados')",

    "ia_barreira_roi":
        "('P3_g_5 ', 'Retorno sobre investimento (ROI) não comprovado de IA Generativa')",

    "ia_barreira_dados":
        "('P3_g_6 ', 'Dados da empresa não estão prontos para uso de IA Generativa')",

    "ia_barreira_expertise":
        "('P3_g_7 ', 'Falta de expertise ou falta de recursos')",

    "ia_barreira_lideranca":
        "('P3_g_8 ', 'Alta direção da empresa não vê valor ou não vê como prioridade')",

    "ia_barreira_propriedade_intelectual":
        "('P3_g_9 ', 'Preocupações com propriedade intelectual')",

    # --------------------------------------------------------
    # IA - USO NA VISÃO DOS PROFISSIONAIS
    # --------------------------------------------------------
    "ia_uso_profissionais":
        "('P4_l ', 'Qual o tipo de uso de AI Generativa e LLMs na empresa')",

    "ia_prof_uso_descentralizado":
        "('P4_l_1 ', 'Colaboradores usando AI generativa de forma independente e descentralizada')",

    "ia_prof_uso_centralizado":
        "('P4_l_2 ', 'Direcionamento centralizado do uso de AI generativa')",

    "ia_prof_copilot":
        "('P4_l_3 ', 'Desenvolvedores utilizando Copilots')",

    "ia_prof_produto_externo":
        "('P4_l_4 ', 'AI Generativa e LLMs para melhorar produtos externos para os clientes finais')",

    "ia_prof_produto_interno":
        "('P4_l_5 ', 'AI Generativa e LLMs para melhorar produtos internos para os colaboradores')",

    "ia_prof_principal_negocio":
        "('P4_l_6 ', 'IA Generativa e LLMs como principal frente do negócio')",

    "ia_prof_nao_prioridade":
        "('P4_l_7 ', 'IA Generativa e LLMs não é prioridade')",

    "ia_prof_nao_sabe":
        "('P4_l_8 ', 'Não sei opinar sobre o uso de IA Generativa e LLMs na empresa')",

    # --------------------------------------------------------
    # IA - PRODUTIVIDADE / CUSTO
    # --------------------------------------------------------
    "usa_ia_trabalho":
        "('P4_m ', 'Utiliza ChatGPT ou LLMs no trabalho?')",

    "ia_prod_nao_usa":
        "('P4_m_1 ', 'Não uso soluções de AI Generativa com foco em produtividade')",

    "ia_prod_gratuita":
        "('P4_m_2 ', 'Uso soluções gratuitas de AI Generativa com foco em produtividade')",

    "ia_prod_paga_proprio":
        "('P4_m_3 ', 'Uso e pago pelas soluções de AI Generativa com foco em produtividade')",

    "ia_prod_paga_empresa":
        "('P4_m_4 ', 'A empresa que trabalho paga pelas soluções de AI Generativa com foco em produtividade')",

    "ia_prod_copilot":
        "('P4_m_5 ', 'Uso soluções do tipo Copilot')"
}


# ============================================================
# 7. MAPEAMENTO DA BASE 2024
# ============================================================

mapa_2024 = {

    # --------------------------------------------------------
    # PERFIL
    # --------------------------------------------------------
    "id": "0.a_token",
    "idade": "1.a_idade",
    "faixa_idade": "1.a.1_faixa_idade",
    "genero": "1.b_genero",
    "cor_raca_etnia": "1.c_cor/raca/etnia",
    "pcd": "1.d_pcd",
    "uf": "1.i.1_uf_onde_mora",
    "regiao": "1.i.2_regiao_onde_mora",
    "nivel_ensino": "1.l_nivel_de_ensino",
    "area_formacao": "1.m_área_de_formação",

    # --------------------------------------------------------
    # TRABALHO / MERCADO
    # --------------------------------------------------------
    "situacao_trabalho": "2.a_situação_de_trabalho",
    "setor": "2.b_setor",
    "cargo_atual": "2.f_cargo_atual",
    "nivel_profissional": "2.g_nivel",
    "faixa_salarial": "2.h_faixa_salarial",
    "tempo_experiencia_dados": "2.i_tempo_de_experiencia_em_dados",
    "modelo_trabalho": "2.r_modelo_de_trabalho_atual",

    # --------------------------------------------------------
    # TECNOLOGIAS
    # --------------------------------------------------------
    "linguagem_preferida": "4.f_linguagem_preferida",
    "cloud_preferida": "4.i_cloud_preferida",
    "usa_sql": "4.d.1_SQL",
    "usa_python": "4.d.3_Python",
    "usa_aws": "4.h.1_Amazon Web Services (AWS)",
    "usa_powerbi": "4.j.1_Microsoft PowerBI",

    # --------------------------------------------------------
    # IA - PRIORIDADE
    # --------------------------------------------------------
    "ia_prioridade_empresa":
        "3.e_ai_generativa_e_llm_é_uma_prioridade?",

    # --------------------------------------------------------
    # IA - USO NA VISÃO DE GESTORES
    # --------------------------------------------------------
    "ia_uso_gestores":
        "3.f_tipo_de_uso_de_ai_generativa_e_llm_na_empresa",

    "ia_gestor_uso_descentralizado":
        "3.f.1 Colaboradores usando AI generativa de forma independente e descentralizada",

    "ia_gestor_uso_centralizado":
        "3.f.2 Direcionamento centralizado do uso de AI generativa",

    "ia_gestor_copilot":
        "3.f.3 Desenvolvedores utilizando Copilots",

    "ia_gestor_produto_externo":
        "3.f.4 AI Generativa e LLMs para melhorar produtos externos para os clientes finais",

    "ia_gestor_produto_interno":
        "3.f.5 AI Generativa e LLMs para melhorar produtos internos para os colaboradores",

    "ia_gestor_principal_negocio":
        "3.f.6 IA Generativa e LLMs como principal frente do negócio",

    "ia_gestor_nao_prioridade":
        "3.f.7 IA Generativa e LLMs não é prioridade",

    "ia_gestor_nao_sabe":
        "3.f.8 Não sei opinar sobre o uso de IA Generativa e LLMs na empresa",

    # Essa pergunta específica só existe em 2025.
    "ia_resultado_empresa":
        None,

    # --------------------------------------------------------
    # IA - BARREIRAS
    # --------------------------------------------------------
    "ia_barreiras_empresa":
        "3.g_motivos_para_não_usar_ai_generativa_e_llm",

    "ia_barreira_casos_uso":
        "3.g.1 Falta de compreensão dos casos de uso",

    "ia_barreira_confiabilidade":
        "3.g.2 Falta de confiabilidade das saídas (alucinação dos modelos)",

    "ia_barreira_regulamentacao":
        "3.g.3 Incerteza em relação a regulamentação",

    "ia_barreira_seguranca":
        "3.g.4 Preocupações com segurança e privacidade de dados",

    "ia_barreira_roi":
        "3.g.5 Retorno sobre investimento (ROI) não comprovado de IA Generativa",

    "ia_barreira_dados":
        "3.g.6 Dados da empresa não estão prontos para uso de IA Generativa",

    "ia_barreira_expertise":
        "3.g.7 Falta de expertise ou falta de recursos",

    "ia_barreira_lideranca":
        "3.g.8 Alta direção da empresa não vê valor ou não vê como prioridade",

    "ia_barreira_propriedade_intelectual":
        "3.g.9 Preocupações com propriedade intelectual",

    # --------------------------------------------------------
    # IA - USO NA VISÃO DOS PROFISSIONAIS
    # --------------------------------------------------------
    "ia_uso_profissionais":
        "4.l_tipo_de_uso_de_ai_generativa_e_llm_na_empresa",

    "ia_prof_uso_descentralizado":
        "4.l.1 Colaboradores usando AI generativa de forma independente e descentralizada",

    "ia_prof_uso_centralizado":
        "4.l.2 Direcionamento centralizado do uso de AI generativa",

    "ia_prof_copilot":
        "4.l.3 Desenvolvedores utilizando Copilots",

    "ia_prof_produto_externo":
        "4.l.4 AI Generativa e LLMs para melhorar produtos externos para os clientes finais",

    "ia_prof_produto_interno":
        "4.l.5 AI Generativa e LLMs para melhorar produtos internos para os colaboradores",

    "ia_prof_principal_negocio":
        "4.l.6 IA Generativa e LLMs como principal frente do negócio",

    "ia_prof_nao_prioridade":
        "4.l.7 IA Generativa e LLMs não é prioridade",

    "ia_prof_nao_sabe":
        "4.l.8 Não sei opinar sobre o uso de IA Generativa e LLMs na empresa",

    # --------------------------------------------------------
    # IA - PRODUTIVIDADE / CUSTO
    # --------------------------------------------------------
    "usa_ia_trabalho":
        "4.m_usa_chatgpt_ou_copilot_no_trabalho?",

    "ia_prod_nao_usa":
        "4.m.1 Não uso soluções de AI Generativa com foco em produtividade",

    "ia_prod_gratuita":
        "4.m.2 Uso soluções gratuitas de AI Generativa com foco em produtividade",

    "ia_prod_paga_proprio":
        "4.m.3 Uso e pago pelas soluções de AI Generativa com foco em produtividade",

    "ia_prod_paga_empresa":
        "4.m.4 A empresa que trabalho paga pelas soluções de AI Generativa com foco em produtividade",

    "ia_prod_copilot":
        "4.m.5 Uso soluções do tipo Copilot"
}


# ============================================================
# 8. MAPEAMENTO DA BASE 2025
# ============================================================

mapa_2025 = {

    # --------------------------------------------------------
    # PERFIL
    # --------------------------------------------------------
    "id": "0.a_token",
    "idade": "1.a_idade",
    "faixa_idade": "1.a.1_faixa_idade",
    "genero": "1.b_genero",
    "cor_raca_etnia": "1.c_cor/raca/etnia",
    "pcd": "1.d_pcd",
    "uf": "1.i.1_uf_onde_mora",
    "regiao": "1.i.2_regiao_onde_mora",
    "nivel_ensino": "1.l_nivel_de_ensino",
    "area_formacao": "1.m_área_de_formação",

    # --------------------------------------------------------
    # TRABALHO / MERCADO
    # --------------------------------------------------------
    "situacao_trabalho": "2.a_situação_de_trabalho",
    "setor": "2.b_setor",
    "cargo_atual": "2.f_cargo_atual",
    "nivel_profissional": "2.g_nivel",
    "faixa_salarial": "2.h_faixa_salarial",
    "tempo_experiencia_dados": "2.i_tempo_de_experiencia_em_dados",
    "modelo_trabalho": "2.q_modelo_de_trabalho_atual",

    # --------------------------------------------------------
    # TECNOLOGIAS
    # --------------------------------------------------------
    "linguagem_preferida": "4.c_linguagem_preferida",
    "cloud_preferida": "4.f_cloud_preferida",
    "usa_sql": "4.c.1_SQL",
    "usa_python": "4.c.3_Python",
    "usa_aws": "4.e.1_Amazon Web Services (AWS)",
    "usa_powerbi": "4.g.1_Microsoft PowerBI",

    # --------------------------------------------------------
    # IA - PRIORIDADE
    # --------------------------------------------------------
    "ia_prioridade_empresa":
        "3.e_ai_generativa_e_llm_é_uma_prioridade?",

    # --------------------------------------------------------
    # IA - USO NA VISÃO DE GESTORES
    # --------------------------------------------------------
    "ia_uso_gestores":
        "3.f_tipo_de_uso_de_ai_generativa_e_llm_na_empresa",

    "ia_gestor_uso_descentralizado":
        "3.f.1 Colaboradores usando AI generativa de forma independente e descentralizada",

    "ia_gestor_uso_centralizado":
        "3.f.2 Direcionamento centralizado do uso de AI generativa",

    "ia_gestor_copilot":
        "3.f.3 Desenvolvedores utilizando Copilots",

    "ia_gestor_produto_externo":
        "3.f.4 AI Generativa e LLMs para melhorar produtos externos para os clientes finais",

    "ia_gestor_produto_interno":
        "3.f.5 AI Generativa e LLMs para melhorar produtos internos para os colaboradores",

    "ia_gestor_principal_negocio":
        "3.f.6 IA Generativa e LLMs como principal frente do negócio",

    "ia_gestor_nao_prioridade":
        "3.f.7 IA Generativa e LLMs não é prioridade",

    "ia_gestor_nao_sabe":
        "3.f.8 Não sei opinar sobre o uso de IA Generativa e LLMs na empresa",

    # Pergunta nova da edição 2025.
    "ia_resultado_empresa":
        "3.g_empresa_está_conseguindo_ter_bons_resultados_com_llms",

    # --------------------------------------------------------
    # IA - BARREIRAS
    # Em 2025, o bloco mudou de 3.g para 3.h.
    # --------------------------------------------------------
    "ia_barreiras_empresa":
        "3.h_motivos_para_não_usar_ai_generativa_e_llm",

    "ia_barreira_casos_uso":
        "3.h.1 Falta de compreensão dos casos de uso",

    "ia_barreira_confiabilidade":
        "3.h.2 Falta de confiabilidade das saídas (alucinação dos modelos)",

    "ia_barreira_regulamentacao":
        "3.h.3 Incerteza em relação a regulamentação",

    "ia_barreira_seguranca":
        "3.h.4 Preocupações com segurança e privacidade de dados",

    "ia_barreira_roi":
        "3.h.5 Retorno sobre investimento (ROI) não comprovado de IA Generativa",

    "ia_barreira_dados":
        "3.h.6 Dados da empresa não estão prontos para uso de IA Generativa",

    "ia_barreira_expertise":
        "3.h.7 Falta de expertise ou falta de recursos",

    "ia_barreira_lideranca":
        "3.h.8 Alta direção da empresa não vê valor ou não vê como prioridade",

    "ia_barreira_propriedade_intelectual":
        "3.h.9 Preocupações com propriedade intelectual",

    # --------------------------------------------------------
    # IA - USO NA VISÃO DOS PROFISSIONAIS
    # Em 2025, o bloco mudou de 4.l para 4.i.
    # --------------------------------------------------------
    "ia_uso_profissionais":
        "4.i_tipo_de_uso_de_ai_generativa_e_llm_na_empresa",

    "ia_prof_uso_descentralizado":
        "4.i.1 Colaboradores usando AI generativa de forma independente e descentralizada",

    "ia_prof_uso_centralizado":
        "4.i.2 Direcionamento centralizado do uso de AI generativa",

    "ia_prof_copilot":
        "4.i.3 Desenvolvedores utilizando Copilots",

    "ia_prof_produto_externo":
        "4.i.4 AI Generativa e LLMs para melhorar produtos externos para os clientes finais",

    "ia_prof_produto_interno":
        "4.i.5 AI Generativa e LLMs para melhorar produtos internos para os colaboradores",

    "ia_prof_principal_negocio":
        "4.i.6 IA Generativa e LLMs como principal frente do negócio",

    "ia_prof_nao_prioridade":
        "4.i.7 IA Generativa e LLMs não é prioridade",

    "ia_prof_nao_sabe":
        "4.i.8 Não sei opinar sobre o uso de IA Generativa e LLMs na empresa",

    # --------------------------------------------------------
    # IA - PRODUTIVIDADE / CUSTO
    # Em 2025, o bloco mudou de 4.m para 4.j.
    # --------------------------------------------------------
    "usa_ia_trabalho":
        "4.j_usa_chatgpt_ou_copilot_no_trabalho?",

    "ia_prod_nao_usa":
        "4.j.1 Não uso soluções de AI Generativa com foco em produtividade",

    "ia_prod_gratuita":
        "4.j.2 Uso soluções gratuitas de AI Generativa com foco em produtividade",

    "ia_prod_paga_proprio":
        "4.j.3 Uso e pago pelas soluções de AI Generativa com foco em produtividade",

    "ia_prod_paga_empresa":
        "4.j.4 A empresa que trabalho paga pelas soluções de AI Generativa com foco em produtividade",

    "ia_prod_copilot":
        "4.j.5 Uso soluções do tipo Copilot"
}


# ============================================================
# 9. FUNÇÃO PARA VALIDAR AS COLUNAS
# ============================================================

def validar_colunas(df, mapa, nome_base):

    colunas_existentes = set(df.columns)
    colunas_faltantes = []

    for nome_original in mapa.values():

        # None representa uma pergunta que não existia naquele ano.
        if nome_original is None:
            continue

        if nome_original not in colunas_existentes:
            colunas_faltantes.append(nome_original)

    if colunas_faltantes:

        print("============================================")
        print(f"ERRO - COLUNAS AUSENTES NA BASE {nome_base}")
        print("============================================")

        for coluna in colunas_faltantes:
            print("COLUNA NÃO ENCONTRADA:", coluna)

        raise Exception(
            f"A base {nome_base} possui colunas ausentes."
        )

    print(f"Validação da base {nome_base}: OK")


# ============================================================
# 10. VALIDAR OS TRÊS MAPEAMENTOS
# ============================================================

validar_colunas(df_2023, mapa_2023, "2023")
validar_colunas(df_2024, mapa_2024, "2024")
validar_colunas(df_2025, mapa_2025, "2025")


# ============================================================
# 11. FUNÇÃO DE PADRONIZAÇÃO
# ============================================================

def padronizar(df, mapa, ano):

    colunas = []

    for nome_final, nome_original in mapa.items():

        if nome_original is None:

            # Campo inexistente naquele ano.
            coluna = (
                F.lit(None)
                .cast("string")
                .alias(nome_final)
            )

        else:

            # Backticks protegem nomes com pontos, barras,
            # espaços, acentos, parênteses etc.
            coluna = (
                F.col(f"`{nome_original}`")
                .alias(nome_final)
            )

        colunas.append(coluna)

    resultado = df.select(*colunas)

    # Mantém o nome usado pela Gold antiga, mas agora com
    # os anos corretos: 2023, 2024 e 2025.
    resultado = (
        resultado
        .withColumn(
            "edicao_pesquisa",
            F.lit(str(ano))
        )
    )

    return resultado


# ============================================================
# 12. PADRONIZAÇÃO DAS TRÊS BASES
# ============================================================

silver_2023 = padronizar(
    df_2023,
    mapa_2023,
    2023
)

silver_2024 = padronizar(
    df_2024,
    mapa_2024,
    2024
)

silver_2025 = padronizar(
    df_2025,
    mapa_2025,
    2025
)


# ============================================================
# 13. LIMPEZA DOS TEXTOS
# ============================================================

def limpar_textos(df):

    for campo, tipo in df.dtypes:

        if tipo == "string":

            df = (
                df
                .withColumn(
                    campo,
                    F.when(
                        F.trim(F.col(campo)) == "",
                        None
                    )
                    .otherwise(
                        F.trim(F.col(campo))
                    )
                )
            )

    return df


silver_2023 = limpar_textos(silver_2023)
silver_2024 = limpar_textos(silver_2024)
silver_2025 = limpar_textos(silver_2025)


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


silver_2023 = tratar_idade(silver_2023)
silver_2024 = tratar_idade(silver_2024)
silver_2025 = tratar_idade(silver_2025)


# ============================================================
# 15. INDICADORES BINÁRIOS
# ============================================================

INDICADORES = [

    # Tecnologias
    "usa_sql",
    "usa_python",
    "usa_aws",
    "usa_powerbi",

    # IA - gestores
    "ia_gestor_uso_descentralizado",
    "ia_gestor_uso_centralizado",
    "ia_gestor_copilot",
    "ia_gestor_produto_externo",
    "ia_gestor_produto_interno",
    "ia_gestor_principal_negocio",
    "ia_gestor_nao_prioridade",
    "ia_gestor_nao_sabe",

    # IA - barreiras
    "ia_barreira_casos_uso",
    "ia_barreira_confiabilidade",
    "ia_barreira_regulamentacao",
    "ia_barreira_seguranca",
    "ia_barreira_roi",
    "ia_barreira_dados",
    "ia_barreira_expertise",
    "ia_barreira_lideranca",
    "ia_barreira_propriedade_intelectual",

    # IA - profissionais
    "ia_prof_uso_descentralizado",
    "ia_prof_uso_centralizado",
    "ia_prof_copilot",
    "ia_prof_produto_externo",
    "ia_prof_produto_interno",
    "ia_prof_principal_negocio",
    "ia_prof_nao_prioridade",
    "ia_prof_nao_sabe",

    # IA - produtividade/custo
    "ia_prod_nao_usa",
    "ia_prod_gratuita",
    "ia_prod_paga_proprio",
    "ia_prod_paga_empresa",
    "ia_prod_copilot"
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


silver_2023 = tratar_indicadores(silver_2023)
silver_2024 = tratar_indicadores(silver_2024)
silver_2025 = tratar_indicadores(silver_2025)


# ============================================================
# 16. INDICADORES DERIVADOS DE IA
# ============================================================

def criar_indicadores_ia(df):

    # O bloco é considerado respondido quando pelo menos uma
    # das alternativas de produtividade/custo possui valor.
    respondeu_bloco = (
        F.col("ia_prod_nao_usa").isNotNull()
        |
        F.col("ia_prod_gratuita").isNotNull()
        |
        F.col("ia_prod_paga_proprio").isNotNull()
        |
        F.col("ia_prod_paga_empresa").isNotNull()
        |
        F.col("ia_prod_copilot").isNotNull()
    )

    usa_alguma_ia = (
        (F.coalesce(F.col("ia_prod_gratuita"), F.lit(0)) == 1)
        |
        (F.coalesce(F.col("ia_prod_paga_proprio"), F.lit(0)) == 1)
        |
        (F.coalesce(F.col("ia_prod_paga_empresa"), F.lit(0)) == 1)
        |
        (F.coalesce(F.col("ia_prod_copilot"), F.lit(0)) == 1)
    )

    # 1 = usa pelo menos uma solução de IA focada em produtividade
    # 0 = declarou que não usa
    # NULL = não respondeu ao bloco
    df = (
        df
        .withColumn(
            "ia_adota_produtividade",
            F.when(
                ~respondeu_bloco,
                F.lit(None).cast("int")
            )
            .when(
                usa_alguma_ia,
                F.lit(1)
            )
            .when(
                F.col("ia_prod_nao_usa") == 1,
                F.lit(0)
            )
            .otherwise(
                F.lit(None).cast("int")
            )
        )
    )

    # Facilita a criação de cartões de custo na Gold/BI.
    df = (
        df
        .withColumn(
            "ia_tem_custo_empresa",
            F.when(
                F.col("ia_prod_paga_empresa").isNull(),
                F.lit(None).cast("int")
            )
            .otherwise(
                F.col("ia_prod_paga_empresa")
            )
        )
        .withColumn(
            "ia_tem_custo_profissional",
            F.when(
                F.col("ia_prod_paga_proprio").isNull(),
                F.lit(None).cast("int")
            )
            .otherwise(
                F.col("ia_prod_paga_proprio")
            )
        )
    )

    return df


silver_2023 = criar_indicadores_ia(silver_2023)
silver_2024 = criar_indicadores_ia(silver_2024)
silver_2025 = criar_indicadores_ia(silver_2025)


# ============================================================
# 17. UNIÃO DAS TRÊS PESQUISAS
# ============================================================

silver = (
    silver_2023
    .unionByName(silver_2024)
    .unionByName(silver_2025)
)


# ============================================================
# 18. VALIDAÇÃO DO RESULTADO
# ============================================================

total_silver = silver.count()

print("============================================")
print("CAMADA SILVER")
print("============================================")
print("TOTAL DE REGISTROS:", total_silver)

print("============================================")
print("REGISTROS POR ANO")
print("============================================")

(
    silver
    .groupBy("edicao_pesquisa")
    .count()
    .orderBy("edicao_pesquisa")
    .show(truncate=False)
)


# ============================================================
# 19. VALIDAÇÃO DOS NOVOS CAMPOS DE IA
# ============================================================

print("============================================")
print("VALIDAÇÃO DOS CAMPOS DE IA")
print("============================================")

(
    silver
    .groupBy("edicao_pesquisa")
    .agg(
        F.count("usa_ia_trabalho").alias(
            "respostas_uso_ia"
        ),

        F.count("ia_adota_produtividade").alias(
            "respondentes_validos_adocao"
        ),

        F.sum("ia_adota_produtividade").alias(
            "usuarios_ia_produtividade"
        ),

        F.sum("ia_prod_gratuita").alias(
            "usa_ia_gratuita"
        ),

        F.sum("ia_prod_paga_proprio").alias(
            "profissional_paga_ia"
        ),

        F.sum("ia_prod_paga_empresa").alias(
            "empresa_paga_ia"
        ),

        F.sum("ia_prod_copilot").alias(
            "usa_copilot"
        ),

        F.sum("ia_barreira_roi").alias(
            "roi_nao_comprovado"
        ),

        F.sum("ia_barreira_expertise").alias(
            "falta_expertise_recursos"
        ),

        F.sum("ia_barreira_dados").alias(
            "dados_nao_preparados"
        ),

        F.count("ia_resultado_empresa").alias(
            "respostas_resultado_empresa"
        )
    )
    .orderBy("edicao_pesquisa")
    .show(truncate=False)
)


# ============================================================
# 20. AMOSTRA DOS DADOS
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
        "ia_prioridade_empresa",
        "usa_ia_trabalho",
        "ia_adota_produtividade",
        "ia_prod_gratuita",
        "ia_prod_paga_proprio",
        "ia_prod_paga_empresa",
        "ia_prod_copilot",
        "ia_resultado_empresa",
        "ia_barreira_roi",
        "ia_barreira_expertise",
        "ia_barreira_dados",
        "ia_barreira_seguranca",
        "ia_barreira_casos_uso"
    )
    .show(
        20,
        truncate=False
    )
)


# ============================================================
# 21. VERIFICAÇÃO DE NULOS IMPORTANTES
# ============================================================

print("============================================")
print("VERIFICAÇÃO DE CAMPOS NULOS")
print("============================================")

(
    silver
    .select(
        F.sum(
            F.col("idade").isNull().cast("int")
        ).alias("idade_nulos"),

        F.sum(
            F.col("genero").isNull().cast("int")
        ).alias("genero_nulos"),

        F.sum(
            F.col("cargo_atual").isNull().cast("int")
        ).alias("cargo_nulos"),

        F.sum(
            F.col("nivel_profissional").isNull().cast("int")
        ).alias("nivel_nulos"),

        F.sum(
            F.col("faixa_salarial").isNull().cast("int")
        ).alias("salario_nulos"),

        F.sum(
            F.col("usa_ia_trabalho").isNull().cast("int")
        ).alias("uso_ia_nulos"),

        F.sum(
            F.col("ia_resultado_empresa").isNull().cast("int")
        ).alias("resultado_empresa_nulos")
    )
    .show()
)


# ============================================================
# 22. GRAVAÇÃO DA CAMADA SILVER EM PARQUET
# ============================================================

print("============================================")
print("INICIANDO GRAVAÇÃO DA SILVER")
print("============================================")

(
    silver
    .write
    .mode("overwrite")
    .partitionBy("edicao_pesquisa")
    .parquet(CAMINHO_SILVER)
)


# ============================================================
# 23. CONFIRMAÇÃO
# ============================================================

print("============================================")
print("PROCESSAMENTO CONCLUÍDO")
print("============================================")

print("CAMINHO SILVER:")
print(CAMINHO_SILVER)

print("TOTAL DE REGISTROS:")
print(total_silver)


# ============================================================
# 24. FINALIZAÇÃO DO GLUE JOB
# ============================================================

job.commit()
