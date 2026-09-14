-- ============================================================
-- 1. RESUMO GERAL
-- ============================================================

SELECT *
FROM database_tech.gold_resumo_geral
ORDER BY edicao_pesquisa;
-- ============================================================
-- 2. TOTAL DE RESPONDENTES POR ANO
-- ============================================================

SELECT
    edicao_pesquisa,
    total_respondentes
FROM database_tech.gold_resumo_geral
ORDER BY edicao_pesquisa;
-- ============================================================
-- 3. COMO ESTÁ ESTRUTURADO O MERCADO BRASILEIRO DE DADOS?
-- Distribuição dos cargos por ano
-- ============================================================

SELECT
    edicao_pesquisa,
    cargo_atual,
    SUM(quantidade_profissionais) AS profissionais
FROM database_tech.gold_mercado_cargos
GROUP BY
    edicao_pesquisa,
    cargo_atual
ORDER BY
    edicao_pesquisa,
    profissionais DESC;
-- ============================================================
-- 4. TOP 10 CARGOS - 2025
-- ============================================================

SELECT
    cargo_atual,
    SUM(quantidade_profissionais) AS profissionais
FROM database_tech.gold_mercado_cargos
WHERE edicao_pesquisa = '2025'
GROUP BY cargo_atual
ORDER BY profissionais DESC
LIMIT 10;
-- ============================================================
-- 5. SENIORIDADE
-- ============================================================

SELECT
    edicao_pesquisa,
    nivel_profissional,
    SUM(quantidade_profissionais) AS profissionais
FROM database_tech.gold_mercado_cargos
WHERE nivel_profissional IS NOT NULL
GROUP BY
    edicao_pesquisa,
    nivel_profissional
ORDER BY
    edicao_pesquisa,
    profissionais DESC;
-- ============================================================
-- 6. REMUNERAÇÃO POR CARGO + SENIORIDADE
-- ============================================================

SELECT
    edicao_pesquisa,
    cargo_atual,
    nivel_profissional,
    faixa_salarial_tratada,
    ordem_faixa_salarial,
    quantidade_profissionais,
    total_cargo_senioridade,
    percentual_cargo_senioridade
FROM database_tech.gold_remuneracao_senioridade
ORDER BY
    edicao_pesquisa,
    cargo_atual,
    nivel_profissional,
    ordem_faixa_salarial;
-- ============================================================
-- 7. REMUNERAÇÃO - ANALISTA DE DADOS
-- ============================================================

SELECT
    edicao_pesquisa,
    cargo_atual,
    nivel_profissional,
    faixa_salarial_tratada,
    ordem_faixa_salarial,
    quantidade_profissionais,
    percentual_cargo_senioridade
FROM database_tech.gold_remuneracao_senioridade
WHERE LOWER(cargo_atual) LIKE '%analista de dados%'
ORDER BY
    edicao_pesquisa,
    nivel_profissional,
    ordem_faixa_salarial;
-- ============================================================
-- 8. DIVERSIDADE DE GÊNERO
-- ============================================================

SELECT
    edicao_pesquisa,
    genero,
    quantidade_profissionais,
    total_respostas,
    percentual
FROM database_tech.gold_diversidade_genero
ORDER BY
    edicao_pesquisa,
    percentual DESC;
-- ============================================================
-- 9. DISTRIBUIÇÃO REGIONAL
-- ============================================================

SELECT
    edicao_pesquisa,
    regiao,
    quantidade_profissionais,
    total_respostas,
    percentual
FROM database_tech.gold_regioes
ORDER BY
    edicao_pesquisa,
    percentual DESC;
-- ============================================================
-- 10. MODELO DE TRABALHO
-- ============================================================

SELECT
    edicao_pesquisa,
    modelo_trabalho_resumido,
    quantidade_profissionais,
    total_respostas,
    percentual
FROM database_tech.gold_modelo_trabalho
ORDER BY
    edicao_pesquisa,
    percentual DESC;
-- ============================================================
-- 11. TECNOLOGIAS
-- ============================================================

SELECT
    edicao_pesquisa,
    tecnologia,
    quantidade_usuarios,
    respondentes_validos,
    percentual_adocao
FROM database_tech.gold_tecnologias
ORDER BY
    edicao_pesquisa,
    percentual_adocao DESC;
-- ============================================================
-- 12. ÍNDICE DE ADOÇÃO DE IA
-- ============================================================

SELECT
    edicao_pesquisa,
    usuarios_ia,
    nao_usuarios_ia,
    respondentes_validos,
    percentual_adocao_ia
FROM database_tech.gold_adocao_ia
ORDER BY edicao_pesquisa;
-- ============================================================
-- 13. IA - PRODUTIVIDADE E CUSTO
-- ============================================================

SELECT
    edicao_pesquisa,
    metrica,
    quantidade,
    respondentes_validos,
    percentual
FROM database_tech.gold_ia_produtividade_custo
ORDER BY
    edicao_pesquisa,
    percentual DESC;
-- ============================================================
-- 14. IA - PRIORIDADE ESTRATÉGICA NAS EMPRESAS
-- ============================================================

SELECT
    edicao_pesquisa,
    ia_prioridade_empresa,
    quantidade,
    respondentes_validos,
    percentual
FROM database_tech.gold_ia_prioridade
ORDER BY
    edicao_pesquisa,
    percentual DESC;
-- ============================================================
-- 15. IA - APLICAÇÕES / CASOS DE USO
-- ============================================================

SELECT
    edicao_pesquisa,
    categoria,
    metrica,
    quantidade,
    respondentes_validos,
    percentual
FROM database_tech.gold_ia_aplicacoes
ORDER BY
    edicao_pesquisa,
    categoria,
    percentual DESC;
-- ============================================================
-- 16. IA - PRINCIPAIS BARREIRAS
-- ============================================================

SELECT
    edicao_pesquisa,
    metrica,
    quantidade,
    respondentes_validos,
    percentual
FROM database_tech.gold_ia_barreiras
ORDER BY
    edicao_pesquisa,
    percentual DESC;
-- ============================================================
-- 17. BARREIRAS DE IA - 2025
-- ============================================================

SELECT
    metrica,
    quantidade,
    respondentes_validos,
    percentual
FROM database_tech.gold_ia_barreiras
WHERE edicao_pesquisa = '2025'
ORDER BY percentual DESC;
-- ============================================================
-- 18. IA - RESULTADOS / IMPACTO NO NEGÓCIO
-- ============================================================

SELECT
    edicao_pesquisa,
    ia_resultado_empresa,
    quantidade,
    respondentes_validos,
    percentual
FROM database_tech.gold_ia_impacto_negocio
ORDER BY percentual DESC;


