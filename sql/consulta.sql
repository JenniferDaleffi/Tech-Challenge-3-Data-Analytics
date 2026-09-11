-- Resumo Geral

SELECT *
FROM database_tech.gold_resumo_geral
ORDER BY edicao_pesquisa;

SELECT
    edicao_pesquisa,
    total_respondentes
FROM database_tech.gold_resumo_geral
ORDER BY edicao_pesquisa;

-- Como está estruturado o mercado brasileiro de Dados?

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


-- Maiores cargos

SELECT
    cargo_atual,
    SUM(quantidade_profissionais) AS profissionais
FROM database_tech.gold_mercado_cargos
WHERE edicao_pesquisa = '2025-2026'
GROUP BY cargo_atual
ORDER BY profissionais DESC
LIMIT 10;


-- Senioridade

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

-- Remuneração por senioridade e cargo

SELECT
    edicao_pesquisa,
    cargo_atual,
    nivel_profissional,
    faixa_salarial_tratada,
    quantidade_profissionais,
    percentual_cargo_senioridade
FROM database_tech.gold_remuneracao_senioridade
ORDER BY
    edicao_pesquisa,
    cargo_atual,
    nivel_profissional,
    ordem_faixa_salarial;


-- Remuneração por senioridade do cargo de Analista de Dados

SELECT
    edicao_pesquisa,
    cargo_atual,
    nivel_profissional,
    faixa_salarial_tratada,
    quantidade_profissionais,
    percentual_cargo_senioridade
FROM database_tech.gold_remuneracao_senioridade
WHERE LOWER(cargo_atual) LIKE '%analista de dados%'
ORDER BY
    edicao_pesquisa,
    nivel_profissional,
    ordem_faixa_salarial;

-- Diversidade de gênero

SELECT
    edicao_pesquisa,
    genero,
    quantidade_profissionais,
    percentual
FROM database_tech.gold_diversidade_genero
ORDER BY
    edicao_pesquisa,
    percentual DESC;

-- Distribuição regional

SELECT
    edicao_pesquisa,
    regiao,
    quantidade_profissionais,
    percentual
FROM database_tech.gold_regioes
ORDER BY
    edicao_pesquisa,
    percentual DESC;

-- Modelo de trabalho

SELECT
    edicao_pesquisa,
    modelo_trabalho_resumido,
    quantidade_profissionais,
    percentual
FROM database_tech.gold_modelo_trabalho
ORDER BY
    edicao_pesquisa,
    percentual DESC;

-- Tecnologias

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

-- Adoção de Inteligência Artificial

SELECT
    edicao_pesquisa,
    usuarios_ia,
    nao_usuarios_ia,
    respondentes_validos,
    percentual_adocao_ia
FROM database_tech.gold_adocao_ia
ORDER BY edicao_pesquisa;