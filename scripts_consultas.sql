// Total de alunos cadastrados na instituição
SELECT COUNT(*) FROM stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos"."dados_alunos.parquet";

// Total de alunos únicos na instituição
SELECT DISTINCT COUNT(*) FROM stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos"."dados_alunos.parquet";

// Média de aprovação de alunos por curso
SELECT 
    Curso,
    SUM(CASE WHEN Status = 'Aprovado' THEN 1 ELSE 0 END) * 100 / COUNT(*) AS MediaAprovacao
FROM 
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos_curso_disciplinas"."dados_alunos_curso_disciplinas.parquet"
GROUP BY 
    Curso
ORDER BY 
    MediaAprovacao DESC;

// Tempo médio de conclusão de curso
SELECT 
    Curso,
    AVG(YEAR(Data_Conclusao) - YEAR(Data_Ingresso)) AS TempoMedioConclusaoAnos
FROM 
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos"."dados_alunos.parquet"
WHERE 
     Data_Conclusao IS NOT NULL
GROUP BY 
    Curso
ORDER BY 
    TempoMedioConclusaoAnos;

// TOP 10 disciplinas que possuem maior índice de alunos reprovados
SELECT 
    Codigo_Disciplina,
    SUM(CASE WHEN Status = 'Reprovado' THEN 1 ELSE 0 END) * 100 / COUNT(*) AS MediaReprovacao
FROM 
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos_curso_disciplinas"."dados_alunos_curso_disciplinas.parquet"
GROUP BY 
    Codigo_Disciplina
ORDER BY 
    MediaReprovacao DESC
LIMIT 10;

// TOP 10 disciplinas que possuem o menor índice de alunos reprovados
SELECT 
    Codigo_Disciplina,
    SUM(CASE WHEN Status = 'Reprovado' THEN 1 ELSE 0 END) * 100 / COUNT(*) AS MediaReprovacao
FROM 
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos_curso_disciplinas"."dados_alunos_curso_disciplinas.parquet"
GROUP BY 
    Codigo_Disciplina
ORDER BY 
    MediaReprovacao ASC
LIMIT 10;

/*        g. Faixa etária dos alunos que possuem mais notas altas e baixas
            i. Para gerar as faixas etárias, considere o range a seguir:
            ii. 18 a 25 anos (pré-adulto)
            iii. 26 a 30 anos (jovem adulto I)
            iv. 31 a 35 anos (jovem adulto II)
            v. 36 a 40 anos (adulto médio I)
            vi. 41 a 50 anos (adulto médio II)
            vii. 50 a 60 anos (adulto médio III)
            viii. Acima de 61 anos (adulto idoso) */

WITH MediaGeral AS (
    SELECT AVG(Nota_Obtida) AS MediaDasNotas 
    FROM stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_atividades_alunos"."dados_atividades_alunos.parquet"
),
AlunosFaixaEtaria AS (
    SELECT
        da.Matricula,
        CASE
            WHEN YEAR(CURRENT_DATE) - YEAR(da.Data_Nascimento) BETWEEN 18 AND 25 THEN '18 a 25 anos (pré-adulto)'
            WHEN YEAR(CURRENT_DATE) - YEAR(da.Data_Nascimento) BETWEEN 26 AND 30 THEN '26 a 30 anos (jovem adulto I)'
            WHEN YEAR(CURRENT_DATE) - YEAR(da.Data_Nascimento) BETWEEN 31 AND 35 THEN '31 a 35 anos (jovem adulto II)'
            WHEN YEAR(CURRENT_DATE) - YEAR(da.Data_Nascimento) BETWEEN 36 AND 40 THEN '36 a 40 anos (adulto médio I)'
            WHEN YEAR(CURRENT_DATE) - YEAR(da.Data_Nascimento) BETWEEN 41 AND 50 THEN '41 a 50 anos (adulto médio II)'
            WHEN YEAR(CURRENT_DATE) - YEAR(da.Data_Nascimento) BETWEEN 51 AND 60 THEN '51 a 60 anos (adulto médio III)'
            ELSE 'Acima de 61 anos (adulto idoso)'
        END AS FaixaEtaria,
        CASE
            WHEN daa.Nota_Obtida > (SELECT MediaDasNotas FROM MediaGeral) THEN 'Alta'
            ELSE 'Baixa'
        END AS ClassificacaoNota
    FROM
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos"."dados_alunos.parquet" as da
INNER JOIN 
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_atividades_alunos"."dados_atividades_alunos.parquet" as daa
ON da.Matricula = daa.Matricula_Aluno
)
SELECT
    FaixaEtaria,
    COUNT(DISTINCT CASE WHEN ClassificacaoNota = 'Alta' THEN Matricula ELSE NULL END) AS TotalNotasAltas,
    COUNT(DISTINCT CASE WHEN ClassificacaoNota = 'Baixa' THEN Matricula ELSE NULL END) AS TotalNotasBaixas
FROM
    AlunosFaixaEtaria
GROUP BY
    FaixaEtaria
ORDER BY
    FaixaEtaria;

// Quais são os 10 melhores alunos de todos os tempos
SELECT 
    Matricula_Aluno,
    SUM(Nota_Aluno) AS Notas
FROM 
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos_curso_disciplinas"."dados_alunos_curso_disciplinas.parquet"
GROUP BY 
    Matricula_Aluno
ORDER BY 
    Notas DESC
LIMIT 10;

//Quantidade de alunos reprovados por frequência
SELECT 
    Motivo_Reprovacao,
    COUNT(Motivo_Reprovacao) AS Qtde_Reprovados
FROM 
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos_curso_disciplinas"."dados_alunos_curso_disciplinas.parquet"
WHERE 
    Motivo_Reprovacao ='frequência'
GROUP BY
    Motivo_Reprovacao;

//Quantidade de alunos reprovados por nota
SELECT 
    Motivo_Reprovacao,
    COUNT(Motivo_Reprovacao) AS Qtde_Reprovados
FROM 
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos_curso_disciplinas"."dados_alunos_curso_disciplinas.parquet"
WHERE 
    Motivo_Reprovacao ='nota'
GROUP BY
    Motivo_Reprovacao;

//Curso que possui o maior índice de aprovação
SELECT 
    Curso,
    COUNT(Status) AS Status
FROM 
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos_curso_disciplinas"."dados_alunos_curso_disciplinas.parquet"
WHERE 
    Status ='Aprovado'
GROUP BY
    Curso
ORDER BY
    Status DESC;

// Curso que possui o menor índice de aprovação
SELECT 
    Curso,
    COUNT(Status) AS Status
FROM 
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_alunos_curso_disciplinas"."dados_alunos_curso_disciplinas.parquet"
WHERE 
    Status ='Aprovado'
GROUP BY
    Curso
ORDER BY
    Status ASC;

// Quais são as disciplinas que possuem o maior percentual de atividades com notas acima da média

WITH MediaGeral AS (
    SELECT AVG(Nota_Obtida) AS MediaDasNotas 
    FROM stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_atividades_alunos"."dados_atividades_alunos.parquet"
),
AtividadesAcimaDaMedia AS (
    SELECT
        Codigo_Disciplina,
        COUNT(CASE WHEN Nota_Obtida > (SELECT MediaDasNotas FROM MediaGeral) THEN 1 END) as QuantidadeAcimaDaMedia,
        COUNT(*) AS TotalAtividades
    FROM
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_atividades_alunos"."dados_atividades_alunos.parquet"
    GROUP BY
        Codigo_Disciplina
)
SELECT
    Codigo_Disciplina,
    (CAST(QuantidadeAcimaDaMedia AS DOUBLE) / TotalAtividades) * 100 AS PercentualAcimaDaMedia
FROM
    AtividadesAcimaDaMedia
ORDER BY
    PercentualAcimaDaMedia DESC;

// Quais são as disciplinas que possuem o menor percentual de atividades com notas acima da média

WITH MediaGeral AS (
    SELECT AVG(Nota_Obtida) AS MediaDasNotas 
    FROM stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_atividades_alunos"."dados_atividades_alunos.parquet"
),
AtividadesAcimaDaMedia AS (
    SELECT
        Codigo_Disciplina,
        COUNT(CASE WHEN Nota_Obtida > (SELECT MediaDasNotas FROM MediaGeral) THEN 1 END) as QuantidadeAcimaDaMedia,
        COUNT(*) AS TotalAtividades
    FROM
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_atividades_alunos"."dados_atividades_alunos.parquet"
    GROUP BY
        Codigo_Disciplina
)
SELECT
    Codigo_Disciplina,
    (CAST(QuantidadeAcimaDaMedia AS DOUBLE) / TotalAtividades) * 100 AS PercentualAcimaDaMedia
FROM
    AtividadesAcimaDaMedia
ORDER BY
    PercentualAcimaDaMedia ASC;

// Quais são as disciplinas que possuem o maior percentual de atividades com notas abaixo da média

WITH MediaGeral AS (
    SELECT AVG(Nota_Obtida) AS MediaDasNotas 
    FROM stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_atividades_alunos"."dados_atividades_alunos.parquet"
),
AtividadesAbaixoDaMedia AS (
    SELECT
        Codigo_Disciplina,
        COUNT(CASE WHEN Nota_Obtida < (SELECT MediaDasNotas FROM MediaGeral) THEN 1 END) as QuantidadeAbaixoDaMedia,
        COUNT(*) AS TotalAtividades
    FROM
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_atividades_alunos"."dados_atividades_alunos.parquet"
    GROUP BY
        Codigo_Disciplina
)
SELECT
    Codigo_Disciplina,
    (CAST(QuantidadeAbaixoDaMedia AS DOUBLE) / TotalAtividades) * 100 AS PercentualAbaixoDaMedia
FROM
    AtividadesAbaixoDaMedia
ORDER BY
    PercentualAbaixoDaMedia DESC;

// Quais são as disciplinas que possuem o menor percentual de atividades com notas abaixo da média
WITH MediaGeral AS (
    SELECT AVG(Nota_Obtida) AS MediaDasNotas 
    FROM stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_atividades_alunos"."dados_atividades_alunos.parquet"
),
AtividadesAbaixoDaMedia AS (
    SELECT
        Codigo_Disciplina,
        COUNT(CASE WHEN Nota_Obtida < (SELECT MediaDasNotas FROM MediaGeral) THEN 1 END) as QuantidadeAbaixoDaMedia,
        COUNT(*) AS TotalAtividades
    FROM
stgaccount1096500."datalake-dados-faculdade".consume."dados_faculdade"."dados_atividades_alunos"."dados_atividades_alunos.parquet"
    GROUP BY
        Codigo_Disciplina
)
SELECT
    Codigo_Disciplina,
    (CAST(QuantidadeAbaixoDaMedia AS DOUBLE) / TotalAtividades) * 100 AS PercentualAbaixoDaMedia
FROM
    AtividadesAbaixoDaMedia
ORDER BY
    PercentualAbaixoDaMedia ASC;

