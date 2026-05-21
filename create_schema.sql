-- ClickHouse schema for the vacinas dashboard.
--
-- Provisions everything the API queries at runtime: the main `events`
-- table and the two AggregatingMergeTree materialized views that
-- back the dashboard's hot KPI paths. Run this once against your CH
-- cluster, then point the ingestion script at it.
--
-- Design notes:
--   * ORDER BY (sigla_uf_paciente, sigla_vacina, data_vacina,
--     numero_idade_paciente) matches the dominant dashboard request
--     shape — state and vaccine narrow the scan first; date and age
--     trim within. Primary-key pruning kicks in the moment the user
--     picks a state or vaccine.
--   * PARTITION BY toYYYYMM(data_vacina) — ~12 partitions/year. Coarse
--     enough that merges complete cheaply at 1B rows, fine enough
--     that `last_30_days` filters scan only one partition.
--   * LowCardinality(String) for everything with <10k distinct values:
--     at 1B rows this is the difference between ~300 GiB and ~30 GiB
--     on disk, plus dictionary-fast group-by.
--   * proj_by_date covers queries that filter only on date (eg
--     /monitoring/real-time-stats) — CH picks it automatically.
--   * The two MVs replace the 11 DuckDB cubes via group-by rollups.

CREATE DATABASE IF NOT EXISTS vacinas;

-- =========================================================================
-- Main events table
-- =========================================================================
CREATE TABLE IF NOT EXISTS vacinas.events
(
    -- Delta(2) before ZSTD shrinks the date column ~5x at 1B rows.
    -- Two-byte delta because Date is internally a UInt16.
    data_vacina                       Date CODEC(Delta(2), ZSTD(3)),

    -- Patient
    sigla_uf_paciente                 LowCardinality(String),
    nome_uf_paciente                  LowCardinality(String),
    nome_municipio_paciente           LowCardinality(String),
    codigo_municipio_paciente         LowCardinality(String),
    codigo_paciente                   String CODEC(ZSTD(3)),
    tipo_sexo_paciente                LowCardinality(String),
    numero_idade_paciente             UInt8,
    nome_raca_cor_paciente            LowCardinality(String),
    codigo_raca_cor_paciente          LowCardinality(String),
    codigo_pais_paciente              LowCardinality(String),
    nome_pais_paciente                LowCardinality(String),
    descricao_nacionalidade_paciente  LowCardinality(String),
    numero_cep_paciente               String,
    codigo_etnia_indigena_paciente    LowCardinality(Nullable(String)),
    nome_etnia_indigena_paciente      LowCardinality(Nullable(String)),

    -- Vaccine
    sigla_vacina                      LowCardinality(String),
    descricao_vacina                  LowCardinality(String),
    codigo_vacina                     LowCardinality(String),
    codigo_dose_vacina                UInt8,
    descricao_dose_vacina             LowCardinality(String),
    codigo_vacina_fabricante          LowCardinality(Nullable(String)),
    descricao_vacina_fabricante       LowCardinality(String),
    codigo_lote_vacina                String,
    codigo_via_administracao          LowCardinality(String),
    descricao_via_administracao       LowCardinality(String),
    codigo_local_aplicacao            LowCardinality(String),
    descricao_local_aplicacao         LowCardinality(String),
    codigo_estrategia_vacinacao       LowCardinality(String),
    descricao_estrategia_vacinacao    LowCardinality(String),
    codigo_vacina_categoria_atendimento  LowCardinality(Nullable(String)),
    descricao_vacina_categoria_atendimento LowCardinality(Nullable(String)),
    codigo_vacina_grupo_atendimento   LowCardinality(Nullable(String)),
    descricao_vacina_grupo_atendimento LowCardinality(Nullable(String)),
    codigo_condicao_maternal          LowCardinality(Nullable(String)),
    descricao_condicao_maternal       LowCardinality(Nullable(String)),

    -- Establishment
    sigla_uf_estabelecimento          LowCardinality(String),
    nome_uf_estabelecimento           LowCardinality(String),
    nome_municipio_estabelecimento    LowCardinality(String),
    codigo_municipio_estabelecimento  LowCardinality(String),
    codigo_cnes_estabelecimento       LowCardinality(String),
    nome_razao_social_estabelecimento String,
    nome_fantasia_estalecimento       String,
    codigo_tipo_estabelecimento       LowCardinality(String),
    descricao_tipo_estabelecimento    LowCardinality(String),
    codigo_natureza_estabelecimento   LowCardinality(String),
    descricao_natureza_estabelecimento LowCardinality(String),

    -- System
    codigo_documento                  String CODEC(ZSTD(3)),
    status_documento                  LowCardinality(String),
    codigo_sistema_origem             LowCardinality(String),
    descricao_sistema_origem          LowCardinality(String),
    codigo_origem_registro            LowCardinality(Nullable(String)),
    descricao_origem_registro         LowCardinality(Nullable(String)),
    codigo_troca_documento            LowCardinality(Nullable(String)),
    data_entrada_rnds                 Nullable(Date),
    data_deletado_rnds                Nullable(Date),

    -- Skip indexes for columns filtered but not in the sort key.
    INDEX idx_municipio_pac nome_municipio_paciente
        TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_cnes codigo_cnes_estabelecimento
        TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_estab_nome nome_razao_social_estabelecimento
        TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4,

    -- Alternate sort order picked automatically by CH for date-first
    -- queries with no state/vaccine filter.
    PROJECTION proj_by_date (
        SELECT *
        ORDER BY data_vacina, sigla_uf_paciente, sigla_vacina
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(data_vacina)
ORDER BY (sigla_uf_paciente, sigla_vacina, data_vacina, numero_idade_paciente)
SETTINGS index_granularity = 8192;


-- =========================================================================
-- MV: (state, vaccine, date) rollup
--
-- Source for /dashboard/overview filtered KPIs, vaccine performance,
-- daily/weekly/monthly trends, and the state-vaccine cube path.
-- =========================================================================
CREATE TABLE IF NOT EXISTS vacinas.mv_state_vaccine_date_data
(
    sigla_uf_paciente  LowCardinality(String),
    sigla_vacina       LowCardinality(String),
    data_vacina        Date,

    total_doses        SimpleAggregateFunction(sum, UInt64),
    unique_patients    AggregateFunction(uniq, String),
    female_count       SimpleAggregateFunction(sum, UInt64),
    male_count         SimpleAggregateFunction(sum, UInt64),
    sum_age            SimpleAggregateFunction(sum, UInt64),
    count_age          SimpleAggregateFunction(sum, UInt64),
    first_doses        SimpleAggregateFunction(sum, UInt64),
    second_doses       SimpleAggregateFunction(sum, UInt64),
    boosters           SimpleAggregateFunction(sum, UInt64),
    children           SimpleAggregateFunction(sum, UInt64),
    young_adults       SimpleAggregateFunction(sum, UInt64),
    adults             SimpleAggregateFunction(sum, UInt64),
    middle_aged        SimpleAggregateFunction(sum, UInt64),
    seniors            SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(data_vacina)
ORDER BY (sigla_uf_paciente, sigla_vacina, data_vacina);

CREATE MATERIALIZED VIEW IF NOT EXISTS vacinas.mv_state_vaccine_date
TO vacinas.mv_state_vaccine_date_data AS
SELECT
    sigla_uf_paciente,
    sigla_vacina,
    data_vacina,
    count()                                                       AS total_doses,
    uniqState(codigo_paciente)                                    AS unique_patients,
    countIf(tipo_sexo_paciente = 'F')                             AS female_count,
    countIf(tipo_sexo_paciente = 'M')                             AS male_count,
    sum(numero_idade_paciente)                                    AS sum_age,
    count(numero_idade_paciente)                                  AS count_age,
    countIf(codigo_dose_vacina = 1)                               AS first_doses,
    countIf(codigo_dose_vacina = 2)                               AS second_doses,
    countIf(codigo_dose_vacina >= 3)                              AS boosters,
    countIf(numero_idade_paciente < 18)                           AS children,
    countIf(numero_idade_paciente BETWEEN 18 AND 29)              AS young_adults,
    countIf(numero_idade_paciente BETWEEN 30 AND 49)              AS adults,
    countIf(numero_idade_paciente BETWEEN 50 AND 64)              AS middle_aged,
    countIf(numero_idade_paciente >= 65)                          AS seniors
FROM vacinas.events
GROUP BY sigla_uf_paciente, sigla_vacina, data_vacina;


-- =========================================================================
-- MV: (state, age, gender, race, date) rollup
--
-- Source for /analytics/demographic-insights and the demo-cube path
-- on /dashboard/overview. Exact age (UInt8), not pre-bucketed, so the
-- frontend slider's min/max maps to WHERE age BETWEEN m AND n.
-- =========================================================================
CREATE TABLE IF NOT EXISTS vacinas.mv_state_demo_data
(
    sigla_uf_paciente      LowCardinality(String),
    numero_idade_paciente  UInt8,
    tipo_sexo_paciente     LowCardinality(String),
    nome_raca_cor_paciente LowCardinality(String),
    data_vacina            Date,

    total_doses            SimpleAggregateFunction(sum, UInt64),
    unique_patients        AggregateFunction(uniq, String),
    first_doses            SimpleAggregateFunction(sum, UInt64),
    second_doses           SimpleAggregateFunction(sum, UInt64),
    boosters               SimpleAggregateFunction(sum, UInt64),
    completed_series       SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(data_vacina)
ORDER BY (sigla_uf_paciente, numero_idade_paciente, tipo_sexo_paciente, nome_raca_cor_paciente, data_vacina);

CREATE MATERIALIZED VIEW IF NOT EXISTS vacinas.mv_state_demo
TO vacinas.mv_state_demo_data AS
SELECT
    sigla_uf_paciente,
    numero_idade_paciente,
    tipo_sexo_paciente,
    nome_raca_cor_paciente,
    data_vacina,
    count()                                AS total_doses,
    uniqState(codigo_paciente)             AS unique_patients,
    countIf(codigo_dose_vacina = 1)        AS first_doses,
    countIf(codigo_dose_vacina = 2)        AS second_doses,
    countIf(codigo_dose_vacina >= 3)       AS boosters,
    countIf(codigo_dose_vacina >= 2)       AS completed_series
FROM vacinas.events
GROUP BY
    sigla_uf_paciente,
    numero_idade_paciente,
    tipo_sexo_paciente,
    nome_raca_cor_paciente,
    data_vacina;


-- After the bulk ingest finishes (see vacinas_data/load_clickhouse_from_s3.py),
-- backfill the MVs once from the now-populated events table:
--
-- INSERT INTO vacinas.mv_state_vaccine_date_data
-- SELECT sigla_uf_paciente, sigla_vacina, data_vacina,
--        count(),
--        uniqState(codigo_paciente),
--        countIf(tipo_sexo_paciente='F'),
--        countIf(tipo_sexo_paciente='M'),
--        sum(numero_idade_paciente),
--        count(numero_idade_paciente),
--        countIf(codigo_dose_vacina=1),
--        countIf(codigo_dose_vacina=2),
--        countIf(codigo_dose_vacina>=3),
--        countIf(numero_idade_paciente < 18),
--        countIf(numero_idade_paciente BETWEEN 18 AND 29),
--        countIf(numero_idade_paciente BETWEEN 30 AND 49),
--        countIf(numero_idade_paciente BETWEEN 50 AND 64),
--        countIf(numero_idade_paciente >= 65)
-- FROM vacinas.events
-- GROUP BY sigla_uf_paciente, sigla_vacina, data_vacina;
--
-- INSERT INTO vacinas.mv_state_demo_data
-- SELECT sigla_uf_paciente, numero_idade_paciente, tipo_sexo_paciente,
--        nome_raca_cor_paciente, data_vacina,
--        count(),
--        uniqState(codigo_paciente),
--        countIf(codigo_dose_vacina=1),
--        countIf(codigo_dose_vacina=2),
--        countIf(codigo_dose_vacina>=3),
--        countIf(codigo_dose_vacina>=2)
-- FROM vacinas.events
-- GROUP BY sigla_uf_paciente, numero_idade_paciente, tipo_sexo_paciente,
--          nome_raca_cor_paciente, data_vacina;
