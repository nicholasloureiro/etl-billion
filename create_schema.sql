-- Create database
CREATE DATABASE IF NOT EXISTS vacinas;

-- Create table
CREATE TABLE IF NOT EXISTS vacinas.events
(
    `Unnamed: 0` UInt32,

    `descricao_natureza_estabelecimento` String,

    `codigo_via_administracao` String,

    `nome_pais_paciente` String,

    `codigo_origem_registro` Nullable(String),

    `codigo_pais_paciente` String,

    `nome_raca_cor_paciente` String,

    `sigla_vacina` String,

    `codigo_vacina_fabricante` Nullable(String),

    `data_vacina` Date,

    `codigo_condicao_maternal` Nullable(String),

    `nome_razao_social_estabelecimento` String,

    `sigla_uf_estabelecimento` String,

    `nome_municipio_estabelecimento` String,

    `codigo_sistema_origem` String,

    `status_documento` String,

    `descricao_tipo_estabelecimento` String,

    `codigo_documento` String,

    `codigo_municipio_estabelecimento` String,

    `descricao_estrategia_vacinacao` String,

    `data_deletado_rnds` Nullable(Date),

    `nome_uf_paciente` String,

    `numero_cep_paciente` String,

    `codigo_etnia_indigena_paciente` Nullable(String),

    `codigo_vacina_categoria_atendimento` Nullable(String),

    `descricao_local_aplicacao` String,

    `numero_idade_paciente` UInt16,

    `codigo_lote_vacina` String,

    `codigo_cnes_estabelecimento` String,

    `descricao_vacina_fabricante` String,

    `codigo_tipo_estabelecimento` String,

    `codigo_natureza_estabelecimento` String,

    `codigo_raca_cor_paciente` String,

    `codigo_vacina_grupo_atendimento` Nullable(String),

    `codigo_paciente` String,

    `descricao_sistema_origem` String,

    `codigo_municipio_paciente` String,

    `nome_municipio_paciente` String,

    `nome_fantasia_estalecimento` String,

    `descricao_condicao_maternal` Nullable(String),

    `descricao_vacina_grupo_atendimento` Nullable(String),

    `codigo_local_aplicacao` String,

    `sigla_uf_paciente` String,

    `nome_uf_estabelecimento` String,

    `codigo_vacina` String,

    `descricao_via_administracao` String,

    `codigo_estrategia_vacinacao` String,

    `descricao_vacina` String,

    `descricao_origem_registro` Nullable(String),

    `data_entrada_rnds` Nullable(Date),

    `descricao_vacina_categoria_atendimento` Nullable(String),

    `nome_etnia_indigena_paciente` Nullable(String),

    `tipo_sexo_paciente` String,

    `descricao_nacionalidade_paciente` String,

    `codigo_troca_documento` Nullable(String),

    `codigo_dose_vacina` String,

    `descricao_dose_vacina` String,

    INDEX idx_state sigla_uf_paciente TYPE set(0) GRANULARITY 1,

    INDEX idx_vac sigla_vacina TYPE set(0) GRANULARITY 1,

    INDEX idx_dose codigo_dose_vacina TYPE set(0) GRANULARITY 1,

    INDEX idx_sex tipo_sexo_paciente TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY `Unnamed: 0`
SETTINGS index_granularity = 8192;
