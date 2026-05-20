"""Data transformation module for converting API responses to DB format."""
import logging
from datetime import datetime, date
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms API response data to ClickHouse table format."""

    # Fields that need date parsing
    DATE_FIELDS = {"data_vacina", "data_deletado_rnds", "data_entrada_rnds"}

    # Fields that should be nullable strings
    NULLABLE_STRING_FIELDS = {
        "codigo_origem_registro",
        "codigo_vacina_fabricante",
        "codigo_condicao_maternal",
        "codigo_etnia_indigena_paciente",
        "codigo_vacina_categoria_atendimento",
        "codigo_vacina_grupo_atendimento",
        "descricao_condicao_maternal",
        "descricao_vacina_grupo_atendimento",
        "descricao_origem_registro",
        "descricao_vacina_categoria_atendimento",
        "nome_etnia_indigena_paciente",
        "codigo_troca_documento",
    }

    # Required string fields (empty string if null)
    REQUIRED_STRING_FIELDS = {
        "descricao_natureza_estabelecimento",
        "codigo_via_administracao",
        "nome_pais_paciente",
        "codigo_pais_paciente",
        "nome_raca_cor_paciente",
        "sigla_vacina",
        "nome_razao_social_estabelecimento",
        "sigla_uf_estabelecimento",
        "nome_municipio_estabelecimento",
        "codigo_sistema_origem",
        "status_documento",
        "descricao_tipo_estabelecimento",
        "codigo_documento",
        "codigo_municipio_estabelecimento",
        "descricao_estrategia_vacinacao",
        "nome_uf_paciente",
        "numero_cep_paciente",
        "descricao_local_aplicacao",
        "codigo_lote_vacina",
        "codigo_cnes_estabelecimento",
        "descricao_vacina_fabricante",
        "codigo_tipo_estabelecimento",
        "codigo_natureza_estabelecimento",
        "codigo_raca_cor_paciente",
        "codigo_paciente",
        "descricao_sistema_origem",
        "codigo_municipio_paciente",
        "nome_municipio_paciente",
        "nome_fantasia_estalecimento",
        "codigo_local_aplicacao",
        "sigla_uf_paciente",
        "nome_uf_estabelecimento",
        "codigo_vacina",
        "descricao_via_administracao",
        "codigo_estrategia_vacinacao",
        "descricao_vacina",
        "tipo_sexo_paciente",
        "descricao_nacionalidade_paciente",
        "codigo_dose_vacina",
        "descricao_dose_vacina",
    }

    def __init__(self) -> None:
        """Initialize transformer."""
        pass

    def _parse_date(self, value: Optional[str]) -> Optional[date]:
        """Parse date string to date object."""
        if value is None or value == "":
            return None

        # Handle format: "2025-01-06 00:00:00-03"
        try:
            # Remove timezone suffix and parse
            date_str = value.split(" ")[0]
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to parse date '{value}': {e}")
            return None

    def _clean_string(self, value: Any, nullable: bool = False) -> Optional[str]:
        """Clean string value."""
        if value is None:
            return None if nullable else ""
        return str(value).strip()

    def _clean_int(self, value: Any, default: int = 0) -> int:
        """Clean integer value."""
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single API record to ClickHouse format.

        Args:
            record: Raw API record

        Returns:
            Transformed record ready for ClickHouse insertion
        """
        transformed = {
            "Unnamed: 0": 0,
        }

        # Process each field
        for field in self.REQUIRED_STRING_FIELDS:
            transformed[field] = self._clean_string(record.get(field), nullable=False)

        for field in self.NULLABLE_STRING_FIELDS:
            transformed[field] = self._clean_string(record.get(field), nullable=True)

        for field in self.DATE_FIELDS:
            transformed[field] = self._parse_date(record.get(field))

        # Handle numeric field
        transformed["numero_idade_paciente"] = self._clean_int(
            record.get("numero_idade_paciente"), default=0
        )

        return transformed

    def transform_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform a batch of records.

        Args:
            records: List of raw API records

        Returns:
            List of transformed records
        """
        transformed = []
        for record in records:
            try:
                transformed.append(self.transform_record(record))
            except Exception as e:
                logger.error(f"Failed to transform record: {e}")
                # Continue with other records

        return transformed
