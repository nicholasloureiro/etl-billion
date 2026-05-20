"""Synthetic vaccination data generator with statistical accuracy."""
import json
import logging
import random
import uuid
import hashlib
from datetime import date, timedelta
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
from calendar import monthrange

from database import ClickHouseConnection, ClickHouseRepository
from config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class WeightedDistribution:
    """Handles weighted random selection based on real data distributions."""
    items: List[Any]
    weights: List[int]

    def __post_init__(self):
        total = sum(self.weights)
        self.probabilities = [w / total for w in self.weights]

    def sample(self) -> Any:
        return random.choices(self.items, weights=self.probabilities, k=1)[0]

    def sample_batch(self, n: int) -> List[Any]:
        return random.choices(self.items, weights=self.probabilities, k=n)


class SyntheticDataGenerator:
    """Generates synthetic vaccination data maintaining statistical distributions."""

    # Day weights to simulate weekday/weekend patterns (Mon-Sun pattern repeated)
    # Lower weights for weekends
    WEEKDAY_WEIGHTS = [0.18, 0.18, 0.18, 0.18, 0.16, 0.06, 0.06]

    # Monthly seasonality (vaccination campaigns often peak in certain months)
    MONTHLY_WEIGHTS = {
        1: 1.0,   # Jan (base)
        2: 0.95,  # Feb
        3: 1.05,  # Mar
        4: 1.10,  # Apr (autumn campaigns)
        5: 1.15,  # May (flu season)
        6: 1.10,  # Jun
        7: 0.95,  # Jul
        8: 1.05,  # Aug (children's vaccination)
        9: 1.10,  # Sep
        10: 1.05, # Oct
        11: 0.95, # Nov
        12: 0.85, # Dec (holidays)
    }

    def __init__(self, distributions_file: str = "distributions.json"):
        with open(distributions_file, "r") as f:
            self.distributions = json.load(f)

        self._setup_distributions()
        logger.info("SyntheticDataGenerator initialized")

    def _setup_distributions(self):
        """Setup weighted distributions from loaded data."""
        # UF distribution: (sigla, nome, count)
        self.uf_dist = WeightedDistribution(
            items=[(d[0], d[1]) for d in self.distributions["uf"]],
            weights=[d[2] for d in self.distributions["uf"]]
        )

        # Vaccine distribution: (sigla, descricao, codigo, count)
        self.vacina_dist = WeightedDistribution(
            items=[(d[0], d[1], d[2]) for d in self.distributions["vacina"]],
            weights=[d[3] for d in self.distributions["vacina"]]
        )

        # Dose distribution: (codigo, descricao, count)
        self.dose_dist = WeightedDistribution(
            items=[(d[0], d[1]) for d in self.distributions["dose"]],
            weights=[d[2] for d in self.distributions["dose"]]
        )

        # Municipio distribution: (codigo, nome, uf, count)
        self.municipio_dist = WeightedDistribution(
            items=[(d[0], d[1], d[2]) for d in self.distributions["municipio"]],
            weights=[d[3] for d in self.distributions["municipio"]]
        )

        # Tipo estabelecimento: (codigo, descricao, count)
        self.tipo_estab_dist = WeightedDistribution(
            items=[(d[0], d[1]) for d in self.distributions["tipo_estabelecimento"]],
            weights=[d[2] for d in self.distributions["tipo_estabelecimento"]]
        )

        # Fabricante: (codigo, descricao, count)
        self.fabricante_dist = WeightedDistribution(
            items=[(d[0], d[1]) for d in self.distributions["fabricante"]],
            weights=[d[2] for d in self.distributions["fabricante"]]
        )

        # Race: (codigo, nome, count)
        self.raca_dist = WeightedDistribution(
            items=[(d[0], d[1]) for d in self.distributions["raca"]],
            weights=[d[2] for d in self.distributions["raca"]]
        )

        # Age: (idade, count)
        self.idade_dist = WeightedDistribution(
            items=[d[0] for d in self.distributions["idade"]],
            weights=[d[1] for d in self.distributions["idade"]]
        )

        # Sex distribution (from analysis: F=51.68%, M=48.31%, I=0.01%)
        self.sex_dist = WeightedDistribution(
            items=["F", "M", "I"],
            weights=[5168, 4831, 1]
        )

        # Natureza estabelecimento
        self.natureza_dist = WeightedDistribution(
            items=[
                ("1", "ADMINISTRACAO PUBLICA"),
                ("2", "ENTIDADES EMPRESARIAIS"),
                ("3", "ENTIDADES SEM FINS LUCRATIVOS"),
                ("4", "PESSOAS FISICAS"),
            ],
            weights=[9466, 431, 102, 1]
        )

        # Estrategia vacinacao
        self.estrategia_dist = WeightedDistribution(
            items=[
                ("1", "Rotina"),
                ("2", "Especial"),
                ("8", "Serviço Privado"),
                ("", ""),
                ("3", "Pós-exposição"),
                ("4", "Campanha indiscriminada"),
                ("5", "Intensificação"),
                ("6", "Campanha seletiva"),
                ("7", "Monitoramento rápido de cobertura vacinal"),
                ("9", "Pesquisa"),
            ],
            weights=[8948, 378, 377, 113, 83, 32, 20, 13, 11, 10]
        )

        # Via administracao
        self.via_admin_dist = WeightedDistribution(
            items=[
                ("0", "Sem registro no sistema de informação de origem"),
                ("10916", "Subcutânea"),
                ("10915", "Intramuscular"),
                ("10918", "Oral"),
                ("10917", "Intradérmica"),
            ],
            weights=[60, 15, 15, 8, 2]
        )

        # Local aplicacao
        self.local_aplic_dist = WeightedDistribution(
            items=[
                ("0", "Sem registro no sistema de informação de origem"),
                ("14", "Face Externa Superior do Braço Direito"),
                ("15", "Face Externa Superior do Braço Esquerdo"),
                ("2", "Deltóide Esquerdo"),
                ("1", "Deltóide Direito"),
                ("7", "Vasto Lateral da Coxa Direita"),
                ("8", "Vasto Lateral da Coxa Esquerda"),
                ("17", "Glúteo"),
            ],
            weights=[50, 12, 12, 8, 8, 4, 4, 2]
        )

    def _generate_date(self, year: int, month: int) -> date:
        """Generate a random date within the month with weekday bias."""
        _, days_in_month = monthrange(year, month)

        # Generate weighted by day of week
        day = random.randint(1, days_in_month)
        d = date(year, month, day)

        # Apply weekday weight - resample if weekend with some probability
        dow = d.weekday()
        if dow >= 5:  # Weekend
            if random.random() > 0.12:  # 88% chance to resample to weekday
                day = random.randint(1, days_in_month)
                d = date(year, month, day)
                while d.weekday() >= 5:
                    day = random.randint(1, days_in_month)
                    d = date(year, month, day)

        return d

    def _generate_patient_id(self) -> str:
        """Generate a hashed patient ID."""
        return hashlib.sha256(uuid.uuid4().bytes).hexdigest()

    def _generate_document_id(self) -> str:
        """Generate a document ID in the expected format."""
        return f"{uuid.uuid4()}-i0b0"

    def _generate_cnes(self) -> str:
        """Generate a random CNES code."""
        return str(random.randint(1000000, 9999999))

    def _generate_lote(self) -> str:
        """Generate a vaccine lot code."""
        prefix = random.choice(["23", "24", "25"])
        letters = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=4))
        numbers = str(random.randint(1, 999)).zfill(3)
        suffix = random.choice(["NA", "NB", "NC", ""])
        return f"{prefix}{letters}{numbers}{suffix}"

    def _generate_cep(self) -> str:
        """Generate a CEP prefix (5 digits)."""
        return str(random.randint(10000, 99999))

    def _generate_sistema_origem(self) -> Tuple[str, str]:
        """Generate sistema origem code and description."""
        sistemas = [
            ("18602", "ESUS APS - NACIONAL (OFFLINE)"),
            ("29376", "SIGA Saúde"),
            ("39474", "Prontuário Eletrônico do Cidadão / e-SUS APS"),
            ("18822", "SIMUS"),
            ("36307", "Prontuário Eletrônico do Cidadão / e-SUS APS"),
            ("38976", "Snak Sistemas"),
        ]
        return random.choice(sistemas)

    def generate_record(self, data_vacina: date) -> Dict[str, Any]:
        """Generate a single synthetic record."""
        # Sample from distributions
        uf_sigla, uf_nome = self.uf_dist.sample()
        vacina_sigla, vacina_desc, vacina_cod = self.vacina_dist.sample()
        dose_cod, dose_desc = self.dose_dist.sample()
        mun_cod, mun_nome, mun_uf = self.municipio_dist.sample()
        tipo_estab_cod, tipo_estab_desc = self.tipo_estab_dist.sample()
        raca_cod, raca_nome = self.raca_dist.sample()
        idade = self.idade_dist.sample()
        sexo = self.sex_dist.sample()
        natureza_cod, natureza_desc = self.natureza_dist.sample()
        estrategia_cod, estrategia_desc = self.estrategia_dist.sample()
        via_cod, via_desc = self.via_admin_dist.sample()
        local_cod, local_desc = self.local_aplic_dist.sample()
        sistema_cod, sistema_desc = self._generate_sistema_origem()

        # Fabricante (nullable - 70% chance of having one)
        if random.random() < 0.7:
            fab_cod, fab_desc = self.fabricante_dist.sample()
        else:
            fab_cod, fab_desc = None, ""

        # Generate entry date (1-60 days after vaccination)
        data_entrada = data_vacina + timedelta(days=random.randint(1, 60))

        return {
            "Unnamed: 0": 0,
            "descricao_natureza_estabelecimento": natureza_desc,
            "codigo_via_administracao": via_cod,
            "nome_pais_paciente": "BRASIL",
            "codigo_origem_registro": None,
            "codigo_pais_paciente": "10",
            "nome_raca_cor_paciente": raca_nome,
            "sigla_vacina": vacina_sigla,
            "codigo_vacina_fabricante": fab_cod,
            "data_vacina": data_vacina,
            "codigo_condicao_maternal": None,
            "nome_razao_social_estabelecimento": f"ESTABELECIMENTO DE SAUDE {mun_nome}",
            "sigla_uf_estabelecimento": mun_uf,
            "nome_municipio_estabelecimento": mun_nome,
            "codigo_sistema_origem": sistema_cod,
            "status_documento": "final",
            "descricao_tipo_estabelecimento": tipo_estab_desc,
            "codigo_documento": self._generate_document_id(),
            "codigo_municipio_estabelecimento": mun_cod,
            "descricao_estrategia_vacinacao": estrategia_desc,
            "data_deletado_rnds": None,
            "nome_uf_paciente": uf_nome,
            "numero_cep_paciente": self._generate_cep(),
            "codigo_etnia_indigena_paciente": None,
            "codigo_vacina_categoria_atendimento": None,
            "descricao_local_aplicacao": local_desc,
            "numero_idade_paciente": idade,
            "codigo_lote_vacina": self._generate_lote(),
            "codigo_cnes_estabelecimento": self._generate_cnes(),
            "descricao_vacina_fabricante": fab_desc,
            "codigo_tipo_estabelecimento": tipo_estab_cod,
            "codigo_natureza_estabelecimento": natureza_cod,
            "codigo_raca_cor_paciente": raca_cod,
            "codigo_vacina_grupo_atendimento": None,
            "codigo_paciente": self._generate_patient_id(),
            "descricao_sistema_origem": sistema_desc,
            "codigo_municipio_paciente": mun_cod,
            "nome_municipio_paciente": mun_nome,
            "nome_fantasia_estalecimento": f"UBS {mun_nome}",
            "descricao_condicao_maternal": None,
            "descricao_vacina_grupo_atendimento": None,
            "codigo_local_aplicacao": local_cod,
            "sigla_uf_paciente": uf_sigla,
            "nome_uf_estabelecimento": uf_nome,
            "codigo_vacina": vacina_cod,
            "descricao_via_administracao": via_desc,
            "codigo_estrategia_vacinacao": estrategia_cod,
            "descricao_vacina": vacina_desc,
            "descricao_origem_registro": None,
            "data_entrada_rnds": data_entrada,
            "descricao_vacina_categoria_atendimento": None,
            "nome_etnia_indigena_paciente": None,
            "tipo_sexo_paciente": sexo,
            "descricao_nacionalidade_paciente": "B",
            "codigo_troca_documento": None,
            "codigo_dose_vacina": dose_cod,
            "descricao_dose_vacina": dose_desc,
        }

    def generate_batch(self, year: int, month: int, batch_size: int) -> List[Dict[str, Any]]:
        """Generate a batch of records for a specific month."""
        records = []
        for _ in range(batch_size):
            data_vacina = self._generate_date(year, month)
            records.append(self.generate_record(data_vacina))
        return records


def main():
    """Generate synthetic data for Feb-Dec 2025."""
    config = load_config()
    conn = ClickHouseConnection(config.clickhouse)
    repo = ClickHouseRepository(conn)
    generator = SyntheticDataGenerator()

    # Get current count dynamically
    TARGET_TOTAL = 50_000_000
    current_count = repo.count_records()
    logger.info(f"Current row count: {current_count:,}")

    NEEDED = TARGET_TOTAL - current_count
    if NEEDED <= 0:
        logger.info("Target already reached!")
        return

    logger.info(f"Need to generate: {NEEDED:,} records")

    # Base per month, adjusted by seasonality
    base_per_month = NEEDED / 11

    BATCH_SIZE = 50_000  # Larger batches for better throughput
    total_inserted = 0

    months = list(range(2, 13))  # Feb to Dec

    for month in months:
        # Adjust count by monthly seasonality
        month_weight = generator.MONTHLY_WEIGHTS[month]
        month_target = int(base_per_month * month_weight)

        logger.info(f"Generating {month_target:,} records for 2025-{month:02d}")
        month_inserted = 0

        while month_inserted < month_target:
            batch_size = min(BATCH_SIZE, month_target - month_inserted)
            batch = generator.generate_batch(2025, month, batch_size)

            try:
                repo.insert_batch(batch, max_retries=3)
                month_inserted += batch_size
                total_inserted += batch_size

                if month_inserted % 500_000 == 0:
                    logger.info(f"  Month {month:02d}: {month_inserted:,}/{month_target:,} "
                              f"(Total: {total_inserted:,}/{NEEDED:,})")
            except Exception as e:
                logger.error(f"Failed to insert batch: {e}")
                raise

        logger.info(f"Completed month {month:02d}: {month_inserted:,} records")

    logger.info(f"Synthetic data generation complete! Total inserted: {total_inserted:,}")

    # Verify final count
    final_count = repo.count_records()
    logger.info(f"Final table count: {final_count:,}")


if __name__ == "__main__":
    main()
