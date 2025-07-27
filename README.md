# data-engineering-tech-test-frejosaru

**Ejercicio 1: Orquestación local** — Pipeline ETL con **Airflow** que:
- Descarga/lee un CSV de 1M filas (`sample_transactions.csv`), soporta `.csv.gz`.
- Transforma los datos en Python.
- Carga a **SQLite** (cambiable a PostgreSQL).
- Implementa **sensores** (existencia y tamaño mínimo del archivo), **reintentos**, **validación post-carga** y **alertas simuladas**.
- Modularizado en paquetes reutilizables.
- Incluye **tests unitarios e integración**.
- (Opcional) Réplica del pipeline en **Prefect** para comparar enfoques.
- **Lectura por chunks**, métricas simples y compatibilidad con archivos comprimidos.

## Indicaciones importantes (aplicadas)
- No es necesario completar todo: se prioriza **calidad, estructura, documentación y decisiones técnicas**.
- Se documentan **supuestos y decisiones** en este README y en el código.
- **Modularidad**, **manejo de errores**, **tests** y **logs estructurados** incluidos.
- Se agregan extras como CI mínimo (GitHub Actions), Makefile y compatibilidad Docker.

---

## Estructura

```text
data-engineering-tech-test-frejosaru/
├── README.md
├── requirements.txt
├── Makefile
├── .gitignore
├── .env.example
├── airflow/
│   ├── dags/
│   │   └── etl_transactions_dag.py
│   └── plugins/
├── src/
│   ├── config.py
│   ├── common/
│   │   ├── exceptions.py
│   │   └── logger.py
│   ├── etl/
│   │   ├── extract.py
│   │   ├── transform.py
│   │   └── load.py
│   └── validations/
│       └── schemas.py   # Placeholder para expansión
├── tests/
│   ├── test_dag_structure.py
│   ├── test_etl_units.py
│   └── test_integration_etl.py
├── prefect/
│   └── flow_transactions.py   # opcional
└── .github/
    └── workflows/
        └── ci.yml
```

---

## Variables de entorno

Copia `.env.example` a `.env` y ajusta según tu entorno:

```env
CSV_PATH=/mnt/data/sample_transactions.csv    # cámbialo a tu ruta local
SQLALCHEMY_URL=sqlite:////tmp/transactions.db
TABLE_NAME=transactions
CHUNKSIZE=100000
MIN_FILE_SIZE_BYTES=1024
```

---

## Cómo correr Airflow local

**Opción rápida (standalone):**

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags

make airflow-standalone
```

Abre la UI: http://localhost:8080 y ejecuta el DAG `etl_transactions_local` manualmente.

---

## Tests

```bash
make test
```

---

## CI (GitHub Actions)

Cada push/pull request ejecuta lint + tests (workflow en `.github/workflows/ci.yml`).

---

## (Opcional) Prefect

Ejecuta el flujo para comparar:

```bash
python prefect/flow_transactions.py
```

---

## Próximos pasos / Ideas de mejora

- Alertas reales (EmailOperator / Slack).
- Observabilidad (Prometheus + Grafana / StatsD).
- docker-compose con Airflow + Postgres + Adminer.
- Great Expectations / pandera para validaciones de datos.
- Orquestación en la nube (MWAA, GCP Composer) o Prefect Cloud.
- CDC / cargas incrementales.
- dbt para capa de transformación analítica.
