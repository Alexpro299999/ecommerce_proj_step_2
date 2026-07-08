# 🚀 E-commerce Analytics Pipeline

This repository is a practical trainee assignment for a modern Airflow-based ETL pipeline.
The current implementation uses Apache Airflow to orchestrate a daily report workflow that extracts nested event data, enriches it from PostgreSQL, transforms it with Pandas, and writes CSV reports.

This is not a simple script exercise. You should understand the pipeline structure, the separation between orchestration and business logic, and the way Airflow passes execution metadata into tasks.

## 🎯 What You Will Learn

- **Airflow TaskFlow API:** Define DAGs and tasks with decorators.
- **DAG design:** Keep business logic out of the DAG file and use adapter/task wrapper functions.
- **File-based run isolation:** Use the Airflow logical date (`ds`) to keep each run separate.
- **PostgreSQL extraction:** Load `customers` and `products` tables from the database.
- **Pandas transformation:** Join, filter, aggregate, and write final reports.
- **Cleanup:** Remove temporary artifacts after the DAG completes.

---

## 📦 Current Architecture

The pipeline is organized as follows:

- `dags/ecommerce_dag.py` — Airflow DAG definition and task orchestration.
- `pipeline/airflow_tasks.py` — lightweight wrappers that execute the core ETL work.
- `pipeline/config.py` — Pydantic settings for `data_dir`, `tmp_dir`, and `reports_dir`.
- `data_generator.py` — creates nested zip event archives for local testing.
- `sql/init.sql` — initializes the PostgreSQL database with customers and products.

The DAG currently defines five tasks:

1. `extract_files_task` — extracts event data from nested ZIP archives.
2. `extract_db_task` — extracts customers and products from PostgreSQL.
3. `transform_task` — joins, filters, and aggregates sales data.
4. `load_task` — writes final CSV output to the `reports/` folder.
5. `cleanup_task` — removes temporary files even if the DAG fails.

---

## 🧠 Data Flow and Requirements

### Data sources

1. **File source:** nested ZIP files under `data/`

   - top-level ZIP contains daily archives
   - each daily archive contains JSON part-files
   - event records include `timestamp`, `customer_id`, `event_type`, `product_id`, `quantity`
2. **Database source:** PostgreSQL tables

   - `customers` contains customer metadata and segment
   - `products` contains product metadata, price, and category

### Business goal

Produce a sales report that shows revenue and volume by:

- `category`
- `customer_segment`

The report should include:

- `total_revenue`
- `units_sold`
- `unique_customers`

---

## ⚙️ Environment Setup

This project uses Docker Compose for PostgreSQL and Airflow.
The service configuration is already defined in `docker-compose.yml`.

### Start the stack

```bash
# Start the services in the background
docker compose up -d
```

### Airflow UI

Open the UI at:

- `http://localhost:8081`

Use the built-in credentials:

- **Username:** `admin`
- **Password:** `admin`

### Local Python environment

Use `uv` if available, otherwise install dependencies using the normal Python workflow.

```bash
uv sync
source .venv/bin/activate
```

If you do not use `uv`, install the dependencies from `pyproject.toml`.

---

## 🧪 Running the Project

### Generate the event dataset

```bash
python data_generator.py -c 50
```

This creates the `data/` folder and nested event archives for the pipeline to process.

### Run unit tests

```bash
python -m pytest -q
```

### Inspect Airflow DAGs

The DAG is loaded from `dags/ecommerce_dag.py`.
Look for `ecommerce_daily_report` in the Airflow UI.

---

## 🛠️ Task Assignment

Your work should be focused on the following responsibilities:

- Keep the DAG file small and declarative.
- Put extraction, transformation, and loading logic into `pipeline/` modules.
- Use the Airflow execution date (`ds`) to create per-run temporary directories.
- Persist intermediate CSV files under `data/tmp/<run_date>/`.
- Keep the final output in `reports/`.
- Ensure `cleanup_task` removes temporary artifacts with `trigger_rule='all_done'`.

### Expected behavior

- The pipeline should be idempotent: rerunning the same DAG date must not overwrite unrelated runs.
- Temporary files should be removed after execution.
- The final report should be easy to inspect in `reports/`.

---

## 📁 Important Paths

- `data/` — source event archives and temporary run data
- `data/tmp/` — intermediate files for each `ds` run
- `reports/` — final CSV outputs
- `dags/` — Airflow DAG definitions
- `pipeline/` — ETL task adapters and pipeline logic

---

## 🚀 What to Deliver

For this task, the trainee should provide:

- A working Airflow DAG in `dags/ecommerce_dag.py`
- ETL logic in `pipeline/airflow_tasks.py` and related pipeline modules
- A generated report in `reports/sales_report.csv`
- Clean, type-hinted Python code with docstrings
- A working `cleanup_task` that preserves disk space

---

## ⭐ Bonus improvements

If you want to go further, try these enhancements:

- Add more robust logging to the pipeline.
- Add integration tests for the DAG logic.
- Create one CSV file per product category in `reports/`.
- Use `pytest` fixtures to mock `db` and file extraction during tests.
