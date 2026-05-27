# LeafData

LeafData is an Apache Airflow based file-transfer and data-orchestration service. It includes custom Airflow operators, a Flask API for creating and triggering pipeline definitions, Docker Compose services for local development, and sample data for testing file/database workflows.

## Prerequisites

- Git
- Docker Desktop or Docker Engine with Docker Compose
- At least 4 GB RAM available to Docker
- Python 3.8+ if you want to run API scripts outside Docker

## Clone the Repository

```bash
git clone https://github.com/FontsLabs-Morphus/LeafData.git
cd LeafData
```

## Environment Setup

Create a `.env` file in the project root. Do not commit this file.

```env
AIRFLOW_UID=50000
AIRFLOW__WEBSERVER__SECRET_KEY=replace-with-a-long-random-secret

FT_DB_HOST=postgres
FT_DB_PORT=5432
FT_DB_NAME=file_transfer
FT_DB_USER=airflow
FT_DB_PASSWORD=airflow
OPENAI_API_KEY=optional-for-chatbot-features
```

On Linux, set `AIRFLOW_UID` to your local user ID:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

On Windows or macOS, `AIRFLOW_UID=50000` is usually fine for local Docker usage.

## Run with Docker Compose

Build and start Airflow, Postgres, Redis, the Flask API, demo MySQL, and demo SFTP services:

```bash
docker compose up --build -d
```

Check service status:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f airflow-webserver
docker compose logs -f web
```

Stop the stack:

```bash
docker compose down
```

Reset local containers and volumes:

```bash
docker compose down -v
```

## Local URLs and Credentials

- Airflow UI: http://localhost:8080
- Flask API: http://localhost:5001
- Demo MySQL: `localhost:3306`, root password `root`
- Demo SFTP: `localhost:2222`, username `foo`, password `pass`

The default Airflow login created by Docker Compose is:

```text
Username: airflow
Password: airflow
```

## API Endpoints

The Flask API runs in the `web` service and exposes:

- `POST /connection` - create an Airflow connection
- `PUT /connection` - update an Airflow connection
- `DELETE /connection` - delete an Airflow connection
- `POST /dag` - create a pipeline JSON definition
- `PUT /dag` - update a pipeline JSON definition
- `DELETE /dag` - delete a pipeline JSON definition
- `POST /triggerdag` - trigger an Airflow DAG
- `GET /statusdag?id=<pipelineId>` - check the latest DAG run state

Request bodies are validated against the Avro schemas in `api/avro_schema`.

## Useful Commands

Open a shell in the Airflow webserver container:

```bash
docker compose exec airflow-webserver bash
```

List Airflow DAGs:

```bash
docker compose exec airflow-webserver airflow dags list
```

Trigger a DAG manually:

```bash
docker compose exec airflow-webserver airflow dags trigger file_transfer
```

Run the Flask API locally without Docker, if dependencies are installed and Docker is available:

```bash
cd api
pip install -r requirements.txt
python main.py
```

## Project Structure

```text
api/                 Flask API and Avro schema validation
dags/                Airflow DAG definitions
plugins/             Custom Airflow operators, loggers, and helpers
scripts/             Airflow helper scripts executed by the API
dag_json_data/       Pipeline JSON definitions loaded by the DAG
demo-data/           Local demo SFTP data
docker-compose.yaml  Local Airflow/API service stack
Dockerfile           Custom Airflow image
requirements.txt     Airflow Python dependencies
```

## Notes

- Keep `.env`, logs, local databases, and generated cache files out of Git.
- If ports `8080`, `5001`, `5433`, `3306`, or `2222` are already in use, update `docker-compose.yaml` before starting the stack.
- The API container mounts the Docker socket so it can execute scripts inside the Airflow webserver container.
