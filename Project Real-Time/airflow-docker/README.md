# Airflow Docker Project

This project runs Apache Airflow with Docker Compose.

## Next Time You Open This Project

1. Open VS Code and open this folder:
   `airflow-docker`

2. Open a **new terminal inside VS Code**.
   This matters because this workspace adds a project-local Docker wrapper to the terminal `PATH`.

   If `docker` is still not found, use the project-local wrapper directly:

   ```bash
   ./bin/docker compose ps
   ```

3. Confirm the environment file exists:

   ```bash
   cat .env
   ```

   Expected output:

   ```text
   AIRFLOW_UID=50000
   ```

4. If this is the first start after cloning the project, or if you want to rebuild the Airflow metadata setup, run:

   ```bash
   docker compose up airflow-init
   ```

   Wait until it finishes successfully.

5. Start Airflow:

   ```bash
   docker compose up
   ```

   Fallback if `docker` is not found:

   ```bash
   ./bin/docker compose up
   ```

   Keep that terminal open while you use Airflow.

6. Open the Airflow UI:

   http://localhost:8080

7. Log in with:

   ```text
   username: airflow
   password: airflow
   ```

## Normal Daily Start

If you already initialized the project before, these are the usual commands:

```bash
docker compose up
```

Then open:

http://localhost:8080

## Useful Checks

Check running containers:

```bash
docker compose ps
```

Check the web server quickly:

```bash
curl -I http://localhost:8080
```

If Airflow is up, you should get an HTTP response back.

## Writer Outputs (Local + Azure Blob)

The `load` task writes transformed parquet data to:

1. Local output folder: `/opt/airflow/data/output` (mapped to `data/output` in this project)
2. Azure Blob Storage container and prefix configured via `.env`

Add these variables to `.env`:

```text
AZURE_STORAGE_CONNECTION_STRING=<your-connection-string>
AZURE_STORAGE_CONTAINER=<your-container-name>
AZURE_STORAGE_BLOB_PREFIX=yellow-taxi
```

Dependency for Azure upload is installed through:

```text
_PIP_ADDITIONAL_REQUIREMENTS=redis==5.2.1 azure-storage-blob==12.25.1
```

After changing `.env`, restart Airflow services so containers pick up new values:

```bash
docker compose down
docker compose up -d
```

## Safe Shutdown

To stop the running stack safely, open a terminal in this project and run:

```bash
docker compose down
```

Fallback if `docker` is not found:

```bash
./bin/docker compose down
```

This stops and removes the containers but keeps your project files and persisted database volume.

If `docker compose up` is running in the foreground, you can also press `Ctrl+C` in that terminal first, then run:

```bash
docker compose down
```

## Optional Cleanup

If you want to stop everything and also remove the stored Postgres data, run:

```bash
docker compose down -v
```

Only do this if you want a clean reset, because it removes the Airflow database state.

## Important Note For This Workspace

This project is configured so Docker works from inside this VS Code workspace.

If `docker` does not work in an older terminal, close it and open a **new VS Code terminal** in this folder before running the commands again.

If you are using an older terminal or a terminal outside VS Code, run the wrapper directly with `./bin/docker`.
