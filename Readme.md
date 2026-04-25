# Data Engineering Workspace Setup Guide

This workspace contains two Airflow projects:

- Batch project: `Batch/airflow-docker`
- Live Data project: `Live Data/airflow-docker`

Both expose Airflow on port `8080`, so run only one at a time.

## 1. Prerequisites

Before starting, make sure you have:

1. Docker Desktop (or Docker Engine) running.
2. Docker Compose v2 available (`docker compose`).
3. VS Code opened on the workspace folder `DataEngineering`.

Recommended resources for Docker:

- Memory: at least 4 GB (6+ GB preferred)
- CPU: at least 2 cores

## 2. Workspace Helpers

This workspace includes helper commands in `bin/`:

- `airflow-start`
- `airflow-stop`
- `airflow-status`
- `compose`

The workspace also sets terminal `PATH` so those commands are available in new VS Code terminals.

If a command is not found, open a new terminal in VS Code.

## 3. First-Time Setup (Shared)

Both projects now use one shared environment file at workspace root:

```bash
cd "DataEngineering"
cat .env
```

Important shared values in `.env`:

- `BATCH_COMPOSE_PROJECT_NAME=de_batch`
- `LIVE_COMPOSE_PROJECT_NAME=de_live`
- `AIRFLOW_UID=50000`
- `AZURE_STORAGE_CONNECTION_STRING=...`
- `AZURE_STORAGE_CONTAINER_BATCH=batch`
- `AZURE_STORAGE_CONTAINER_LIVE=live-data`

If you change values in `.env`, restart the running stack after changes.

## 4. Start Batch Project

```bash
cd "Batch/airflow-docker"
../../bin/airflow-start
```

Then check status:

```bash
../../bin/airflow-status
```

Open Airflow UI:

- http://localhost:8080/

Default login:

- Username: `airflow`
- Password: `airflow`

## 5. Start Live Data Project

```bash
cd "Live Data/airflow-docker"
../../bin/airflow-start
```

Then check status:

```bash
../../bin/airflow-status
```

Open Airflow UI:

- http://localhost:8080/

Default login:

- Username: `airflow`
- Password: `airflow`

## 6. Stop a Running Project

From the project folder that is currently running:

```bash
../../bin/airflow-stop
```

This safely stops and removes containers for that project.

## 7. Switch Between Batch and Live Data

Because both projects use port `8080`, use this flow:

1. Stop current project:

```bash
cd "Batch/airflow-docker"   # or "Live Data/airflow-docker"
../../bin/airflow-stop
```

2. Start the other project:

```bash
cd "Live Data/airflow-docker"   # or "Batch/airflow-docker"
../../bin/airflow-start
```

## 8. Daily Workflow (Quick Version)

### Batch day

```bash
cd "Batch/airflow-docker"
../../bin/airflow-start
```

### Live Data day

```bash
cd "Live Data/airflow-docker"
../../bin/airflow-start
```

### End of day

```bash
cd "Batch/airflow-docker"   # or "Live Data/airflow-docker"
../../bin/airflow-stop
```

## 9. Common Troubleshooting

### Error: No docker-compose.yaml in this folder

You are running a helper from the wrong directory.

Fix: `cd` into either:

- `Batch/airflow-docker`
- `Live Data/airflow-docker`

Then run the command again.

### Error: Port 8080 is already in use

Another container already binds `8080`.

Fix:

1. Stop the currently running Airflow project (`../../bin/airflow-stop` in that project folder).
2. Retry `../../bin/airflow-start`.

### Error: docker command not found

Fix options:

1. Open a new VS Code terminal in this workspace.
2. Use the local project wrapper directly:

```bash
./bin/docker compose ps
```

### Airflow starts slowly or UI does not open immediately

Airflow init may take time (db migration, permissions, package checks). Wait and check:

```bash
../../bin/airflow-status
```

### Docker memory warning

If you see memory warnings during init, increase Docker memory allocation and retry.

## 10. Optional Useful Commands

From inside either project folder:

```bash
../../bin/airflow-status
../../bin/compose ps
../../bin/compose logs -f airflow-apiserver
../../bin/compose logs -f airflow-scheduler
```

Hard reset a project (deletes local Airflow DB volume for that project):

```bash
../../bin/compose down -v
```

Use this only when you intentionally want a clean slate.
