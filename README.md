README.md
uv run python ingest_data.py   --pg-user=root   --pg-pass=root   --pg-host=localhost   --pg-port=5432   --pg-db=ny_taxi   --target-table=yellow_taxi_trips

Dockerizado:
cd pipeline
docker build -t taxi_ingest:v001 .
docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips

Ejecutar dos contenedores en la misma red
# 1. Run PostgreSQL on the network
 docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql    \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18

# In another terminal, run pgAdmin on the same network
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4


  localhost:8085 ->
  server: pgdatabase
  host: pgdatabase

# 2. Docker Compose
  Otra opción: Docker compose. Todo lo que se ejecute en docker compose está en la misma network


# Ejecutar ingestion en compose
If you want to re-run the dockerized ingest script when you run Postgres and pgAdmin with docker-compose, you will have to find the name of the virtual network that Docker compose created for the containers.

# check the network link:
docker network ls

# it's pipeline_default (or similar based on directory name)
# now run the script:
docker run -it \
  --network=pipeline_default \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips

## Benefits of Docker Compose
Single command to start all services
Automatic network creation
Easy configuration management
Declarative infrastructure