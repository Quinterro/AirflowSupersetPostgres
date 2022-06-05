# Airflow + Superset + Postgres

## Uruchamianie

W celu uruchomienia należy:

Sklonować repozytorium:
```
git clone git@github.com:Quinterro/AirflowSupersetPostgres
cd AirflowSupersetPostgres
```

W celu uruchomienia Apache Airflow wymagane jest wprowadzenie ID aktualnego użytkownika do pliku
o nazwie: `.env`. Systemy Linux pozwalają na sprawdzenie ID użytkownika dzięki komendzie:
```sh
id -u
```
Plik `.env` wyglądać np. w ten sposób:
```text
AIRFLOW_UID=1000
```

Aby wszystko poprawnie uruchomić należy wprowadzić poniższe polecenie (po uprzednim zainstalowaniu Dockera):
```sh
docker compose up
```
W celu umieszczenia wykorzystanych danych w bazie PostgreSQL należy uruchomić poniższe polecenie:
```sh
python init-postgres.py
```
W celu skorzystania z dostępnych usług należy wejść przez poniższe linki:

- [pgAdmin](http://localhost:5050)
- [Airflow](http://localhost:5053)
- [Superset](http://localhost:5054)

Adres bazy danych klienta (pgAdmin): `client-postgres:5432`

Konfiguracja bazy i pgAdmin:
``` text
POSTGRES_USER: postgres
POSTGRES_PASSWORD: postgres
POSTGRES_DB: postgres
PGADMIN_DEFAULT_EMAIL: admin@admin.com
PGADMIN_DEFAULT_PASSWORD: admin
```