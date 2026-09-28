# Plateforme de données ETL moderne pour l'analyse météo

> **La qualité des données est la matière première de l'IA, des analyses fiables et des décisions d'entreprise ; ce projet illustre une architecture de données robuste, claire et prête pour la production.**

Une plateforme de données orientée production, conçue pour ingérer, normaliser, stocker, transformer et exposer des données météo de qualité avec Python, MinIO, ClickHouse, dbt, Airflow et FastAPI.

Ce projet est construit autour d'un modèle de déploiement simple mais robuste : un pipeline ETL Python orchestré par Airflow, avec MinIO comme lac de données et ClickHouse comme entrepôt analytique. La stack est pensée pour être facile à lancer localement, facile à déployer sur une machine distante, et facile à faire évoluer comme un projet de données de qualité professionnelle.

## Pourquoi ce projet est important

Dans le monde du data engineering et de l'IA, la qualité des données est la base de tout le système. Des données de mauvaise qualité entraînent des tableaux de bord peu fiables, des analyses incohérentes et des modèles d'IA peu performants. À l'inverse, des données bien structurées, bien normalisées et bien transformées permettent des décisions plus sûres, des analyses plus pertinentes et des systèmes d'IA plus robustes.

Ce projet illustre cette logique :
- il structure les données brutes et curées de manière claire,
- il sépare les étapes d'ingestion, stockage et transformation,
- il rend les données exploitables par des outils analytiques et des services applicatifs,
- et il montre comment une base de données de qualité devient un levier pour la data et l'IA.

## Chemin de déploiement actif

Le chemin d'exécution actif de ce dépôt est la stack ETL Python.

Le scénario de production par défaut est le suivant :
- les scripts Python extraient et normalisent les données météo,
- MinIO stocke les données brutes au format parquet,
- Airflow orchestre les workflows,
- dbt transforme les données dans ClickHouse,
- FastAPI expose les données pour consommation applicative.

Airbyte est conservé comme référence historique et optionnelle dans le guide dédié [airbyte/README.md](airbyte/README.md). Il ne constitue pas le chemin d'exécution principal du projet.

## Vue d'ensemble de l'architecture

- Ingestion : scripts Python et jobs ETL planifiés
- Stockage brut : MinIO
- Orchestration : Apache Airflow
- Transformation : dbt + ClickHouse
- Couche de service : FastAPI
- Base métadonnée : PostgreSQL
- Runtime : Docker Compose

## Technologies utilisées

- Python
- Airflow
- dbt
- MinIO
- ClickHouse
- PostgreSQL
- FastAPI
- Docker Compose

## Prérequis

Avant de déployer la stack, vérifiez que les éléments suivants sont disponibles :

- Docker Engine et Docker Compose
- Git
- Accès SSH à la machine cible si vous déployez à distance
- Une clé API valide pour la source météo externe
- Un espace disque suffisant pour MinIO, ClickHouse et PostgreSQL

## Configuration

Créez le fichier d'environnement à partir du modèle :

```bash
git clone https://github.com/donat-konan33/etl-airbyte-minio-clickhouse-dbt-airflow.git
cd etl-airbyte-minio-clickhouse-dbt-airflow
cp .env.example .env
```

Puis modifiez les valeurs dans `.env` pour qu'elles correspondent à votre environnement, notamment :

- `API_KEY`
- `MINIO_USER_NAME` / `MINIO_USER_PASSWORD`
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
- `CLICKHOUSE_DB` / `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`
- `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD`
- `AIRFLOW_ADMIN_*`
- `AIRFLOW__CORE__FERNET_KEY`
- `AIRFLOW__WEBSERVER__SECRET_KEY`

Important :
- utilisez les noms de services Docker à l'intérieur du réseau (`minio`, `clickhouse-server`, `postgres`),
- ne commitez pas les secrets dans le dépôt,
- gardez les noms des services alignés avec ceux définis dans `docker-compose.yml`.

## Déploiement manuel local

Pour lancer la stack par défaut en local :

```bash
docker compose up -d --build --remove-orphans
```

Puis vérifiez les services :

```bash
docker compose ps
docker compose logs -f minio clickhouse-server postgres scheduler webserver databaseapi
```

Points d'entrée attendus localement :
- UI Airflow : http://localhost:8080
- Console MinIO : http://localhost:9001
- API MinIO : http://localhost:9000
- HTTP ClickHouse : http://localhost:8123
- Application FastAPI : http://localhost:8005

## Déploiement manuel distant via SSH

Pour un premier déploiement fonctionnel sur une machine distante, c'est la bonne approche avant d'introduire l'automatisation GitHub Actions.

```bash
ssh -i ~/.ssh/id_ed25519 user@host "mkdir -p /opt/etl-stack && cd /opt/etl-stack && git pull --ff-only || git clone <repo-url> ."
scp -i ~/.ssh/id_ed25519 .env user@host:/opt/etl-stack/.env
scp -i ~/.ssh/id_ed25519 docker-compose.yml user@host:/opt/etl-stack/docker-compose.yml
ssh -i ~/.ssh/id_ed25519 user@host "cd /opt/etl-stack && docker compose pull && docker compose up -d --remove-orphans"
```

Cette méthode permet d'obtenir un déploiement propre et fonctionnel sans forcer la couche CI/CD avant de valider l'infrastructure elle-même.

## Checklist de déploiement

Avant de considérer un déploiement comme valide, vérifiez bien :

1. tous les conteneurs sont en cours d'exécution,
2. l'interface Airflow est accessible,
3. MinIO est sain,
4. ClickHouse accepte les connexions,
5. le service FastAPI répond,
6. les DAGs se chargent sans erreur,
7. le pipeline ETL peut s'exécuter de bout en bout.

## Procédure de rollback

Si le déploiement échoue ou si une nouvelle version introduit des régressions :

```bash
git checkout <previous-commit>
docker compose down
docker compose up -d --build
```

Si vous souhaitez conserver les données, gardez les volumes nommés. Si vous voulez un état complètement propre, supprimez-les explicitement avec les commandes Docker correspondantes.

## Commandes de validation

Avant de considérer la stack comme opérationnelle, exécutez les vérifications suivantes :

```bash
pytest -q tests/test_docker_compose.py tests/test_data_quality.py tests/test_etl.py tests/test_setup.py
docker compose --env-file .env config
```

## Dépannage

### Les services ne démarrent pas

```bash
docker compose ps
docker compose logs --tail=200 scheduler webserver clickhouse-server minio postgres
```

### Problèmes de connexion MinIO

Vérifiez :
- les identifiants dans `.env` sont corrects,
- le conteneur MinIO est sain,
- les noms de réseau dans le compose correspondent bien à la configuration applicative.

### Problèmes ClickHouse

Vérifiez :
- `CLICKHOUSE_HOST=clickhouse-server`,
- `CLICKHOUSE_USER` et `CLICKHOUSE_PASSWORD` sont valides,
- le healthcheck ClickHouse passe bien,
- le profil dbt pointe vers le même host et la même base.

### Problèmes de démarrage Airflow

Vérifiez :
- la clé Fernet est bien définie,
- la secret key est bien définie,
- PostgreSQL est sain avant le démarrage du scheduler.


## Exposition publique et sécurisation de l'API

### Sécurisation de l'API

L'API FastAPI intègre une authentification par clé API afin de sécuriser son exposition publique.

Le mécanisme repose sur `APIKeyHeader` de FastAPI : une clé API doit être fournie dans l'en-tête HTTP de chaque requête avant que l'accès aux endpoints protégés soit autorisé.

La clé API est stockée dans les variables d'environnement et ne doit jamais être commitée dans le dépôt.

> **Important :** `APIKeyHeader` assure l'authentification par clé API. Le chiffrement des échanges est, quant à lui, assuré par HTTPS/TLS lors de l'exposition publique.

### Exposition publique avec Tailscale

Une fois la stack ETL déployée et l'API FastAPI opérationnelle, celle-ci peut être exposée sur Internet à l'aide de Tailscale.

Cette solution permet notamment de rendre l'API accessible depuis un service externe tel que **Streamlit Cloud**, tout en conservant l'infrastructure de données sur la machine distante.

#### 1. Installation et configuration de Tailscale

Après le déploiement de la stack ETL, rendre le script d'installation exécutable :

```bash
chmod +x scripts/tailscale.sh
```

Puis lancer le script :

```bash
./scripts/tailscale.sh
```

Suivez les instructions affichées afin d'installer et de configurer le client Tailscale.

Le script permet de mettre en place la connexion Tailscale et la configuration nécessaire à l'exposition de l'API.

#### 2. Accès à l'API depuis Internet

Une fois le tunnel configuré, l'API FastAPI devient accessible depuis Internet et peut notamment être appelée depuis le frontend Streamlit Cloud.

Le flux devient alors :

```text
Streamlit Cloud
       │
       │ HTTPS + API Key
       ▼
Tailscale
       │
       ▼
FastAPI
       │
       ▼
ClickHouse
```

> **Rappel de sécurité :** sécurisez l'API avec une clé API avant de l'exposer publiquement. Ne rendez jamais une API contenant des données ou des fonctionnalités sensibles accessible publiquement sans mécanisme d'authentification approprié.

Tailscale propose une offre gratuite adaptée à ce type d'usage personnel ou de démonstration. Les modalités et limitations des fonctionnalités d'exposition publique peuvent toutefois évoluer ; consultez la documentation Tailscale pour les conditions actuelles.

## Gros point de ce projet

Cette stack est conçue pour être claire, portable et réaliste. Elle illustre un workflow de Data Engineering de bout en bout : collecte, stockage, transformation et exposition des données via une API.

Au-delà des outils utilisés, le projet met en évidence un principe central du Data Engineering : la qualité et la fiabilité des données conditionnent directement la qualité des analyses et des applications qui en dépendent. Une architecture de données structurée permet ainsi de produire des données plus fiables, traçables et facilement exploitables pour l’analyse, les applications métier ou les systèmes d’IA.

Ce projet constitue une mise en pratique concrète de cette chaîne, depuis l’ingestion des données jusqu'à leur mise à disposition.

## Référence Airbyte

Ce dépôt garde une référence optionnelle à Airbyte dans le document dédié [airbyte/README.md](airbyte/README.md). Ce matériel est historique et expérimental par conception. Le chemin de déploiement par défaut du projet reste la stack ETL Python décrite ci-dessus.
