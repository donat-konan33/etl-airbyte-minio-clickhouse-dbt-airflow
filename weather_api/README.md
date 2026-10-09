# API météo et énergie solaire

API REST développée avec FastAPI permettant d'accéder aux données météorologiques et solaires stockées dans ClickHouse. Elle propose des endpoints pour consulter les prévisions météo, les indicateurs de production solaire et des analyses à différentes échelles géographiques (régions et départements).

## Fonctionnalités

- **Données météorologiques** : températures, précipitations, vitesse du vent et descriptions météo
- **Énergie solaire** : rayonnement solaire et indicateurs de production d'énergie (kWh/m²)
- **Données géospatiales** : données de localisation avec coordonnées géographiques et GeoJSON
- **Analyses** : statistiques et agrégations par région ou département
- **Données pour l'apprentissage automatique** : jeux de données préparés pour les pipelines de machine learning
- **Gestion des utilisateurs** : système d'authentification utilisateur de base

## Technologies utilisées

- **Framework** : FastAPI
- **Base de données** : ClickHouse (via clickhouse-sqlalchemy)
- **ORM** : SQLAlchemy
- **Schémas** : modèles Pydantic définis, mais pas encore utilisés par les endpoints
- **Serveur ASGI** : Uvicorn

FastAPI valide les paramètres déclarés dans les routes (par exemple les paramètres
de requête). En revanche, les modèles de `schemas.py` ne sont actuellement branchés
sur aucun endpoint : il n'y a pas de validation des réponses au moyen de
`response_model` ni de validation des données entrantes avec ces modèles.

## Tables de la base de données

L'API utilise plusieurs tables ClickHouse :

- `mart_newdata_` : table principale des données météo et solaires
- `archived_data` : données météorologiques historiques
- `mart_today` et `mart_next_3_days` : données du jour et prévisions
- `mart_today_stats` et `mart_next_3_days_stats` : agrégations statistiques
- `users` : données d'authentification des utilisateurs
- `raw_depcode` : données de référence des départements et localisations

## Endpoints de l'API

### Endpoints généraux

- `GET /` : état de l'API et liens vers la documentation
- `GET /docs` : documentation interactive de l'API (Swagger UI)
- `GET /redoc` : documentation alternative

### Endpoints de données

#### Données brutes

- `GET /get_data` : récupère toutes les données météo et solaires brutes

#### Métadonnées

- `GET /date` : liste les dates disponibles dans le jeu de données

#### Ensoleillement et énergie solaire

- `GET /get_sunshine_data` : mesures de durée d'ensoleillement et d'exposition solaire
- `GET /solar_geo_data?date={date}` : données solaires géospatiales pour une date donnée
- `GET /get_solarenergy_agg_pday?department={dept}` : agrégations journalières d'énergie solaire

L'estimation de production solaire utilise un panneau de 2,7 m² et le rendement
maximal de 21,7 % du module **Trina Solar TSM-DE19R**. Sa fiche technique indique
une puissance comprise entre 565 et 585 W (jusqu'à 585 W). L'endpoint applique
ces hypothèses à l'énergie solaire moyenne par mètre carré du département.
Il s'agit d'une estimation théorique qui ne tient pas compte des conditions
d'installation ni des pertes du système.

Référence : [fiche technique Trina Solar TSM-DE19R](https://static.trinasolar.com/sites/default/files/Datasheet_Vertex_DE19R_FR_2023%20C_web.pdf).

#### Données régionales et départementales

- `GET /get_region_sunshine_data?region={region}` : données d'ensoleillement par région
- `GET /get_entire_region_data?region={region}` : ensemble des données météo régionales
- `GET /get_entire_department_data?department={dept}` : ensemble des données météo départementales

#### Données de température

- `GET /get_temp_data?department={dept}` : indicateurs de température (actuelle, minimale, maximale et ressentie)

#### Indicateurs et données pour le machine learning

- `GET /common_features?department={dept}` : indicateurs calculés pour les analyses
- `GET /get_ml_data` : jeux de données préparés pour le machine learning

#### Analyses

- `GET /analytics/stats?level={region|department}&period={today|next3days}&top={true|false}` : statistiques et indicateurs synthétiques

## Modèles de données

### Champs des données météorologiques

- `dates` : date de la mesure météo
- `weekday_name` : jour de la semaine
- `department` : code ou nom du département français
- `reg_name` : nom de la région
- `temp`, `tempmin`, `tempmax` : températures (°C)
- `feelslike`, `feelslikemin`, `feelslikemax` : températures ressenties
- `descriptions` : description météo
- `windspeed`, `windgust` : mesures du vent
- `pressure` : pression atmosphérique
- `precip` : précipitations
- `solarenergy_kwhpm2` : énergie solaire (kWh/m²)
- `solarradiation` : rayonnement solaire
- `geo_point_2d` : coordonnées géographiques
- `geojson` : géométrie au format GeoJSON

## Installation et configuration

1. **Variables d'environnement**
   ```bash
   export CLICKHOUSE_DATABASE_URL="******host:port/database"
   ```

2. **Installation des dépendances**
   Ce projet utilise Poetry pour la gestion des dépendances. Depuis la racine du projet :
   ```bash
   poetry install --with api
   ```

3. **Lancement de l'API**
   ```bash
   poetry run python weather_api/main.py
   ```

   Ou avec Uvicorn :
   ```bash
   poetry run uvicorn weather_api.main:app --host 0.0.0.0 --port 8005 --reload
   ```

## Exemples d'utilisation

### Obtenir la météo du jour pour un département

```bash
curl "http://localhost:8005/get_temp_data?department=75"
```

### Obtenir les données solaires pour une date précise

```bash
curl "http://localhost:8005/solar_geo_data?date=2024-01-15"
```

### Obtenir les statistiques analytiques

```bash
curl "http://localhost:8005/analytics/stats?level=department&period=today&top=true"
```

## Documentation de l'API

Une fois l'API lancée, consulter :

- **Swagger UI** : http://localhost:8005/docs
- **ReDoc** : http://localhost:8005/redoc

## Structure du projet

```
weather_api/
├── main.py          # Application FastAPI et endpoints
├── models.py        # Modèles SQLAlchemy et définitions des tables
├── crud.py          # Opérations et requêtes de base de données
├── schemas.py       # Modèles Pydantic (pas encore utilisés par les endpoints)
├── database.py      # Configuration de la connexion à la base de données
├── __init__.py      # Initialisation du package
└── README.md        # Ce fichier
```

## Contribution

Cette API fait partie d'un pipeline ETL plus large comprenant Airbyte, MinIO,
ClickHouse, DBT et Airflow. Avant d'utiliser les endpoints, vérifier que les
tables de la base de données contiennent les données nécessaires.
