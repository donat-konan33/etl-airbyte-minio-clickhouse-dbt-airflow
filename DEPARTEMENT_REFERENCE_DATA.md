# Flux d'insertion des données de département (raw_depcode_)

## Vue d'ensemble

```
docker compose up
     │
     ├── ClickHouse démarre
     │      │
     │      └── 01_init.sql (création des tables)
     │             ├── CREATE DATABASE datawarehouse
     │             ├── CREATE TABLE raw_depcode_ (vide au départ)
     │             └── CREATE TABLE raw_weather_ (vide au départ)
     │
     ├── Airflow Scheduler démarre
     │      │
     │      └── DAG: bootstrap_init_reference_tables
     │             ├── Task 1: load_department_reference
     │             │    └── ClickHouseQueries()._load_department_reference_data()
     │             │         ├── Charge data/location/departements_france_selection.csv
     │             │         ├── Charge data/location/france_region_department96.parquet
     │             │         ├── Fusionne les données sur dep_current_code
     │             │         ├── Transforme WKB → WKT → GeoJSON
     │             │         └── INSERT into raw_depcode_
     │             │
     │             └── Task 2: validate_reference_tables
     │                  └── Vérifie que raw_depcode_ n'est pas vide
     │
     └── Les autres DAGs (python_etl, dbt, etc.) peuvent démarrer
```

## 1. Données d'entrée

### A. Codes de département (données fixes)
**Fichier** : `data/location/departements_france_selection.csv`
- Généré par : `scripts/department_code_builder.py`
- Contenu : `department`, `dep_current_code`
- Exemples :
  ```
  Tarn,81
  Aude,11
  Paris,75
  Corse-du-Sud,2A
  ```

### B. Données géographiques des régions/départements
**Fichier** : `data/location/france_region_department96.parquet`
- Source : OpenDataSoft (données publiques françaises)
- Colonnes :
  - `geo_point_2d` : Point géographique (WKB)
  - `geo_shape` : Forme géométrique (WKB - Well-Known Binary)
  - `reg_code` : Code région
  - `reg_name` : Nom région
  - `dep_current_code` : Code département (clé de jointure)
  - `dep_name_upper` : Nom département
  - `dep_status` : Statut (nullable)

## 2. Modules d'insertion

### Module principal : `ClickHouseQueries` (clickhouse_crud.py)

#### Méthode : `_load_department_reference_data()`
Charge les données de département fixes dans `raw_depcode_` via 3 étapes :

**Étape 1 : Chargement des données**
```python
department_data = pd.read_csv("data/location/departements_france_selection.csv")
dep_geo = pd.read_parquet("data/location/france_region_department96.parquet")
```

**Étape 2 : Fusion**
```python
data = dep_geo.merge(department_data, on="dep_current_code", how="left")
```

**Étape 3 : Transformation géométrique**
```python
# WKB (format binaire) → WKT (format texte) → GeoJSON (format JSON)
def wkb_to_wkt(x):
    return shapely.wkb.loads(x).wkt

data["geo_point_2d"] = data["geo_point_2d"].apply(wkb_to_wkt)
data["geo_shape"] = data["geo_shape"].apply(wkb_to_wkt)
# Convertir WKT en GeoJSON
data["geo_shape"] = data["geo_shape"].apply(
    lambda w: wkt.loads(w).__geo_interface__ if w else None
)
data["geo_shape"] = data["geo_shape"].apply(
    lambda x: json.dumps(x) if isinstance(x, dict) else x
)
```

**Étape 4 : Normalisation des noms**
```python
normalizer = TransformData()
department_data["dep_normalized"] = normalizer.normalize(
    department_data["department"]
)
# Exemple : "Île-de-France" → "ILE DE FRANCE"
```

**Étape 5 : Insertion dans ClickHouse**
```python
self.load_data_to_clickhouse(
    table_name="raw_depcode_",
    data=data,
    is_to_truncate=False
)
```

#### Méthode : `load_data_to_clickhouse(table_name, data, is_to_truncate)`
- Appelée par `_load_department_reference_data()` pour insérer les données
- Utilise la connexion native ClickHouse : `client.get_conn().insert_df(table, data)`
- Supporte le mode `TRUNCATE` pour réinitialiser les données

#### Méthode : `bootstrap_init_reference_tables()`
- Crée les tables s'il elles n'existent pas
- Charge les données de département si `raw_depcode_` est vide
- Valide que les deux tables de référence ont des données

### Classe `TransformData` (functions.py)
- **Méthode** : `normalize(pd.Series)`
- **Objectif** : Normaliser les noms de département pour la jointure
- **Transformation** : Accent supprimés, tirets/apostrophes remplacés, majuscules
- **Exemple** : "Île-de-France" → "ILE DE FRANCE"

## 3. Flux dans Airflow

### DAG : `bootstrap_init_reference_tables`
**Moment d'exécution** : Au premier démarrage ou manuellement (pas de schedule)

**Tâches** :
1. `load_department_reference` → Charge les données fixes + géographiques
2. `validate_reference_tables` → Valide que tout est prêt pour dbt

```python
load_departments_task >> validate_tables_task
```

## 4. Résumé des données dans raw_depcode_

| Colonne | Type | Source | Contenu |
|---------|------|--------|---------|
| `geo_point_2d` | String (WKT) | france_region_department96.parquet | Point géographique |
| `geo_shape` | String (GeoJSON) | france_region_department96.parquet | Forme du département |
| `reg_name` | String | france_region_department96.parquet | "Île-de-France", "Occitanie", etc. |
| `reg_code` | String | france_region_department96.parquet | Code région |
| `dep_name_upper` | String | france_region_department96.parquet | "PARIS", "LYON", etc. |
| `dep_current_code` | String | departements_france_selection.csv | "75", "69", "2A", etc. |
| `dep_status` | String (nullable) | france_region_department96.parquet | Statut administratif |
| `department` | String | departements_france_selection.csv | "Paris", "Lyon", etc. |
| `dep_normalized` | String | Créé par TransformData | "PARIS", "LYON", etc. (sans accents) |

## 5. Différence avec raw_weather_

| Aspect | raw_depcode_ | raw_weather_ |
|--------|--------------|--------------|
| **Fréquence** | Une seule fois (données fixes) | Quotidienne |
| **Source** | Données géographiques statiques | API Visual Crossing |
| **Insertion** | DAG bootstrap au démarrage | DAG python_etl (schedule 2h du matin) |
| **Table ENGINE** | MergeTree (ORDER BY dep_current_code) | MergeTree (ORDER BY id, department, datetime) |
| **Données** | 96 départements français | N lignes/jour selon API |

## 6. Comment utiliser

### Exécution manuelle du bootstrap
```bash
# Depuis le conteneur Airflow ou via l'UI
poetry run airflow dags test bootstrap_init_reference_tables <date>
```

### Vérifier que les données sont chargées
```bash
# Via le client ClickHouse
clickhouse-client --user <username> --password <pwd> --database datawarehouse
SELECT COUNT(*) FROM raw_depcode_;
SELECT * FROM raw_depcode_ LIMIT 5;
```

### Recharger les données
```python
queries = ClickHouseQueries()
queries._load_department_reference_data()  # Chargement
queries.bootstrap_init_reference_tables()  # Validation
```
