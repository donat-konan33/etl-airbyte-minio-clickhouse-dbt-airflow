# Solution : Insertion des données géographiques dans raw_depcode_

## ✅ Réponses à tes questions

### 1️⃣ Quel module permet l'insertion des données géographiques ?

**Module** : `ClickHouseQueries` de [project_functions/python/clickhouse_crud.py](project_functions/python/clickhouse_crud.py)

**Méthodes principales** :
- `_load_department_reference_data()` ← **Nouvelle** : Charge les données de département
- `load_data_to_clickhouse()` : Insère les données DataFrame dans ClickHouse
- `bootstrap_init_reference_tables()` : Orchestration du bootstrap

```python
queries = ClickHouseQueries()
queries._load_department_reference_data()  # Charge geo + codes
```

### 2️⃣ À quel étape et comment est créé et inséré raw_depcode_ ?

**Étapes du flux** :

| Étape | Moment | Fichier | Action |
|-------|--------|---------|--------|
| 1️⃣ | Docker startup | `clickhouse/init/01_init.sql` | `CREATE TABLE raw_depcode_` (vide) |
| 2️⃣ | Airflow startup | `dags/bootstrap_init_reference_tables_dag.py` | Lance Task: `load_department_reference` |
| 3️⃣ | Task execution | `project_functions/python/clickhouse_crud.py:_load_department_reference_data()` | Fusionne données + insertion |
| 4️⃣ | Task validation | `dags/bootstrap_init_reference_tables_dag.py` | Vérifie que table n'est pas vide |

**Flux détaillé** :

```python
# Dans le DAG Airflow
def load_department_reference():
    queries = ClickHouseQueries()
    queries._load_department_reference_data()
    # Qui fait :
    # 1. Charge departements_france_selection.csv
    # 2. Charge france_region_department96.parquet
    # 3. Fusionne sur dep_current_code
    # 4. Transforme WKB → WKT → GeoJSON
    # 5. INSERT INTO raw_depcode_
```

### 3️⃣ Où est le fichier CSV / GeoJSON des données statiques ?

**Données fixes (codes de département)** :
- 📄 `data/location/departements_france_selection.csv`
- Généré par `scripts/department_code_builder.py`
- Contient 96 départements français avec leurs codes

**Données géographiques (formes, régions)** :
- 📦 `data/location/france_region_department96.parquet`
- Source : OpenDataSoft (données publiques)
- Contient coordonnées, formes géométriques (WKB), infos régions

## 📋 Propositions implémentées

### 1. Nouvelle méthode : `_load_department_reference_data()`

**Fichier** : `project_functions/python/clickhouse_crud.py`

```python
def _load_department_reference_data(self) -> None:
    """
    Load fixed French department reference data (raw_depcode_).
    Merges department codes with geographic data (regions, coordinates, shapes).
    """
    # Charge les deux fichiers source
    department_data = pd.read_csv("data/location/departements_france_selection.csv")
    dep_geo = pd.read_parquet("data/location/france_region_department96.parquet")

    # Fusionne les données
    data = dep_geo.merge(department_data, on="dep_current_code", how="left")

    # Transforme les formats géométriques WKB → WKT → GeoJSON
    data["geo_point_2d"] = data["geo_point_2d"].apply(wkb_to_wkt)
    data["geo_shape"] = data["geo_shape"].apply(wkb_to_wkt_to_geojson)

    # Normalise les noms de département (pour les jointures)
    data["dep_normalized"] = TransformData.normalize(data["department"])

    # Insère dans ClickHouse
    self.load_data_to_clickhouse("raw_depcode_", data, is_to_truncate=True)
```

**Avantages** :
✅ Encapsule toute la logique en une seule méthode
✅ Réutilisable dans Airflow ou scripts Python
✅ Gère les transformations géométriques (WKB → GeoJSON)
✅ Intégration facile au bootstrap

### 2. DAG Airflow amélioré

**Fichier** : `dags/bootstrap_init_reference_tables_dag.py`

```python
load_departments_task >> validate_tables_task

# Task 1 : Charge les données
load_departments_task = PythonOperator(
    task_id="load_department_reference",
    python_callable=load_department_reference,
    doc="Loads French department codes and geographic data"
)

# Task 2 : Valide que tout est OK
validate_tables_task = PythonOperator(
    task_id="validate_reference_tables",
    python_callable=validate_reference_tables,
    doc="Validates raw_depcode_ and raw_weather_ are populated"
)
```

**Améliorations** :
✅ Sépare le chargement et la validation (meilleure traçabilité)
✅ Docstring explicites
✅ Contrôle d'exécution clair (>> dependency)
✅ Erreurs explicites si les données manquent

### 3. Amélioration du bootstrap

**Fichier** : `project_functions/python/clickhouse_crud.py`

```python
def bootstrap_init_reference_tables(self) -> None:
    """One-off initialization phase"""

    # Crée les tables
    self.ensure_table_exists("raw_depcode_")
    self.ensure_table_exists("raw_weather_")

    # Charge les données de département si la table est vide
    if count_rows("raw_depcode_") == 0:
        self._load_department_reference_data()

    # Valide que tout est peuplé
    assert count_rows("raw_depcode_") > 0
    assert count_rows("raw_weather_") > 0
```

**Améliorations** :
✅ Automatise le chargement au bootstrap
✅ Pas besoin d'exécuter manuellement `load_department_infos_to_clickhouse.py`
✅ Idempotent (peut être exécuté plusieurs fois)

### 4. Script de test direct

**Fichier** : `scripts/test_load_department_data.py`

```bash
# Exécution directe (sans Airflow)
python scripts/test_load_department_data.py

# Sortie :
# [1/3] Création de la table...
# [2/3] Chargement des données...
# [3/3] Validation...
# ✓ 96 département(s) chargé(s)
```

**Utile pour** :
✅ Tester le chargement avant Airflow
✅ Déboguer les problèmes de données
✅ Recharger après une réinitialisation

### 5. Documentation complète

**Fichier** : `DEPARTEMENT_DATA_FLOW.md`

- Diagramme du flux complet
- Description des données d'entrée (CSV + Parquet)
- Architecture des modules
- Table récapulative des colonnes
- Instructions d'exécution manuelle
- Comparaison raw_depcode_ vs raw_weather_

## 🚀 Utilisation

### Option 1 : Airflow (recommandé)
```bash
# Au démarrage, le DAG bootstrap s'exécute automatiquement
docker compose up

# Ou manuellement via l'UI Airflow
poetry airflow dags test bootstrap_init_reference_tables 2026-09-27
```

### Option 2 : Script direct (pour le test/debug)
```bash
# Depuis le conteneur Airflow
docker exec airflow-scheduler poetry run python scripts/test_load_department_data.py

# Ou localement (si ClickHouse est accessible)
poetry run python scripts/test_load_department_data.py
```

### Option 3 : Code Python
```python
from project_functions.python.clickhouse_crud import ClickHouseQueries

queries = ClickHouseQueries()
queries._load_department_reference_data()
print("✓ Données chargées")
```

## 📊 Vérification

```bash
# Vérifier que les données sont chargées
clickhouse-client --database datawarehouse \
  --query "SELECT COUNT(*) FROM raw_depcode_"

# Afficher un aperçu
clickhouse-client --database datawarehouse \
  --query "SELECT department, dep_current_code, reg_name FROM raw_depcode_ LIMIT 10"
```

## 📝 Résumé des fichiers modifiés

| Fichier | Action | Raison |
|---------|--------|--------|
| `clickhouse_crud.py` | Nouvelle méthode `_load_department_reference_data()` | Encapsuler la logique |
| `clickhouse_crud.py` | Modification `bootstrap_init_reference_tables()` | Auto-loader si vide |
| `bootstrap_init_reference_tables_dag.py` | Nouveau DAG avec 2 tasks | Clarifier le flux |
| `DEPARTEMENT_REFERENCE_DATA.md` | Créé | Documentation complète |
| `test_load_department_data.py` | Créé | Script de test/debug |

---

**Résultat** : `raw_depcode_` est maintenant automatiquement peuplée au bootstrap avec 96 départements français, leurs codes, régions, coordonnées et formes géographiques (en format GeoJSON pour ClickHouse).
