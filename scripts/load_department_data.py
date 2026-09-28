#!/usr/bin/env python
"""
Script d'administration : Charge les données de département dans raw_depcode_
Usage : python scripts/load_department_data.py

Ce script est un utilitaire d'administration pour :
- Charger manuellement les données de département
- Déboguer les problèmes de chargement
- Vérifier que les données sont correctement insérées

Note : Ce script est exécuté automatiquement au bootstrap Airflow.
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from project_functions.python.clickhouse_crud import ClickHouseQueries

def main():
    print("=" * 80)
    print("TEST : Chargement des données de département dans raw_depcode_")
    print("=" * 80)

    try:
        queries = ClickHouseQueries()

        # Étape 1 : Créer la table si elle n'existe pas
        print("\n[1/3] Création de la table raw_depcode_ si nécessaire...")
        queries.ensure_table_exists("raw_depcode_")
        print("✓ Table raw_depcode_ existe")

        # Étape 2 : Charger les données
        print("\n[2/3] Chargement des données de département...")
        queries._load_department_reference_data()
        print("✓ Données chargées avec succès")

        # Étape 3 : Valider
        print("\n[3/3] Validation des données...")
        client = queries.clickhouse_client
        count = client.get_conn().query_df("SELECT COUNT(*) as cnt FROM raw_depcode_")
        rows = int(count.iloc[0].iloc[0])
        print(f"✓ {rows} département(s) chargé(s)")

        # Afficher un aperçu
        print("\n" + "=" * 80)
        print("APERÇU des données (5 premiers)")
        print("=" * 80)
        sample = client.get_conn().query_df(
            "SELECT department, dep_current_code, reg_name, dep_normalized FROM raw_depcode_ LIMIT 5"
        )
        print(sample.to_string())

        print("\n" + "=" * 80)
        print("✓ TEST RÉUSSI : Les données sont prêtes pour dbt")
        print("=" * 80)

    except FileNotFoundError as e:
        print(f"\n✗ ERREUR : Fichier de données manquant")
        print(f"  {e}")
        print(f"\n  Assurez-vous que ces fichiers existent :")
        print(f"  - data/location/departements_france_selection.csv")
        print(f"  - data/location/france_region_department96.parquet")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ERREUR : {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
