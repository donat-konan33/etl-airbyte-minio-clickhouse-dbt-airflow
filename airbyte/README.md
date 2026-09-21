# Référence historique Airbyte

Ce dossier contient le matériel historique et optionnel lié à Airbyte dans ce projet. Il est conservé pour la référence, les expérimentations et une réintégration éventuelle future.

Important : le chemin de déploiement actif de ce dépôt est la stack ETL Python décrite dans le README principal. Airbyte ne fait pas partie du flux d'exécution par défaut.

## Objectif de ce dossier

Cette section documente les premières explorations autour d'un schéma de pipeline d'ingestion basé sur des connecteurs Airbyte pour les données météo et d'autres sources API. Elle contient des configurations de connecteurs, des métadonnées de connexion et des notes de configuration liées aux expérimentations de départ du projet.

Les fichiers présents peuvent inclure :
- `custom_api_builder.yaml`
- `weather_connections.json`
- `france_departments_chunks.json`

## Stratégie par défaut du projet

Le chemin de déploiement maintenu et supporté est le suivant :
- extraction et normalisation Python,
- MinIO comme couche de stockage objet,
- ClickHouse comme entrepôt analytique,
- dbt pour la logique de transformation,
- Airflow pour l'orchestration,
- FastAPI pour la mise à disposition des données.

C'est cette architecture qui est considérée comme la route de production actuelle.

## Quand utiliser Airbyte dans ce projet

Utilisez la configuration Airbyte uniquement si vous voulez intentionnellement tester ou prototyper :
- un flux d'ingestion piloté par connecteur,
- un pipeline source-cible structuré,
- une expérience d'ingestion sur API personnalisée,
- une migration vers un modèle plus visuel de no-code orchestration.

Pour le déploiement principal du portfolio et le flux de données actif, il faut préférer le chemin ETL Python.

## Installation Airbyte et intégration à la stack existante

### ⚠️ Avertissement critique sur les ressources

**Airbyte est très coûteux en ressources système.** Une instance Airbyte complète en Docker consomme typiquement :
- **4-8 GB de RAM** minimum pour fonctionner correctement
- **CPU**: au moins 2-4 cœurs dédiés
- **Stockage**: 20-50 GB d'espace pour les bases de données Airbyte et les logs

**Si vous exécutez la stack Python ETL complète + Airbyte simultanément**, vous aurez besoin de :
- **16 GB+ de RAM** (8 GB pour Airbyte + 6-8 GB pour MinIO, ClickHouse, PostgreSQL, Airflow)
- **8+ cœurs CPU** pour éviter la saturation
- Une machine dédiée ou un cluster Kubernetes

**Recommandation** : Déployez Airbyte sur une machine séparée ou utilisez Airbyte Cloud si les ressources locales sont limitées.

### Installation manuelle pour expérimentations

Si vous souhaitez initialiser l'environnement Airbyte historique avant une expérimentation manuelle, exécutez les commandes suivantes depuis la racine du dépôt :

```bash
./scripts/docker_external_volumes.sh && ./scripts/docker_external_networks.sh
docker compose up -d
./scripts/airbyte_connect_to_containers_network.sh
```

Cette séquence prépare les volumes et réseaux Docker externes nécessaires, démarre la stack conteneurisée, puis connecte Airbyte au réseau du projet pour permettre aux anciens flux d'expérimentation d'atteindre les services de données.

### Configuration Airbyte pour les sources API personnalisées

Airbyte peut être configuré pour ingérer des données via un connecteur API personnalisé. Pour les données météo (ex: VisualCrossing), vous devrez :

**1. Prérequis de configuration**

- **API Key** de votre compte VisualCrossing (ou autre source météo)
- **Base URL** : `https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timelinemulti`
- **Locations** : Format `London,UK|Paris,France|Tokyo,Japan` (jusqu'à 5 locations par requête pour les comptes gratuits)
- **unitGroup** : `metric` (pour °C, m, km/h, etc. utilisés en France)

Pour la configuration d'authentification personnalisée, consultez la [documentation Airbyte Authentication](https://docs.airbyte.com/platform/connector-development/connector-builder-ui/authentication).

**2. Configuration du connecteur API personnalisé**

Utilisez le fichier [`custom_api_builder.yaml`](custom_api_builder.yaml) qui contient la structure de connexion à l'API VisualCrossing. Vous pouvez soit :
- L'importer directement dans Airbyte UI,
- Utiliser l'interface no-code d'Airbyte Connector Builder pour le recréer,
- Adapter le fichier YAML selon vos besoins.

Consultez la [documentation YAML overview](https://docs.airbyte.com/platform/connector-development/config-based/understanding-the-yaml-file/yaml-overview) pour comprendre les composants.

**3. Configuration de la destination MinIO (S3-compatible)**

Airbyte supporte les destinations S3-compatible. MinIO fonctionne de manière transparente avec les APIs Amazon S3 :

```
MinIO Endpoint: minio:9000 (ou votre adresse MinIO en réseau)
Access Key: <MINIO_USER_NAME>
Secret Key: <MINIO_USER_PASSWORD>
Bucket: <votre-bucket>
S3 Path Prefix: /raw/weather/ (ou chemin de votre choix)
```

Vérifiez que le bucket MinIO dispose de permissions en lecture/écriture et que le chiffrement en transit est activé.

**4. Versions de compatibilité**

Pour assurer la compatibilité entre les composants Airbyte et la stack existante :

| Composant | Version recommandée |
|-----------|-------------------|
| Airbyte Server | 1.8.0 |
| Airbyte Connector Builder | 1.8.0 |
| Airbyte Worker | 1.8.0 |
| Airbyte DB | 1.7.0-17 |
| ClickHouse (Altinity) | 24.8.14.10501.altinitystable-alpine |
| MinIO | latest (à fixer) |
| ClickHouse natif | 23.7.4.5-alpine |

**5. Connections multiples pour couverture géographique complète**

Pour couvrir l'intégralité des départements français (99), créez plusieurs connexions sources Airbyte, chacune gérant un sous-ensemble de locations (pour respecter les limites de l'API gratuite). MinIO servira de destination commune pour tous les flux.

### Ressources supplémentaires

- [Guide de démarrage rapide Airbyte OSS](https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart)
- [Tutoriel Connector Builder](https://docs.airbyte.com/platform/connector-development/connector-builder-ui/tutorial)
- [Documentation Airbyte deployment (Kubernetes/Helm)](https://docs.airbyte.com/platform/deploying-airbyte)

## Recommandation

Conservez ce dossier comme référence historique et documentaire, pas comme chemin de production actif.

Pour l'usage quotidien de ce dépôt, suivez le README principal et le flux ETL Python.
