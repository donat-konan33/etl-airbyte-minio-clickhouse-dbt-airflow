#!/bin/bash
set -e

echo "→ Mise à jour du dépôt"
git pull --ff-only origin master

echo "→ Reconstruction et démarrage des services"
docker compose up -d --build

echo "✓ Déploiement terminé"
docker compose ps
