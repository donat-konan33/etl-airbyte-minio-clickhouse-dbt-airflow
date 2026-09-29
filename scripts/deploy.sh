#!/bin/bash
set -e

echo "→ Synchronisation avec GitHub"

git fetch origin
git reset --hard origin/master

echo "→ Reconstruction et démarrage des services"

docker compose up -d --build

echo "✓ Déploiement terminé"

docker compose ps
