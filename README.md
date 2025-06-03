# TP-Final NoSQL - Gestion de Drones

## Description

Ce projet est un TP final pour la gestion de drones utilisant une base de données NoSQL (MongoDB). Il permet de stocker, analyser et visualiser des données de drones via des scripts Python et des notebooks Jupyter.

## Structure du projet

- `src/` : Contient les scripts Python principaux :
  - `main.py` : Point d’entrée du projet.
  - `db_setup.py` : Initialisation et configuration de la base de données.
  - `mqtt_listener.py` : Écoute et traitement des messages MQTT.
  - `aggregations.py` : Fonctions d’agrégation et d’analyse des données.
- `data/` : Données utilisées par le projet.
  - `drones.json` : Jeu de données principal.
  - `db/` : Fichiers de la base MongoDB.
- `aggregations_analysis.ipynb` : Notebook Jupyter pour l’analyse des données.
- `requirements.txt` : Dépendances Python.
- `compose.yml` : Fichier Docker Compose pour lancer MongoDB et les services associés.

## Installation

1. Cloner le dépôt :

   ```sh
   git clone https://github.com/DEV-Velris/TP-Final-NoSQL.git
   cd TP-Final-NoSQL
   ```

2. Installer les dépendances Python :

   ```sh
   pip install -r requirements.txt
   ```

3. Lancer MongoDB (via Docker Compose) :
   ```sh
   docker compose up -d
   ```

## Utilisation

- Lancer les scripts Python depuis le dossier `src/` selon les besoins :
  ```sh
  python src/main.py
  ```
- Utiliser le notebook `aggregations_analysis.ipynb` pour explorer et analyser les données. A l'aide de VSCode par exemple.

## Auteurs

- Alexis Soubieux
- Axel Giraudon

## Licence

Projet à usage pédagogique.
