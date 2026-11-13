# ==============================================================================
# IMAGE DE BASE : Python 3.11 (version slim = allégée)
# ==============================================================================
# On utilise l'image officielle Python 3.11 en version "slim" (sans les outils inutiles)
# pour réduire la taille de l'image finale et améliorer la sécurité.
FROM python:3.11-slim

# ==============================================================================
# RÉPERTOIRE DE TRAVAIL
# ==============================================================================
# On définit /app comme répertoire de travail dans le conteneur.
# Toutes les commandes suivantes seront exécutées depuis ce dossier.
WORKDIR /app

# ==============================================================================
# INSTALLATION DES DÉPENDANCES
# ==============================================================================
# On copie d'abord uniquement le fichier requirements.txt.
# Cette séparation permet à Docker de mettre en cache cette étape :
# si le code change mais pas les dépendances, l'installation sera instantanée.
COPY requirements.txt .

# Installation des dépendances Python (kafka-python).
# L'option --no-cache-dir évite de stocker le cache pip dans l'image (gain d'espace).
RUN pip install --no-cache-dir -r requirements.txt

# ==============================================================================
# COPIE DU CODE SOURCE
# ==============================================================================
# On copie le script Python dans le conteneur.
# Cette étape est séparée de l'installation des dépendances pour optimiser le cache Docker.
COPY simulate.py .

# ==============================================================================
# COMMANDE DE DÉMARRAGE
# ==============================================================================
# Commande exécutée au démarrage du conteneur.
# Format "exec" (JSON) recommandé pour une bonne gestion des signaux (Ctrl+C, etc.).
CMD ["python", "simulate.py"]
