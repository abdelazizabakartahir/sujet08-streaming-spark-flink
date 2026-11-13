# ==============================================================================
# IMAGE DE BASE : Apache Spark 3.5.1 avec Python 3
# ==============================================================================
# On part de l'image officielle Apache Spark 3.5.1 qui inclut déjà Java 11,
# les binaires Spark, et l'environnement Python 3. 
FROM apache/spark:3.5.1-python3

# ==============================================================================
# INSTALLATION DES DÉPENDANCES PYTHON
# ==============================================================================
# On passe en utilisateur root temporairement pour installer les bibliothèques.
USER root

# Installation de kafka-python (nécessaire pour que PySpark communique avec Kafka).
# L'option --no-cache-dir évite de stocker le cache pip dans l'image.
RUN pip install --no-cache-dir kafka-python==2.0.2

# ==============================================================================
# SÉCURITÉ ET RÉPERTOIRE DE TRAVAIL
# ==============================================================================
# On repasse sur l'utilisateur "spark" (créé par l'image de base) 
# pour ne pas exécuter le job en tant que root (bonne pratique de sécurité).
USER spark

# On définit le répertoire de travail où seront montés les scripts via docker-compose.
WORKDIR /opt/spark-jobs