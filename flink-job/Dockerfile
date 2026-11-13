# ==============================================================================
# IMAGE DE BASE : Flink 1.18 avec Scala 2.12
# ==============================================================================
# On part de l'image officielle Apache Flink 1.18 (version Scala 2.12) 
# qui contient déjà Java et les binaires Flink. 
FROM flink:1.18-scala_2.12

# ==============================================================================
# INSTALLATION DE PYTHON ET PYFLINK
# ==============================================================================
# On passe en utilisateur root temporairement pour pouvoir installer des paquets.
USER root

# Mise à jour des dépôts, installation de Python 3 et pip, 
# création d'un lien symbolique pour que la commande "python" pointe vers Python 3,
# puis installation de la bibliothèque PyFlink 1.18.0 via pip.
RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir apache-flink==1.18.0

# ==============================================================================
# SÉCURITÉ ET RÉPERTOIRE DE TRAVAIL
# ==============================================================================
# On repasse sur l'utilisateur "flink" (créé par l'image de base) 
# pour des raisons de sécurité (on ne reste pas en root).
USER flink

# On se place dans le dossier où seront montés les scripts Flink (via docker-compose).
WORKDIR /opt/flink/usrlib
