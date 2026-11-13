# Monitoring 

Ceci est réservé à la supervision avancée du cluster.

## Outils envisagés
- **Prometheus** : collecte des métriques (CPU, RAM, latence des jobs).
- **Grafana** : tableaux de bord de visualisation.
- **Flink Metrics Reporter** : export des métriques vers Prometheus.
- **Spark Metrics** : via les sinks Prometheus.

## Pourquoi n'est-il pas activé ?
Dans le cadre de cette étude, la supervision a été assurée par :
- **Kafka UI** (http://localhost:8080) pour visualiser les topics et les messages.
- **Spark UI** (http://localhost:8081) pour le suivi des jobs Spark.
- **Flink UI** (http://localhost:8082) pour le suivi des jobs Flink.

L'ajout de Prometheus/Grafana aurait alourdi l'infrastructure Docker sans apporter de valeur ajoutée pour la comparaison fonctionnelle demandée dans le sujet.

## Pour aller plus loin
Pour une mise en production, il est recommandé d'ajouter :
1. Un service `prometheus` dans le `docker-compose.yml`.
2. Un service `grafana` pour la visualisation.
3. Les exporters Spark et Flink.