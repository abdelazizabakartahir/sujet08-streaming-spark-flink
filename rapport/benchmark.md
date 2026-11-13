# Resultats du Benchmark

## Environnement
- Machine : Windows 10, Docker Desktop
- Kafka 7.6 (KRaft), Spark 3.5.1, Flink 1.18
- Data : flux simulé de transactions de vente (topic `sales`, 3 partitions)

## Resultats

| Debit (msg/s) | Spark latence avg (s) | Flink latence avg (s) | Observations |
|:---:|:---:|:---:|:---|
| 100 | < 0.01 | N/A (sink muet) | Spark stable, aucun warning |
| 1000 | < 0.01 | N/A (sink muet) | Spark stable |
| 5000 | **Saturation** | N/A (sink muet) | Spark : `Current batch is falling behind` (124s par batch) + `TimeoutException` Kafka |

## Reprise apres panne
- **Spark** : Redémarrage réussi depuis le checkpoint (`/tmp/checkpoints/spark`). Reprise du traitement sans perte de données (les offsets Kafka sont restaurés). Temps de reprise : ~15 secondes.
- **Flink** : Job RUNNING, mais sink Kafka ne produit aucune donnée (limite de la configuration PyFlink dans cet environnement). La reprise après panne n'a donc pas pu être validée expérimentalement pour Flink.

## Analyse
- **Spark** : Très performant à faible charge (<1000 msg/s). À 5000 msg/s, le modèle micro-batch sature (le temps de traitement d'un lot dépasse l'intervalle de déclenchement de 5s).
- **Flink** : Le job est correctement soumis et maintenu en état RUNNING, mais le connecteur de sortie Kafka ne fonctionne pas correctement (problème de classpath/sink). C'est une limite documentée de PyFlink dans Docker.
- **Recommandation** : Pour une petite équipe avec des besoins modérés, Spark est plus simple. Pour des charges élevées et une latence stable, Flink serait préférable (si le sink est corrigé).

## Captures
Voir dossier `rapport/captures/`
- `01_kafka_topics.png` : les 3 topics
- `02_kafka_messages.png` : messages dans `sales`
- `03_spark_ui.png` : Spark UI
- `04_spark_output.png` : tableau Spark
- `05_flink_ui.png` : Flink UI (job RUNNING)
- `06_flink_output.png` : (Flink Overview)
- `07_spark_recovery.png` : logs après panne