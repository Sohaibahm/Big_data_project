# Real-Time Transaction Anomaly Detection Pipeline

> Pipeline Big Data end-to-end — ingestion streaming via Kafka, traitement temps réel avec Spark Structured Streaming, détection d'anomalies par scoring et stockage dans un Data Lake local.

---

## Overview

Ce projet implémente un pipeline Big Data complet pour la détection d'anomalies sur des transactions financières en temps réel. Il simule un environnement de production bancaire où les transactions sont ingérées en continu, analysées à la volée, stockées en format Parquet dans un Data Lake et visualisées via Power BI.

---

## Architecture

```
[transactions.csv]
       ↓
[Kafka Producer]  →  [Kafka Topic: transactions]
                              ↓
                 [Spark Structured Streaming]
                              ↓
                   [Détection d'anomalies]
                    (scoring multi-règles)
                              ↓
              [Data Lake / datalake/curated/anomalies]
                              ↓
              [Spark Analytics Job] → [Power BI]
```

---

## Stack Technique

| Composant | Outil | Rôle |
|---|---|---|
| **Ingestion** | Apache Kafka | Streaming des transactions en temps réel |
| **Traitement** | PySpark 3.5.1 — Structured Streaming | Parsing, transformation, scoring |
| **Détection** | Règles métier (threshold + scoring) | Identification des anomalies |
| **Stockage** | Parquet — Data Lake local | Persistance des anomalies détectées |
| **Analytics** | Spark Batch Job | Agrégations par pays, canal, temps |
| **Visualisation** | Power BI | Dashboard de monitoring |
| **Orchestration** | Docker Compose | Kafka + Zookeeper |

---

## Logique de détection d'anomalies

La détection repose sur un **système de scoring multi-règles** :

| Règle | Condition | Score |
|---|---|---|
| Montant élevé | `amount > 5000` | +1 |
| Pays risqué | `country ∈ [RU, IR, KP, AF]` | +1 |
| **Anomalie détectée** | `anomaly_score ≥ 1` |

> Les seuils et la liste des pays risqués sont configurables dans `spark/spark_streaming_job.py`

---

## Analytics générés

Le job `spark_analytics_job.py` produit 3 agrégations stockées en CSV pour Power BI :

- **Anomalies par pays** — nb anomalies, montant total, montant moyen
- **Anomalies par canal** — Web, Mobile, ATM...
- **Anomalies par minute & pays** — évolution temporelle des anomalies

---

## Installation & Lancement

### Prérequis

- Python 3.8+
- Java 8+ (requis pour Spark)
- Docker & Docker Compose
- Hadoop binaries (Windows uniquement) → `C:/hadoop`

### 1. Lancer Kafka

```bash
cd kafka/
docker-compose up -d
```

### 2. Lancer le Producer Kafka

```bash
python kafka/producer_transactions.py
```

### 3. Lancer le Job Streaming (détection temps réel)

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark/spark_streaming_job.py
```

### 4. Lancer le Job Analytics (après collecte des données)

```bash
spark-submit spark/spark_analytics_job.py
```

### 5. Connecter Power BI

## Résultats

- Pipeline opérationnel en temps réel — lecture continue du topic Kafka `transactions`
- Anomalies détectées et stockées en **format Parquet** dans le Data Lake
- **3 vues analytiques** exportées en CSV et visualisées dans Power BI
- Architecture modulaire et extensible

---

## Améliorations envisagées

- Remplacement de la détection par seuil par un modèle ML (**Isolation Forest**)
- Migration du Data Lake local vers **AWS S3** ou **Azure Data Lake**
- Ajout d'un système d'alerting automatique (email, Slack) sur détection
- Exposition des résultats via une **API FastAPI** en temps réel
---

## Auteur

**HASSIB Sohaib** — AI Engineer

