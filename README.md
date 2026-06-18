# 🔐 SIEM - Sécurité de l'information et gestion des événements  
## Protection des données personnelles chez DJEZZY

## 📌 Description du projet

Ce projet consiste à concevoir une solution **SIEM (Security Information and Event Management)** pour la collecte, le traitement et l’analyse des logs de sécurité afin de détecter les violations potentielles liées aux données personnelles.

Le projet met en place un pipeline **ETL Big Data** basé sur des technologies de traitement distribué permettant l’analyse de grandes quantités de données de sécurité.

---

# 🎯 Objectifs du projet

- Collecter les logs de sécurité
- Centraliser les événements
- Traiter les données de manière distribuée
- Analyser les comportements suspects
- Détecter les incidents liés aux données personnelles
- Améliorer la surveillance de la sécurité informatique

---

# 🏗️ Architecture du pipeline ETL

```
Logs de sécurité
        |
        ↓
Apache NiFi
        |
        ↓
Apache Kafka
        |
        ↓
Apache Spark (Java)
        |
        ↓
Analyse des événements de sécurité
```

---

# 🛠️ Technologies utilisées

- ☕ Java
- ⚡ Apache Spark
- 🔄 Apache Kafka
- 🔧 Apache NiFi
- 🗄️ Big Data Processing
- 🔐 SIEM
- 📊 Analyse des logs

---

# 🔥 Partie développée

La partie disponible dans ce dépôt concerne le traitement des données avec **Apache Spark en Java**.

Fonctionnalités :

- Lecture des logs de sécurité
- Transformation des données
- Nettoyage des événements
- Analyse distribuée avec Spark
- Détection des événements suspects
- Extraction des informations importantes

---

# 📂 Structure du projet

```
SIEM-DJEZZY/

│
├── spark-java/
│
│
├── docs/
│   │
│   ├── Rapport_Master_SIEM.pdf
│   │
│   └── Rapport_PFE_SIEM.pdf
│
└── README.md
```

---

# ⚙️ Installation

## Prérequis

- Java JDK 8+
- Apache Spark
- Apache Maven

---

## Installer les dépendances

Compiler le projet :

```bash
mvn clean install
```

---

# ▶️ Exécution Spark

Exemple :

```bash
spark-submit \
--class SIEM.LogAnalysis \
target/siem-spark.jar
```

---

# 📘 Documentation

Les rapports du projet sont disponibles dans le dossier :

```
docs/
```

Contenu :

📄 Rapport Master :
```
Rapport_Master_SIEM.pdf
```

🎓 Rapport Projet de Fin d'Études :
```
Rapport_PFE_SIEM.pdf
```

---

# 📊 Résultats

La solution permet :

- L'analyse automatique des logs
- Le traitement distribué des événements
- L'identification des activités anormales
- La détection des risques liés aux données personnelles

---

# 👨‍💻 Auteur

**Karim Cherrab**

Projet Master / Projet de Fin d'Études (PFE)

SIEM - Sécurité de l'information et gestion des événements
