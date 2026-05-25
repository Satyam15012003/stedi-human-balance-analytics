# stedi-human-balance-analytics
## Project Overview

This project builds a cloud-based data lakehouse solution for the STEDI Step Trainer application using AWS services. The objective is to process customer, accelerometer, and step trainer sensor data while ensuring that only customers who consented to research data sharing are included in curated machine learning datasets.

The project uses:
- Amazon S3
- AWS Glue
- AWS Glue Crawlers
- AWS Glue Studio Visual ETL
- Amazon Athena

---

# Architecture
The project follows a multi-zone lakehouse architecture:

## Landing Zone
Raw data ingested into Amazon S3.

Tables:
- customer_landing
- accelerometer_landing
- step_trainer_landing

---

## Trusted Zone
Filtered and validated data.

Tables:
- customer_trusted
- accelerometer_trusted
- step_trainer_trusted

---

## Curated Zone
Analytics and machine learning ready datasets.

Tables:
- customer_curated
- machine_learning_curated

---

# AWS Services Used
- Amazon S3
- AWS Glue Data Catalog
- AWS Glue Crawlers
- AWS Glue Studio
- Amazon Athena

---

# S3 Bucket Structure
```text
s3://satyam-stedi-lakehouse/

customer_landing/
accelerometer_landing/
step_trainer_landing/

customer_trusted/
accelerometer_trusted/
customer_curated/
machine_learning_curated/
