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

# Athena Validation Queries
---------------------------
## Landing Zone Counts

| Table | Expected Rows |
|---|---|
| customer_landing | 956 |
| accelerometer_landing | 81273 |
| step_trainer_landing | 28680 |

---

## Trusted & Curated Counts

| Table | Expected Rows |
|---|---|
| customer_trusted | 482 |
| accelerometer_trusted | 40981 |
| customer_curated | 482 |
| step_trainer_trusted | 14460 |
| machine_learning_curated | 43681 |

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
