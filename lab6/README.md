# Lab 6: Spark ML Pipeline on Amazon EMR

## Overview

Customer Churn Prediction using PySpark ML Pipeline on Amazon EMR.

## Dataset

- **Source:** Bank Customer Churn Dataset
- **URL:** https://www.kaggle.com/datasets/shrutimechlearn/churn-modelling
- **Records:** 10,000 customers
- **Target:** `Exited` (0 = No churn, 1 = Churn)

### Features

| Feature         | Type        | Description                      |
| --------------- | ----------- | -------------------------------- |
| CreditScore     | Numerical   | Customer credit score            |
| Geography       | Categorical | Country (France, Spain, Germany) |
| Gender          | Categorical | Male/Female                      |
| Age             | Numerical   | Customer age                     |
| Tenure          | Numerical   | Years as customer                |
| Balance         | Numerical   | Account balance                  |
| NumOfProducts   | Numerical   | Number of products               |
| EstimatedSalary | Numerical   | Estimated salary                 |

## Cluster Configuration

| Node Type    | Instance                  | Count |
| ------------ | ------------------------- | ----- |
| Primary      | m4.large                  | 1     |
| Core         | m4.large                  | 2     |
| Applications | Hadoop 3.4.1, Spark 3.5.6 |

## Quick Start

### 1. Download Dataset

Download `Churn_Modelling.csv` from Kaggle (requires account).

### 2. Upload Dataset to EMR Master

```bash
scp -i ~/labsuser.pem Churn_Modelling.csv hadoop@<master-dns>:~/
```

### 3. SSH into Master Node

```bash
ssh -i ~/labsuser.pem hadoop@<master-dns>
```

### 4. Clone Repository

```bash
git clone https://github.com/Tretorhate/dist_comp.git
cd dist_comp/lab6
```

### 5. Run Experiment

```bash
chmod +x run_experiment.sh
./run_experiment.sh
```

## Manual Commands

### Upload to HDFS

```bash
hdfs dfs -mkdir -p /user/hadoop/churn_input
hdfs dfs -put ~/Churn_Modelling.csv /user/hadoop/churn_input/
hdfs dfs -ls /user/hadoop/churn_input/
```

### Run Spark Job

```bash
spark-submit --master yarn --deploy-mode client churn_pipeline.py
```

### Check Cluster Status

```bash
spark-submit --version
yarn node -list
```

## Pipeline Stages

1. **Data Loading** — Load CSV from HDFS
2. **Categorical Encoding** — StringIndexer + OneHotEncoder for Geography, Gender
3. **Feature Assembly** — VectorAssembler combines all features
4. **Feature Scaling** — StandardScaler normalizes features
5. **Model Training** — Logistic Regression
6. **Prediction** — Generate predictions on test data
7. **Evaluation** — Calculate accuracy, precision, recall, F1, AUC

## Experiment B: Feature Ablation

Compare model performance with and without categorical features.

| Pipeline       | Features Used                                                                                |
| -------------- | -------------------------------------------------------------------------------------------- |
| Full           | CreditScore, Age, Tenure, Balance, NumOfProducts, EstimatedSalary, **Geography**, **Gender** |
| Numerical Only | CreditScore, Age, Tenure, Balance, NumOfProducts, EstimatedSalary                            |

### Expected Results

| Metric   | Full Pipeline | Numerical Only |
| -------- | ------------- | -------------- |
| Accuracy | ~0.81         | ~0.79          |
| AUC      | ~0.76         | ~0.74          |

**Observation:** Removing categorical features (Geography, Gender) slightly decreases model performance, indicating these features contribute to churn prediction.

## Files

```
lab6/
├── churn_pipeline.py   # Spark ML pipeline with experiment
├── run_experiment.sh   # Setup and run script
└── README.md           # This file
```

## Author

Tret - Distributed Computing Lab 6
