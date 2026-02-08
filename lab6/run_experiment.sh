#!/bin/bash
# Lab 6: Spark ML Pipeline - Customer Churn Prediction
# Run Experiment Script

echo "=========================================="
echo "Lab 6 - Spark ML Pipeline Setup & Run"
echo "=========================================="

# Check if dataset exists locally
if [ ! -f ~/Churn_Modelling.csv ]; then
    echo "ERROR: Churn_Modelling.csv not found in home directory!"
    echo ""
    echo "Please upload the dataset first:"
    echo "  1. Download from: https://www.kaggle.com/datasets/shrutimechlearn/churn-modelling"
    echo "  2. From CloudShell: scp -i ~/labsuser.pem Churn_Modelling.csv hadoop@<master-dns>:~/"
    echo ""
    exit 1
fi

# Verify Spark
echo "[1/4] Verifying Spark installation..."
spark-submit --version 2>/dev/null | head -3
echo ""

# Upload dataset to HDFS
echo "[2/4] Uploading dataset to HDFS..."
hdfs dfs -mkdir -p /user/hadoop/churn_input
hdfs dfs -put -f ~/Churn_Modelling.csv /user/hadoop/churn_input/
echo "Dataset uploaded:"
hdfs dfs -ls /user/hadoop/churn_input/
echo ""

# Check cluster status
echo "[3/4] Cluster status..."
yarn node -list 2>/dev/null | grep -E "Total|RUNNING"
echo ""

# Run Spark job
echo "[4/4] Running Spark ML Pipeline..."
echo "=========================================="
echo ""

cd ~/lab6

spark-submit \
    --master yarn \
    --deploy-mode client \
    churn_pipeline.py

echo ""
echo "=========================================="
echo "Experiment completed!"
echo "=========================================="
