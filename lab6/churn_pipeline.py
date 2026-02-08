#!/usr/bin/env python3
"""
Lab 6: Spark ML Pipeline - Customer Churn Prediction
Distributed Computing - Amazon EMR
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
import sys
import time

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("CustomerChurnPipeline") \
        .getOrCreate()

def load_data(spark, path):
    """Load data from HDFS"""
    print(f"Loading data from: {path}")
    data = spark.read.csv(path, header=True, inferSchema=True)
    print(f"Total records: {data.count()}")
    print(f"Columns: {data.columns}")
    return data

def build_full_pipeline():
    """Build pipeline with ALL features (categorical + numerical)"""
    
    # Categorical encoding
    geo_indexer = StringIndexer(inputCol="Geography", outputCol="GeographyIndex")
    gender_indexer = StringIndexer(inputCol="Gender", outputCol="GenderIndex")
    
    encoder = OneHotEncoder(
        inputCols=["GeographyIndex", "GenderIndex"],
        outputCols=["GeographyVec", "GenderVec"]
    )
    
    # Feature assembly (numerical + categorical)
    assembler = VectorAssembler(
        inputCols=[
            "CreditScore", "Age", "Tenure", "Balance",
            "NumOfProducts", "EstimatedSalary",
            "GeographyVec", "GenderVec"
        ],
        outputCol="features"
    )
    
    # Feature scaling
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaledFeatures"
    )
    
    # Model
    lr = LogisticRegression(
        labelCol="Exited",
        featuresCol="scaledFeatures",
        maxIter=100
    )
    
    # Build pipeline
    pipeline = Pipeline(stages=[
        geo_indexer,
        gender_indexer,
        encoder,
        assembler,
        scaler,
        lr
    ])
    
    return pipeline

def build_numerical_only_pipeline():
    """Build pipeline with ONLY numerical features (no categorical)"""
    
    # Feature assembly (numerical only - no Geography, Gender)
    assembler = VectorAssembler(
        inputCols=[
            "CreditScore", "Age", "Tenure", "Balance",
            "NumOfProducts", "EstimatedSalary"
        ],
        outputCol="features"
    )
    
    # Feature scaling
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaledFeatures"
    )
    
    # Model
    lr = LogisticRegression(
        labelCol="Exited",
        featuresCol="scaledFeatures",
        maxIter=100
    )
    
    # Build pipeline
    pipeline = Pipeline(stages=[
        assembler,
        scaler,
        lr
    ])
    
    return pipeline

def evaluate_model(predictions, label_col="Exited"):
    """Evaluate model with multiple metrics"""
    
    # Accuracy
    accuracy_eval = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = accuracy_eval.evaluate(predictions)
    
    # Precision
    precision_eval = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="weightedPrecision"
    )
    precision = precision_eval.evaluate(predictions)
    
    # Recall
    recall_eval = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="weightedRecall"
    )
    recall = recall_eval.evaluate(predictions)
    
    # F1 Score
    f1_eval = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="f1"
    )
    f1 = f1_eval.evaluate(predictions)
    
    # AUC
    auc_eval = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc = auc_eval.evaluate(predictions)
    
    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "auc": auc
    }

def run_pipeline(pipeline, train_data, test_data, pipeline_name):
    """Train and evaluate a pipeline"""
    
    print(f"\n{'='*50}")
    print(f"Running: {pipeline_name}")
    print('='*50)
    
    # Train
    start_time = time.time()
    model = pipeline.fit(train_data)
    train_time = time.time() - start_time
    print(f"Training time: {train_time:.2f} seconds")
    
    # Predict
    start_time = time.time()
    predictions = model.transform(test_data)
    predict_time = time.time() - start_time
    print(f"Prediction time: {predict_time:.2f} seconds")
    
    # Show sample predictions
    print("\nSample Predictions:")
    predictions.select("Exited", "prediction", "probability").show(10)
    
    # Evaluate
    metrics = evaluate_model(predictions)
    
    print(f"\n{pipeline_name} Results:")
    print(f"  Accuracy:  {metrics['accuracy']:.4f}")
    print(f"  Precision: {metrics['precision']:.4f}")
    print(f"  Recall:    {metrics['recall']:.4f}")
    print(f"  F1 Score:  {metrics['f1']:.4f}")
    print(f"  AUC:       {metrics['auc']:.4f}")
    
    return metrics, train_time

def main():
    """Main function"""
    
    print("="*60)
    print("Lab 6: Spark ML Pipeline - Customer Churn Prediction")
    print("Experiment B: Feature Ablation")
    print("="*60)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Load data
    data_path = "hdfs:///user/hadoop/churn_input/Churn_Modelling.csv"
    data = load_data(spark, data_path)
    
    # Show data sample
    print("\nData Sample:")
    data.show(5)
    
    # Split data
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
    print(f"\nTrain size: {train_data.count()}")
    print(f"Test size: {test_data.count()}")
    
    # Run full pipeline (with categorical features)
    full_pipeline = build_full_pipeline()
    full_metrics, full_time = run_pipeline(
        full_pipeline, train_data, test_data,
        "Full Pipeline (with Geography & Gender)"
    )
    
    # Run numerical-only pipeline (without categorical features)
    numerical_pipeline = build_numerical_only_pipeline()
    numerical_metrics, numerical_time = run_pipeline(
        numerical_pipeline, train_data, test_data,
        "Numerical Only Pipeline (without Geography & Gender)"
    )
    
    # Comparison Summary
    print("\n" + "="*60)
    print("EXPERIMENT B: FEATURE ABLATION RESULTS")
    print("="*60)
    print("\n| Metric    | Full Pipeline | Numerical Only | Difference |")
    print("|-----------|---------------|----------------|------------|")
    print(f"| Accuracy  | {full_metrics['accuracy']:.4f}        | {numerical_metrics['accuracy']:.4f}         | {full_metrics['accuracy'] - numerical_metrics['accuracy']:+.4f}     |")
    print(f"| Precision | {full_metrics['precision']:.4f}        | {numerical_metrics['precision']:.4f}         | {full_metrics['precision'] - numerical_metrics['precision']:+.4f}     |")
    print(f"| Recall    | {full_metrics['recall']:.4f}        | {numerical_metrics['recall']:.4f}         | {full_metrics['recall'] - numerical_metrics['recall']:+.4f}     |")
    print(f"| F1 Score  | {full_metrics['f1']:.4f}        | {numerical_metrics['f1']:.4f}         | {full_metrics['f1'] - numerical_metrics['f1']:+.4f}     |")
    print(f"| AUC       | {full_metrics['auc']:.4f}        | {numerical_metrics['auc']:.4f}         | {full_metrics['auc'] - numerical_metrics['auc']:+.4f}     |")
    print(f"| Time (s)  | {full_time:.2f}         | {numerical_time:.2f}          | {full_time - numerical_time:+.2f}      |")
    
    print("\nObservation:")
    if full_metrics['accuracy'] > numerical_metrics['accuracy']:
        print("  → Full pipeline (with categorical features) performs BETTER")
        print("  → Geography and Gender are important features for churn prediction")
    else:
        print("  → Numerical-only pipeline performs similar or better")
        print("  → Categorical features may not significantly impact prediction")
    
    print("\n" + "="*60)
    print("Pipeline completed successfully!")
    print("="*60)
    
    # Stop Spark
    spark.stop()

if __name__ == "__main__":
    main()