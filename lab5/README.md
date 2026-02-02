# Lab 5: Mini-MapReduce on Amazon EMR

## Overview

WordCount MapReduce job running on Amazon EMR cluster with Hadoop streaming.

## Dataset

- **Source:** Wikipedia Simple English dump
- **URL:** https://github.com/LGDoor/Dump-of-Simple-English-Wiki
- **Full dataset:** ~33MB, 254,110 lines
- **Small dataset:** ~10,000 lines (for experiment)

## Cluster Configuration

| Node Type | Instance | Count |
| --------- | -------- | ----- |
| Primary   | m4.large | 1     |
| Core      | m4.large | 2     |

## Quick Start

### 1. SSH into Master Node

```bash
ssh -i labsuser.pem hadoop@<master-dns>
```

### 2. Install Git and Clone Repository

```bash
sudo yum install git -y
git clone https://github.com/Tretorhate/dist_comp.git
```

### 3. Download and Upload Dataset to HDFS

```bash
wget https://github.com/LGDoor/Dump-of-Simple-English-Wiki/raw/refs/heads/master/corpus.tgz
tar -xvzf corpus.tgz
hdfs dfs -mkdir -p /user/hadoop/input
hdfs dfs -put corpus.txt /user/hadoop/input/
```

### 4. Create Small Dataset for Experiment

```bash
head -10000 corpus.txt > corpus_small.txt
hdfs dfs -mkdir -p /user/hadoop/input_small
hdfs dfs -put corpus_small.txt /user/hadoop/input_small/
```

### 5. Verify HDFS Upload

```bash
hdfs dfs -ls /user/hadoop/input/
hdfs dfs -ls /user/hadoop/input_small/
```

### 6. Run MapReduce Job

```bash
cd ~/dist_comp/lab5
chmod +x *.py

STREAMING_JAR=$(find /usr/lib -name "hadoop-streaming*.jar" 2>/dev/null | head -1)

# Run on full dataset
hadoop jar $STREAMING_JAR -input /user/hadoop/input/ -output /user/hadoop/output/wordcount_full -mapper mapper.py -reducer reducer.py -file mapper.py -file reducer.py

# Run on small dataset
hadoop jar $STREAMING_JAR -input /user/hadoop/input_small/ -output /user/hadoop/output/wordcount_small -mapper mapper.py -reducer reducer.py -file mapper.py -file reducer.py
```

### 7. View Results

```bash
# List output files
hdfs dfs -ls /user/hadoop/output/wordcount_full/
hdfs dfs -ls /user/hadoop/output/wordcount_small/

# Top 20 most frequent words (full dataset)
hdfs dfs -cat /user/hadoop/output/wordcount_full/part-* | sort -t$'\t' -k2 -nr | head -20

# Top 20 most frequent words (small dataset)
hdfs dfs -cat /user/hadoop/output/wordcount_small/part-* | sort -t$'\t' -k2 -nr | head -20
```

### 8. Clear Output (if rerunning)

```bash
hdfs dfs -rm -r /user/hadoop/output/wordcount_full
hdfs dfs -rm -r /user/hadoop/output/wordcount_small
```

## Utility Commands

### Check Cluster Status

```bash
yarn node -list
hdfs dfsadmin -report
```

### Test Scripts Locally

```bash
echo "hello world hello" | python3 mapper.py | sort | python3 reducer.py
```

## Experiment: Scenario B — Input Size Comparison

| Dataset | Size   | Lines   | Job Duration |
| ------- | ------ | ------- | ------------ |
| Small   | ~400KB | 10,000  | ~15-20 sec   |
| Full    | ~33MB  | 254,110 | ~1 min       |

**Observation:** Larger input size increases job duration proportionally. The MapReduce framework splits input into chunks processed in parallel, but more data means more map tasks and more data to shuffle/reduce.

**Key metrics from full dataset run:**

- Map input records: 254,110
- Map output records: 5,540,777
- Reduce output records: 342,608 unique words
- 8 map tasks, 4 reduce tasks

## Files

- `mapper.py` - Emits (word, 1) pairs for each word
- `reducer.py` - Aggregates counts by word
- `README.md` - This file

## Author

Tret - Distributed Computing Lab 5
