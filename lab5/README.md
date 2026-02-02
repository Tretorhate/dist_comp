# Lab 5: Mini-MapReduce on Amazon EMR

## Overview

WordCount MapReduce job running on Amazon EMR cluster with Hadoop streaming.

## Dataset

- **Source:** Wikipedia Simple English dump (~33MB text)
- **URL:** https://github.com/LGDoor/Dump-of-Simple-English-Wiki
- **Records:** 254,110 lines
- **Output:** 342,608 unique words

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

### 4. Verify HDFS Upload

```bash
hdfs dfs -ls /user/hadoop/input/
```

### 5. Run MapReduce Job

```bash
cd ~/dist_comp/lab5
chmod +x *.py

STREAMING_JAR=$(find /usr/lib -name "hadoop-streaming*.jar" 2>/dev/null | head -1)

hadoop jar $STREAMING_JAR \
  -input /user/hadoop/input/ \
  -output /user/hadoop/output/wordcount \
  -mapper mapper.py \
  -reducer reducer.py \
  -file mapper.py \
  -file reducer.py
```

### 6. View Results

```bash
# List output files
hdfs dfs -ls /user/hadoop/output/wordcount/

# View sample
hdfs dfs -head /user/hadoop/output/wordcount/part-00000

# Top 20 most frequent words
hdfs dfs -cat /user/hadoop/output/wordcount/part-* | sort -t$'\t' -k2 -nr | head -20
```

### 7. Clear Output (if rerunning)

```bash
hdfs dfs -rm -r /user/hadoop/output/wordcount
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

## Experiment: Scaling (2 vs 4 Core Nodes)

| Nodes | Job Duration |
| ----- | ------------ |
| 2     | ~1 min       |
| 4     | ~45 sec      |

**Observation:** Adding more core nodes reduces job execution time due to increased parallelism in the map phase.

## Files

- `mapper.py` - Emits (word, 1) pairs for each word
- `reducer.py` - Aggregates counts by word

## Author

Tret - Distributed Computing Lab 5
