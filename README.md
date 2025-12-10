# ABCD DDM

Big Data Systems project implementing algorithms for discovering unary Inclusion Dependencies (INDs) using distributed computing frameworks.

## Overview

This repository contains implementations of distributed data management algorithms using two different frameworks:

- **Akka**: Actor-based distributed computing implementation
- **Spark**: Apache Spark cluster computing implementation

Both implementations focus on discovering unary Inclusion Dependencies (INDs) in large datasets that don't fit into the memory of a single node, exploiting distributed cluster resources for efficient processing.

## Project Structure

```text
abcd-ddm/
├── ddm-akka/              # Akka-based implementation
│   ├── data/              # Test datasets (TPCH)
│   ├── src/               # Java source code
│   ├── results/           # Output results
│   └── README.md          # Detailed Akka setup and usage
│
├── ddm-spark/             # Spark-based implementation
│   ├── data/              # Test datasets (TPCH)
│   ├── src/               # Scala source code
│   └── README.md          # Detailed Spark setup and usage
│
└── README.md              # This file
```

## Prerequisites

| Requirement | Akka Project | Spark Project |
|-------------|--------------|---------------|
| Java Version | >= 21 | <= 11 (Java 11 recommended) |
| Build Tool | Maven >= 3.9.11 | sbt (via IntelliJ) |
| Additional | Akka account & token | Scala 2.12, IntelliJ IDEA |

## Quick Start

### Clone Repository

```bash
git clone git@github.com:s3m3dov/abcd-ddm.git
```

### Akka Project

```bash
cd ddm-akka
mvn clean package
java -jar target/ddm-akka.jar master -es true
```

For detailed setup, distributed execution, and result validation, see the **[Akka Implementation Guide](ddm-akka/README.md)**.

### Spark Project

```bash
cd ddm-spark
# Open in IntelliJ IDEA and run DDMSpark.scala
```

For detailed setup, JAR building, and cluster submission, see the **[Spark Implementation Guide](ddm-spark/README.md)**.

## Data

Both projects use the TPCH dataset for testing:

1. Download from: https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/lehre/WS2017/DDA/TPCH.zip
2. Extract to the respective `data/` directory in each subproject

## Results

| Project | Output Location | Validation |
|---------|-----------------|------------|
| Akka | `ddm-akka/results/` | Use `checkResultFiles.sh` script |
| Spark | Console or specified output directory | Manual verification |

## Documentation

- [Akka Implementation Guide](ddm-akka/README.md) - Complete setup, run configurations, and troubleshooting
- [Spark Implementation Guide](ddm-spark/README.md) - IntelliJ setup, JAR building, and cluster submission

## Repository Information

This project is based on course materials from the "Distributed Data Management" lecture.

**Base Repositories:**
- Akka: https://github.com/UMR-Big-Data-Analytics/ddm-akka
- Spark: https://github.com/UMR-Big-Data-Analytics/ddm-spark

## Team

**ABCD** - Big Data Systems Course Project

Members:
- Hikmat Samadov ([@s3m3dov](https://github.com/s3m3dov))
- Sahil Hashimli ([@hashimlijr](https://github.com/hashimlijr))

## License

See individual LICENSE files in each subproject directory.