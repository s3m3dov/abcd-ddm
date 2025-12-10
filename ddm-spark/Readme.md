# ddm-spark

Apache Spark implementation for discovering unary Inclusion Dependencies (INDs) using cluster computing.

> **Based on:** https://github.com/UMR-Big-Data-Analytics/ddm-spark  
> **Course:** Big Data Systems

## Table of Contents

- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Running the Project](#running-the-project)
- [Building for Cluster Deployment](#building-for-cluster-deployment)
- [Cluster Submission Notes](#cluster-submission-notes)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

## Requirements

| Requirement | Version/Details |
|-------------|-----------------|
| Java | <= 11 (**Java 11 recommended**) |
| Scala | 2.12 |
| IDE | IntelliJ IDEA |
| Plugins | Scala plugin, sbt plugin |

> ⚠️ **Important:** This project is only compatible with Java 11 or earlier. Java versions > 11 will cause compatibility issues.

## Getting Started

### 1. Navigate to Folder

```bash
cd ddm-spark
```

### 2. Install IntelliJ IDEA

During installation, ensure you select:
- ✅ Scala plugin
- ✅ sbt plugin

If you missed these during installation, install them later via:
**File → Settings → Plugins → Marketplace**

### 3. Import Project into IntelliJ

1. Start IntelliJ IDEA
2. Select **Open or Import**
3. Navigate to and select `ddm-spark/build.sbt`
4. Click **OK**
5. Click **Open as Project**

> **Note:** Initial setup may take several minutes for dependency downloads and indexing. If dependencies don't download automatically, open the **sbt** tab (right edge of screen) and click **Reload all sbt projects**.

### 4. Download Test Data

1. Download TPCH dataset: https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/lehre/WS2017/DDA/TPCH.zip
2. Extract contents into `ddm-spark/data/`
3. Verify structure: `ddm-spark/data/TPCH/` should contain 7 CSV files

```bash
# From project root
cd data
# Extract TPCH.zip here
unzip TPCH.zip
```

### 5. Verify Setup

Check that `ddm-spark/data/TPCH/` contains the following files:
- `customer.csv`
- `lineitem.csv`
- `nation.csv`
- `orders.csv`
- `part.csv`
- `partsupp.csv`
- `region.csv`
- `supplier.csv`

## Running the Project

### Run the Tutorial

1. In IntelliJ, open **ddm-spark/src/main/scala/de/ddm** in the project tree
2. Right-click on **DDMSpark.scala**
3. Click **Run 'DDMSpark'**

### Implement the Homework

1. Open `DDMSpark.scala`
2. Remove the Tutorial and LongestCommonSubstring calls
3. Implement the **Sindy algorithm** for IND discovery

## Building for Cluster Deployment

### Create a Fat JAR

1. Go to **Run → Edit Configurations**
2. Click **+** → **SBT Task**
3. Configure:
   - **Name:** `DDMSpark assembly`
   - **Tasks:** `clean assembly`
4. Click **OK**
5. Select **DDMSpark assembly** from the run configurations dropdown (top right)
6. Click **Run**

### Locate the JAR

After successful build, find your fat-jar at:
```
ddm-spark/target/scala-2.12/DDMSpark-assembly-0.1.jar
```

### Submit to Spark Cluster

```bash
spark-submit \
  --class de.ddm.DDMSpark \
  --master <spark-master-url> \
  target/scala-2.12/DDMSpark-assembly-0.1.jar
```

## Cluster Submission Notes

When deploying to a Spark cluster, ensure version compatibility:

### Version Matching Requirements

Modify `build.sbt` to match your target Spark installation:

```scala
// Must match cluster's Scala version
scalaVersion := "2.12.x"

// Must match cluster's Spark version
libraryDependencies += "org.apache.spark" %% "spark-core" % "x.x.x"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "x.x.x"
```

Replace `x.x.x` with the exact Spark version installed on your cluster.

> ⚠️ **Critical:** If Scala and Spark versions don't exactly match the cluster installation, you will encounter runtime errors that are difficult to diagnose.

### Checking Cluster Versions

On the cluster, verify versions with:
```bash
spark-submit --version
scala -version
```

## Project Structure

```text
ddm-spark/
├── data/
│   └── TPCH/                    # Test dataset (7 CSV files)
├── src/
│   └── main/
│       └── scala/
│           └── de/
│               └── ddm/
│                   └── DDMSpark.scala    # Main entry point
├── target/
│   └── scala-2.12/
│       └── DDMSpark-assembly-0.1.jar     # Built fat-jar
├── build.sbt                    # Build configuration
├── project/
│   └── plugins.sbt              # sbt plugins
└── README.md                    # This file
```

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Java version incompatible | Install and use Java 11 (not higher) |
| Dependencies not downloading | Click **Reload all sbt projects** in sbt tab |
| Import errors in IDE | Invalidate caches: **File → Invalidate Caches** |
| Assembly fails | Check for dependency conflicts in `build.sbt` |
| Cluster submit fails | Verify Scala/Spark versions match cluster |

### Verifying Java Version

```bash
java -version
```

Should output Java 11 or earlier. If you have multiple Java versions, configure IntelliJ to use Java 11:
**File → Project Structure → Project → SDK**

### sbt Commands Reference

| Command | Description |
|---------|-------------|
| `sbt compile` | Compile the project |
| `sbt run` | Run the main class |
| `sbt clean` | Clean build artifacts |
| `sbt assembly` | Create fat-jar |
| `sbt clean assembly` | Clean and create fat-jar |

## References

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Scala Documentation](https://docs.scala-lang.org/)
- [sbt Documentation](https://www.scala-sbt.org/documentation.html)
- [Course Repository](https://github.com/UMR-Big-Data-Analytics/ddm-spark)