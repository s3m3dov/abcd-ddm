# ABCD DDM

Distributed Data Management project implementing algorithms for discovering unary Inclusion Dependencies (INDs) using distributed computing frameworks.

## Overview

This repository contains implementations of distributed data management algorithms using two different frameworks:

- **Akka**: Actor-based distributed computing implementation
- **Spark**: Cluster computing implementation

Both implementations focus on discovering unary Inclusion Dependencies (INDs) in large datasets that don't fit into the memory of a single node, exploiting distributed cluster resources for efficient processing.

## Project Structure

```text
abcd-ddm/
├── ddm-akka/          # Akka-based implementation
│   ├── data/          # Test datasets (TPCH)
│   ├── src/           # Source code
│   ├── results/       # Output results
│   └── ReadMe.md      # Detailed Akka setup and usage
│
├── ddm-spark/         # Spark-based implementation
│   ├── data/          # Test datasets (TPCH)
│   ├── src/           # Scala source code
│   └── ReadMe.md      # Detailed Spark setup and usage
│
└── README.md          # This file
```

## Prerequisites

### For Akka Project

- Java Version >= 21
- Maven Compiler Version >= 3.9.11
- Akka account and access token (free for development)

### For Spark Project

- Java Version <= 11 (Java 11 recommended)
- Scala 2.12
- IntelliJ IDEA with Scala and sbt plugins

## Quick Start

### Running the Akka Project

1. Navigate to the Akka project directory:

   ```bash
   cd ddm-akka
   ```

2. Follow the detailed setup instructions in [`ddm-akka/README.md`](ddm-akka/ReadMe.md)

3. Build and run:

   ```bash
   mvn clean package
   java -jar target/ddm-akka.jar master -es true
   ```

#### Different Run Variations

**First Run (Demo Mode)**

```bash
cd ddm-akka
java -jar target/ddm-akka.jar master -es true
```

The `-es true` flag is for demo purposes only. Press `enter` to terminate. A working solution should end itself once done.

**Distributed Run (Locally on One Machine)**

```bash
# Terminal 1: Run master
java -Xms2048m -Xmx2048m -jar target/ddm-akka.jar master -w 0

# Terminal 2+: Run workers (repeat in different terminals)
java -Xms2048m -Xmx2048m -jar target/ddm-akka.jar worker -w 1
java -Xms2048m -Xmx2048m -jar target/ddm-akka.jar worker -w 2
```

**Distributed Run (Multiple Machines)**

```bash
# On master machine:
java -Xms2048m -Xmx2048m -jar target/ddm-akka.jar master -w 0 -h <your-ip-address>

# On worker machines:
java -Xms2048m -Xmx2048m -jar target/ddm-akka.jar worker -w 1 -mh <master-host-ip> -h <your-ip-address>
```

> Note: Memory limits (`-Xms2048m -Xmx2048m`) ensure compatibility with Pi cluster constraints. Replace `<your-ip-address>` and `<master-host-ip>` with actual IP addresses.

#### Checking Results

After running the Akka implementation, verify the results:

```bash
cd ddm-akka
./checkResultFiles.sh <your-result-file> <expected-result-file>
```

This script validates the output files in the `results/` directory against expected outcomes, showing:

- **True**: Correctly found INDs
- **False**: Incorrectly reported INDs
- **Missing**: INDs that should have been found but weren't
- **Summary**: Statistics of all, found, false, and missing INDs

For distributed execution and advanced configuration, see the [Akka README](ddm-akka/ReadMe.md).

### Running the Spark Project

1. Navigate to the Spark project directory:

   ```bash
   cd ddm-spark
   ```

2. Follow the detailed setup instructions in [`ddm-spark/ReadMe.md`](ddm-spark/ReadMe.md)

3. Import the project into IntelliJ and run `DDMSpark.scala`

For building JAR files and cluster submission, see the [Spark README](ddm-spark/ReadMe.md).

## Data

Both projects use the TPCH dataset for testing:

- Download from: <https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/lehre/WS2017/DDA/TPCH.zip>
- Extract to the respective `data/` directory in each subproject

## Results

After running the algorithms, check the results:

- **Akka**: Results are stored in `ddm-akka/results/` - use `checkResultFiles.sh` to validate
- **Spark**: Results are output to the console or specified output directory

## Documentation

For detailed information about each implementation:

- [Akka Implementation Guide](ddm-akka/ReadMe.md)
- [Spark Implementation Guide](ddm-spark/ReadMe.md)

## Team

**ABCD** - Distributed Data Management Course Project

## License

See individual LICENSE files in each subproject directory.
