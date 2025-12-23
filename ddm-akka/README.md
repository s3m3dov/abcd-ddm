# ddm-akka

Akka-based implementation for discovering unary Inclusion Dependencies (INDs) using actor-based distributed computing.

> **Based on:** https://github.com/UMR-Big-Data-Analytics/ddm-akka  
> **Course:** Big Data Systems

## Table of Contents

- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Run Configurations](#run-configurations)
  - [Demo Mode](#demo-mode)
  - [Local Distributed Run](#local-distributed-run-single-machine)
  - [Multi-Machine Distributed Run](#multi-machine-distributed-run)
- [Checking Results](#checking-results)
- [Memory Configuration](#memory-configuration)
- [Hints & Best Practices](#hints--best-practices)
- [Troubleshooting](#troubleshooting)

## Requirements

| Requirement | Version |
|-------------|---------|
| Java | >= 21 |
| Maven | >= 3.9.11 |
| Akka Account | Required (free for development) |

## Getting Started

### 1. Navigate to Folder

```bash
cd ddm-akka
```

### 2. Configure Akka Access Token

The Akka license requires an access token to download the libraries.

1. Create an account at [Akka](https://account.akka.io/)
2. Get your token from [Akka Token Page](https://account.akka.io/token)
3. Configure Maven settings:

**Linux/Mac:** `~/.m2/settings.xml`  
**Windows:** `C:\Users\<username>\.m2\settings.xml`

Create the file if it doesn't exist and add:

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0" 
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                              http://maven.apache.org/xsd/settings-1.0.0.xsd">
    <activeProfiles>
        <activeProfile>akka</activeProfile>
    </activeProfiles>
    <profiles>
        <profile>
            <id>akka</id>
            <repositories>
                <repository>
                    <id>akka-secure</id>
                    <name>Akka Secure</name>
                    <url>https://repo.akka.io/#TOKEN/secure</url>
                </repository>
            </repositories>
        </profile>
    </profiles>
</settings>
```

> ⚠️ **Security Note:** Replace `#TOKEN` with your actual token. Never add the repository to `pom.xml` as this would expose your token publicly.

### 3. Download and Extract Test Data

```bash
cd data
# Download from: https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/lehre/WS2017/DDA/TPCH.zip
unzip TPCH.zip
cd ..
```

### 4. Build the Project

```bash
mvn clean package
```

### 5. View Help Documentation

```bash
java -jar target/ddm-akka.jar
```

This displays all available parameters and their descriptions.

## Run Configurations

### Demo Mode

For initial testing and demonstration:

```bash
java -jar target/ddm-akka.jar master -es true
```

> **Note:** The `-es true` flag is for demo purposes only. Press `Enter` to terminate. A working solution should end itself automatically once complete. Skip this flag (`-es false` or omit it) for production runs.

### Local Undistributed Run (Single Process)
Run master and workers in a single process (not distributed):

```bash
java -Xms2048m -Xmx2048m -jar target/ddm-akka.jar master -w 4
```

### Local Distributed Run (Single Machine, 1024 MB, 2+ Worker Nodes) 

Run master and multiple workers on the same machine using different terminal windows:

**Terminal 1 - Master (no local workers):**
```bash
java -Xms1024m -Xmx1024m -jar target/ddm-akka.jar master -w 0
```

**Terminal 2 - Worker 1 (default port 7879):**
```bash
java -Xms1024m -Xmx512m -jar target/ddm-akka.jar worker -w 4
```

### Local Distributed Run (Single Machine, 512 MB, 5+ Worker Nodes) 

Run master and multiple workers on the same machine using different terminal windows:

**Terminal 1 - Master (no local workers):**
```bash
java -Xms512m -Xmx512m -jar target/ddm-akka.jar master -w 0
```

**Terminal 2 - Worker 1 (default port 7879):**
```bash
java -Xms512m -Xmx512m -jar target/ddm-akka.jar worker -w 2
```

**Terminal 3 - Worker 2 (custom port):**
```bash
java -Xms512m -Xmx512m -jar target/ddm-akka.jar worker -w 2 -p 7880
```

change port number for each additional worker.

> **Important:** Each worker on the same machine must use a different port (`-p` flag).

### Multi-Machine Distributed Run

For running across multiple physical machines or nodes:

**Machine 1 - Master:**
```bash
java -Xms1024m -Xmx1024m -jar target/ddm-akka.jar master -w 0 -h <master-ip>
```

**Machine 2 - Worker:**
```bash
java -Xms1024m -Xmx1024m -jar target/ddm-akka.jar worker -w 1 -mh <master-ip> -h <worker2-ip>
```

**Machine 3 - Worker:**
```bash
java -Xms1024m -Xmx1024m -jar target/ddm-akka.jar worker -w 1 -mh <master-ip> -h <worker3-ip>
```

**Finding IP Addresses:**
- Linux/Mac: `hostname -I` or `ifconfig`
- Windows: `ipconfig`
- Online: https://whatismyipaddress.com/

### Command Line Parameters Summary

| Parameter | Description | Default |
|-----------|-------------|---------|
| `-w <n>` | Number of local workers | - |
| `-h <ip>` | Host IP address | localhost |
| `-mh <ip>` | Master host IP | localhost |
| `-p <port>` | Worker port | 7879 |
| `-es <bool>` | End on signal (demo mode) | false |

## Checking Results

After running the implementation, validate your results:

```bash
./checkResultFiles.sh <your-result-file> <expected-result-file>
```

The script outputs:
- **True:** Correctly found INDs
- **False:** Incorrectly reported INDs (false positives)
- **Missing:** INDs that should have been found but weren't (false negatives)
- **Summary:** Statistics of all, found, false, and missing INDs

**Example:**
```bash
./checkResultFiles.sh results/myresults.txt results/expected.txt
```

## Memory Configuration

### JVM Heap Settings

| Flag | Description |
|------|-------------|
| `-Xms<size>` | Initial heap size |
| `-Xmx<size>` | Maximum heap size |

### Recommended Configurations

**Pi Cluster (strict constraints):**
```bash
java -Xms512m -Xmx512m -jar target/ddm-akka.jar ...
```

**Development Machine (more resources):**
```bash
java -Xms2048m -Xmx2048m -jar target/ddm-akka.jar ...
```

> ⚠️ **Pi Cluster Limit:** Maximum heap size must not exceed 2GB (`-Xmx2048m` or `-Xmx2g`) to ensure compatibility with Pi cluster constraints.

## Hints & Best Practices

1. **Help Text:** Run without arguments to see all parameter descriptions:
   ```bash
   java -jar target/ddm-akka.jar
   ```

2. **Memory Testing:** Use restricted memory during development to ensure Pi cluster compatibility:
   ```bash
   java -Xms512m -Xmx512m -jar target/ddm-akka.jar ...
   ```

3. **Large Messages:** Use `LargeMessageProxy` for sending large messages, but be aware of the extra memory overhead this proxy pattern requires.

4. **Memory Profiling:** Use `MemoryUtils` to measure byte sizes and memory usage.
   > ⚠️ Memory measurements in Java are computationally expensive - use sparingly.

5. **Graceful Termination:** A properly implemented solution should terminate automatically without requiring `-es true`.

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Maven can't find Akka dependencies | Verify `settings.xml` configuration and token |
| Out of memory errors | Reduce `-Xmx` value or optimize algorithm |
| Connection refused between nodes | Check firewall settings and IP addresses |
| Port already in use | Use `-p` flag to specify different port |

### Verifying Setup

1. Check Java version: `java -version` (should be >= 21)
2. Check Maven version: `mvn -version` (should be >= 3.9.11)
3. Verify TPCH data exists: `ls data/TPCH/`

## Project Structure

```text
ddm-akka/
├── data/
│   └── TPCH/           # Test dataset (7 CSV files)
├── results/            # Output directory
├── src/                # Java source code
├── target/             # Build output
│   └── ddm-akka.jar    # Executable JAR
├── checkResultFiles.sh # Result validation script
├── pom.xml             # Maven configuration
└── README.md           # This file
```

## References

- [Akka Documentation](https://doc.akka.io/)
- [Course Repository](https://github.com/UMR-Big-Data-Analytics/ddm-akka)
- [IBM JVM Options Guide](https://www.ibm.com/docs/en/sdk-java-technology/8?topic=options-xms)