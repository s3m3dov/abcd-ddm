# Project Documentation: ddm-akka

This document provides a comprehensive overview of the `ddm-akka` project, covering its architecture, message flows, core logic, and key features.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Application Flow & Messages](#application-flow--messages)
    - [Phase 1: Cluster Discovery & Actor Recruitment](#phase-1-cluster-discovery--actor-recruitment)
    - [Phase 2: Data Distribution (Loading Phase)](#phase-2-data-distribution-loading-phase)
    - [Phase 3: Transition Phase](#phase-3-transition-phase)
    - [Phase 4: Validation Phase](#phase-4-validation-phase)
3. [Important Logic & Decisions](#important-logic--decisions)
4. [Completed Work & Features](#completed-work--features)
5. [Summary of Performance Optimizations](#summary-of-performance-optimizations)

---

## Architecture Overview

The project is built on the **Akka Typed** actor framework, designed for distributed unary Inclusion Dependency (IND) discovery. It uses a master-worker architecture where the workload is distributed across multiple nodes (JVM processes).

### Actor Hierarchy

```mermaid
graph TD
    Guardian["Guardian (Root)"] --> Master["Master (Master Role)"]
    Guardian --> Worker["Worker (Worker Role)"]
    
    subgraph Master Process
        Master --> DependencyMiner["DependencyMiner"]
        Master --> ResultCollector["ResultCollector"]
        DependencyMiner --> InputReader["InputReader"]
    end
    
    subgraph Worker Process / Local Workers
        Worker --> DataProvider["DataProvider (Local)"]
        Worker --> DW1["DependencyWorker-1"]
        Worker --> DWN["DependencyWorker-N"]
    end
    
    DW1 <--> DataProvider
    DWN <--> DataProvider
    DependencyMiner <--> DW1
    DependencyMiner <--> DataProvider
```

> `DataProvider` and `DependencyWorker`s are only spawned if the number of local workers (`-w`) is greater than 0. Each worker node (or the master if `-w > 0`) maintains exactly one `DataProvider` that serves all local `DependencyWorker`s.

---

## Application Flow & Messages

The discovery process is divided into several distinct phases, each with specific actor interactions.

### Phase 1: Cluster Discovery & Actor Recruitment

The system uses a dynamic discovery mechanism via the Akka Receptionist to coordinate between the Master and Worker nodes.

```mermaid
graph TD
    %% Nodes
    Master(("Master"))
    DM(("DependencyMiner"))
    IR(("InputReader<br/>(per file)"))
    RC(("ResultCollector"))
    R(("Receptionist"))
    Worker(("Worker"))
    DP(("DataProvider"))
    DW(("DependencyWorker<br/>(n)"))

    %% Spawning Logic
    Master -- Spawn --> DM
    DM -- Spawn --> IR
    DM -- Spawn --> RC
    Worker -- Spawn --> DP
    Worker -- Spawn --> DW

    %% References & Communication
    DW -. "Has Reference To" .-> DP
    DM -. "Register to Receptionist" .-> R
    
    DW -. "Sends RegisterMessage to Miner via Receptionist" .-> R
    DP -. "Sends StatusMessage to Miner via Receptionist" .-> R
    R -.-> DM

    %% Styles
    classDef master fill:#E6E0FF,stroke:#B3A1FF,stroke-width:2px;
    classDef miner fill:#C1F2D0,stroke:#85E0A3,stroke-width:2px;
    classDef component fill:#CCE0FF,stroke:#99C2FF,stroke-width:2px;
    classDef receptionist fill:#E6E6E6,stroke:#CCCCCC,stroke-width:2px;

    class Master,Worker master;
    class DM miner;
    class IR,RC,DP,DW component;
    class R receptionist;
```

1. **Actor Startup**:
    - The **Master** node role spawns the `DependencyMiner`.
    - `DependencyMiner` immediately spawns the `InputReader`s and the `ResultCollector`.
    - The **Worker** node role spawns one `DataProvider` and multiple `DependencyWorker`s (if `-w > 0`).
2. **Discovery**:
    - `DependencyMiner` registers itself with the `Receptionist`.
    - `DataProvider` and `DependencyWorker` use the `Receptionist` to find and communicate with the `DependencyMiner`.
3. **Registration**:
    - Workers send their registration and status messages to the Miner to join the cluster's active resource pool.

### Phase 2: Data Distribution (Loading Phase)

The `DependencyMiner` coordinates the movement of data from the source files to the distributed `DataProvider` memory.

```mermaid
sequenceDiagram
    participant IR as InputReader
    participant DM as DependencyMiner
    participant DP1 as DataProvider A
    participant DP2 as DataProvider B

    IR->>DM: HeaderMessage (Schema)
    Note over DM: Round-Robin Attribute Mapping
    IR->>DM: BatchMessage (CSV Rows)
    
    par Stream to DP A
        DM->>DP1: DataMessage (Attr 0, 2, 4)
    and Stream to DP B
        DM->>DP2: DataMessage (Attr 1, 3, 5)
    end

    Note over DP1, DP2: Memory Status Reporting
    
    opt If DP A Overloaded
        DM->>DP1: MoveDataMessage (Attr 4)
        DP1->>DP2: AcceptDataMessage
        Note over DM: Update Mapping
    end
```

### Phase 3: Transition Phase

Once all data is loaded, the system prepares for validation by sorting data and building probabilistic filters.

```mermaid
sequenceDiagram
    participant DM as DependencyMiner
    participant DP as DataProvider (Master/Worker)

    DM->>DP: TransitionToValidationPhaseMessage
    Note over DP: Sort Columns (Lexicographic)
    Note over DP: Build ColumnStatistics (Min, Max, Cardinality)
    Note over DP: Build BloomFilters
    DP->>DM: DataProviderReadyForValidationMessage
```

### Phase 4: Validation Phase

The core discovery logic where attribute pairs are checked for inclusion dependencies using multiple pruning layers.

```mermaid
sequenceDiagram
    participant DM as DependencyMiner
    participant DW as DependencyWorker
    participant DP as DataProvider (Remote/Local)
    participant RC as ResultCollector

    DM->>DW: ValidateTaskMessage (lhs, rhs)
    DW->>DP: RequestColumnStatistics (lhs, rhs)
    DP-->>DW: ColumnStatisticsMessage
    Note over DW: Pruning (Transitivity, Cardinality, Range)
    alt Inconclusive
        DW->>DP: RequestBloomFilter (rhs)
        DP-->>DW: BloomFilterResponseMessage
        Note over DW: Bloom Filter Pruning
    end
    alt Still Inconclusive
        DW->>DP: RequestColumnData (lhs, rhs)
        DP-->>DW: ColumnDataMessage
        Note over DW: Sorted Subset Check (O(n+m))
    end
    DW->>DM: ResultMessage (IND found/not found)
    DM->>RC: InclusionDependency (if found)
```

---

## Important Logic & Decisions

### Pruning Strategies

To minimize expensive data transfers and comparisons, the system employs several pruning layers:

1. **Transitivity Pruning**: If we know $A \subseteq B$ and $B \subseteq C$, then $A \subseteq C$ is inferred and the validation task is skipped.
2. **Cardinality Pruning**: If $|A| > |B|$, then $A \not\subseteq B$.
3. **Range Pruning**: If $\min(A) < \min(B)$ or $\max(A) > \max(B)$, then $A \not\subseteq B$.
4. **Bloom Filter Pruning**: A probabilistic check using RHS Bloom filter to see if elements of $A$ are present in $B$. If an element is definitely missing, the IND is rejected.

### Memory-Efficient Data Structures

- **Sorted Columns**: Storing columns as sorted `ArrayList<String>` allows for efficient subset checks in $O(n+m)$ time.

- **BloomFilter**: A custom implementation using `BitSet` and MurmurHash3-style mixing, optimized for low memory footprint on Raspberry Pi clusters.
- **Serialization**: Uses `long[]` for `BloomFilter` bitsets to ensure reliable Jackson/CBOR serialization across the cluster.

---

## Completed Work & Features

- [x] **Distributed Akka Cluster**: Support for multi-process and multi-machine execution.
- [x] **Decentralized Data Providers**: Scalable data management across the cluster.
- [x] **Resource-Aware Load Balancing**: Dynamic redistribution of data based on memory pressure.
- [x] **Advanced Pruning**: Multi-layered pruning including Transitivity and Bloom filters.
- [x] **LHS Data Caching**: `DependencyWorker` caches the most recently used LHS column to speed up batch validations.
- [x] **Large Message Support**: Seamless transfer of large datasets via proxy pattern.
- [x] **Custom Bloom Filter**: Memory-optimized implementation with adjustable false-positive rates.

## Summary of Performance Optimizations

| Optimization | Impact | Description |
| :--- | :--- | :--- |
| **Sorted Subset Check** | High | Reduces comparison complexity from quadratic to linear. |
| **Transitivity** | High | Eliminates a large portion of validation tasks as the "known" IND set grows. |
| **Bloom Filters** | Medium-High | Prevents full column data transfer for many negative cases. |
| **Memory Monitoring** | High | Prevents JVM crashes on memory-constrained nodes (e.g., Pi cluster). |
| **LHS Caching** | Medium | Speeds up validation when multiple RHS attributes are checked against the same LHS. |

---
