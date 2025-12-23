# Project Documentation: ddm-akka

This document provides a comprehensive overview of the `ddm-akka` project, covering its architecture, message flows, core logic, and key features.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Application Flow & Messages](#application-flow--messages)
    - [Phase 1: Initialization & Registration](#phase-1-initialization--registration)
    - [Phase 2: Loading Phase](#phase-2-loading-phase)
    - [Phase 3: Transition Phase](#phase-3-transition-phase)
    - [Phase 4: Validation Phase](#phase-4-validation-phase)
3. [Important Logic & Decisions](#important-logic--decisions)
4. [Completed Work & Features](#completed-work--features)
5. [Performance Optimizations](#performance-optimizations)

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

### Phase 1: Initialization & Registration

In this phase, the cluster nodes start up and workers register themselves with the master to join the discovery pool.

```mermaid
graph LR
    Receptionist["Akka Receptionist"]
    Miner["DependencyMiner"]
    Worker["Worker Role Node"]

    Worker -- "1. Subscribe" --> Receptionist
    Receptionist -- "2. Provide Miner Ref" --> Worker
    Worker -- "3. Register (Worker + DP)" --> Miner
    Miner -- "4. Assign IDs" --> Worker
```

### Phase 2: Loading Phase

Data is read from CSV files and distributed across the available `DataProvider` actors to distribute memory load.

```mermaid
sequenceDiagram
    participant IR as InputReader
    participant DM as DependencyMiner
    participant DP1 as DataProvider 1 (Local)
    participant DP2 as DataProvider 2 (Remote)

    IR->>DM: HeaderMessage (Schema)
    Note over DM: Map Attributes to DPs
    IR->>DM: BatchMessage (Data Rows)
    DM->>DP1: DataMessage (Attributes 0, 2, 4)
    DM->>DP2: DataMessage (Attributes 1, 3, 5)
    Note over DM, DP2: Rebalancing triggers if any DP is Overloaded
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

