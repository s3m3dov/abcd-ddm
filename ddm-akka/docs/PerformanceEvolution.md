# Performance Evolution: Optimization Journey (On Modern Laptop)

This document tracks the performance improvements achieved during the development of the `ddm-akka` project. It showcases how various pruning strategies and architectural optimizations reduced the discovery time for Inclusion Dependencies (INDs) by over **13x**.

## Performance Milestone Summary

| Optimization Phase | Execution Time (ms) | Speedup (Cumulative) | Key Improvements |
| :--- | :--- | :--- | :--- |
| **Naive Single** | 146,981 | 1.00x | Initial baseline with full validation. |
| **Bidirectional Transitivity** | 100,517 | 1.46x | Pruning using bidirectional IND knowledge. |
| **Column Statistics** | 45,411 | 3.24x | Cardinality and Range-based pruning. |
| **LHS Caching** | 39,182 | 3.75x | Local caching of LHS data in workers. |
| **Sorted Binary Search** | 26,166 | 5.62x | Optimized internal validation logic. |
| **Bloom Filter Integration** | **11,163** | **13.17x** | Probabilistic pruning of RHS data. |

---

## Detailed Optimization Breakdown

### 1. Fundamental Pruning (Statistics & Transitivity)

The first major leap came from moving beyond a "check everything" approach.

- **Bidirectional Logic**: Validating `A ⊆ B` and `B ⊆ A` within the same context allowed for immediate transitivity updates in both directions.
- **Transitivity Pruning**: Leveraging the mathematical property that if $A \subseteq B$ and $B \subseteq C$, then $A \subseteq C$ is guaranteed. By propagating these discovered dependencies as "hints," the system avoids scheduling redundant validation tasks.
- **Column Statistics**: By comparing `min`, `max`, and `cardinality` of columns before fetching the actual data, the system reached a **~80% pruning rate**, avoiding expensive network transfers for obviously incompatible pairs.

### 2. Local Efficiency (Caching & Search)

Once the number of validation tasks was reduced, the focus shifted to the efficiency of the remaining checks.

- **LHS Caching**: Since many tasks share the same Left-Hand Side (LHS) attribute, caching it locally in the `DependencyWorker` reduced redundant data requests to the `DataProvider`.
- **Sorted Binary Search**: Moving from a naive subset check to a specialized binary search on sorted data significantly reduced the CPU time required for the final validation step.

### 3. The Game Changer: Bloom Filters

The integration of Bloom Filters provided the largest single performance jump in the later stages (reducing mining time from ~26s to ~11s).

- **How it works**: Before fetching a large Right-Hand Side (RHS) column, the worker requests a compact Bloom Filter.
- **Impact**: It allows the worker to prune the RHS direction with high probability if even a single element of the LHS is definitely missing from the RHS, without ever transferring the full column.
- **Result**: Data fetch reduction by nearly **40% for RHS columns**, leading to the final **11.1s** record.

---

## Statistical Deep Dive (Final State)

Based on the latest runs with all optimizations enabled:

- **Total Tasks**: 1540
- **Total Directions**: 3080
- **Pruning Rate**: **89.51%** (2757 of 3080 directions pruned before validation)
- **Top Pruning Methods**:
  - **Cardinality-based**: 1480 directions
  - **Range-based**: 1073 directions
  - **Bloom Filter-based**: 185 directions
  - **Transitivity-based**: 19 directions

This multi-layered approach ensures that the system scales efficiently by minimizing both communication overhead and computational complexity.
