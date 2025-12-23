package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.structures.BloomFilter;
import de.ddm.utils.LoggingInterceptor;
import de.ddm.utils.MemoryUtils;
import it.unimi.dsi.fastutil.ints.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import de.ddm.structures.ColumnStatistics;
import java.util.*;

/**
 * DataProvider actor manages column data distribution across the cluster.
 * It provides a centralized data access point for DependencyWorkers to query
 * column data without needing to store all data locally.
 */
public class DataProvider extends AbstractBehavior<DataProvider.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends LargeMessageProxy.LargeMessage {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataMessage implements Message {
        private static final long serialVersionUID = 8586787707165511461L;
        int attributeId;
        String[] columnBatch;
        int expectedBatchCount;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MoveDataMessage implements Message {
        private static final long serialVersionUID = -1382228729107690967L;
        int[] attributes;
        int toDataProviderId;
        ActorRef<DataProvider.Message> toDataProvider;
        ActorRef<LargeMessageProxy.Message> toDataProviderLMP;
        int expectedMoveCount;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AcceptDataMessage implements Message {
        private static final long serialVersionUID = -7984837187618402770L;
        Map<Integer, Set<String>> columns;
        int totalExpectedMoves;
        int fromDataProviderId;
        double avgValueSizeInBytes; // Memory metric from source DataProvider
    }

    /**
     * Message to request column data for a specific attribute
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RequestColumnMessage implements Message {
        private static final long serialVersionUID = -3571780705578600223L;
        int attributeId;
        ActorRef<DependencyWorker.Message> replyTo;
        ActorRef<LargeMessageProxy.Message> replyToLMP;
    }

    /**
     * Message from Receptionist with DependencyMiner service listing
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReceptionistListingMessage implements Message {
        private static final long serialVersionUID = -8234567890123456789L;
        Receptionist.Listing listing;
    }

    /**
     * Message to set the DataProvider's ID
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SetDataProviderIdMessage implements Message {
        private static final long serialVersionUID = -8765432109876543210L;
        int dataProviderId;
    }

    /**
     * Message to inform DataProvider about its initial attribute responsibilities
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InitialAttributesMessage implements Message {
        private static final long serialVersionUID = -1234567890987654321L;
        int[] attributes;
    }

    /**
     * Message to trigger transition from loading phase to validation phase
     * This will sort columns and free memory from loading phase structures
     */
    @Getter
    @NoArgsConstructor
    public static class TransitionToValidationPhaseMessage implements Message {
        private static final long serialVersionUID = -9876543210987654L;
    }

    /**
     * Message to request column statistics for a specific attribute
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RequestColumnStatisticsMessage implements Message {
        private static final long serialVersionUID = 556677889900112233L;
        int attributeId;
        ActorRef<DependencyWorker.Message> replyTo;
    }

    /**
     * Message to request Bloom filter for a specific attribute
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RequestBloomFilterMessage implements Message {
        private static final long serialVersionUID = 778899001122334455L;
        int attributeId;
        ActorRef<DependencyWorker.Message> replyTo;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dataProvider";

    public static Behavior<Message> create() {
        return LoggingInterceptor.wrap(Behaviors.setup(DataProvider::new));
    }

    private DataProvider(ActorContext<Message> context) {
        super(context);
        this.columns = new Int2ObjectArrayMap<>();

        // Initialize memory tracking - use 70-80% of available memory
        this.availableMemory = (long) (this.MEMORY_USAGE_THRESHOLD
                * (Math.max(MemoryUtils.bytesMax(), MemoryUtils.bytesCommitted()) - MemoryUtils.bytesUsed()));
        this.usedMemoryMeasured = 0;
        this.usedMemoryPredicted = 0;
        this.valueCount = 0;
        this.avgValueSizeInBytes = 0.0;

        // Initialize LargeMessageProxy for handling large messages
        this.largeMessageProxy = context.spawn(
                LargeMessageProxy.create(context.getSelf().unsafeUpcast(), false),
                LargeMessageProxy.DEFAULT_NAME);

        // dataProviderId will be set via SetDataProviderIdMessage
        this.dataProviderId = -1;

        // dependencyMiner will be discovered via Receptionist
        this.dependencyMiner = null;

        // Subscribe to DependencyMiner service
        final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(
                Receptionist.Listing.class, ReceptionistListingMessage::new);
        context.getSystem().receptionist()
                .tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

        context.getLog().info("DataProvider actor initialized with {}MB available memory",
                this.availableMemory / 1024 / 1024);
    }

    /////////////////
    // Actor State //
    /////////////////

    // Memory-related fields
    private final float MEMORY_USAGE_THRESHOLD = 0.75f;
    private final static long VALUE_COUNT_BEFORE_MEMORY_MEASUREMENT = 100000;
    private final long availableMemory;
    private long usedMemoryMeasured;
    private long usedMemoryPredicted;

    // Data distribution fields
    private long valueCount; // Total unique value count across all managed columns
    private double avgValueSizeInBytes;
    private int receivedBatchCount = 0;

    // Maps attribute ID to its column data (loading phase)
    private Int2ObjectMap<Set<String>> columns;
    // Maps attribute ID to sorted column data (validation phase)
    private Int2ObjectMap<List<String>> sortedColumns;

    // Actor references
    private ActorRef<LargeMessageProxy.Message> largeMessageProxy;
    private ActorRef<DependencyMiner.Message> dependencyMiner;
    private int dataProviderId;

    // Statistics for pruning
    private Map<Integer, ColumnStatistics> columnStatistics = new HashMap<>();

    // Bloom filters for pruning
    private Map<Integer, de.ddm.structures.BloomFilter> bloomFilters = new HashMap<>();

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(SetDataProviderIdMessage.class, this::handle)
                .onMessage(InitialAttributesMessage.class, this::handle)
                .onMessage(DataMessage.class, this::handle)
                .onMessage(MoveDataMessage.class, this::handle)
                .onMessage(AcceptDataMessage.class, this::handle)
                .onMessage(RequestColumnMessage.class, this::handle)
                .onMessage(TransitionToValidationPhaseMessage.class, this::handle)
                .onMessage(RequestColumnStatisticsMessage.class, this::handle)
                .onMessage(RequestBloomFilterMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ReceptionistListingMessage message) {
        Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing()
                .getServiceInstances(DependencyMiner.dependencyMinerService);

        // Register with DependencyMiner when discovered
        if (!dependencyMiners.isEmpty() && (this.dependencyMiner == null)) {
            this.dependencyMiner = dependencyMiners.iterator().next();

            // Send registration message to DependencyMiner
            this.dependencyMiner.tell(new DependencyMiner.DataProviderRegistrationMessage(
                    this.getContext().getSelf(),
                    this.largeMessageProxy));

            this.getContext().getLog().info("Registered with DependencyMiner");
        }

        return this;
    }

    private Behavior<Message> handle(SetDataProviderIdMessage message) {
        this.dataProviderId = message.getDataProviderId();
        this.getContext().getLog().info("DataProvider ID set to {}", this.dataProviderId);
        return this;
    }

    private Behavior<Message> handle(InitialAttributesMessage message) {
        // Initialize columns for the attributes we'll manage
        for (int attribute : message.getAttributes()) {
            this.columns.put(attribute, new HashSet<>());
        }
        this.getContext().getLog().info("DataProvider-{} has been assigned {} attributes",
                this.dataProviderId, message.getAttributes().length);
        this.reportStatus();
        return this;
    }

    private Behavior<Message> handle(DataMessage message) {

        // Initialize column if it doesn't exist (lazy initialization)
        if (!this.columns.containsKey(message.getAttributeId())) {
            this.columns.put(message.getAttributeId(), new HashSet<>());
        }

        // Store data and update value counts
        Set<String> localColumn = this.columns.get(message.getAttributeId());
        long columnValueCountBefore = localColumn.size();
        localColumn.addAll(Arrays.asList(message.getColumnBatch()));
        long columnValueCountAfter = localColumn.size();
        this.valueCount = this.valueCount - columnValueCountBefore + columnValueCountAfter;

        // Update memory usage
        this.updateMemoryUsage();

        // Update acceptMessageCounter and test if this was the last message to be
        // accepted, i.e., the batch has arrived
        this.receivedBatchCount++;
        if (this.receivedBatchCount == message.getExpectedBatchCount()) {
            this.receivedBatchCount = 0;

            this.reportStatus();
        }
        return this;
    }

    private Behavior<Message> handle(MoveDataMessage message) {
        this.getContext().getLog().info(
                "DataProvider-{} moving {} columns TO DataProvider-{}",
                this.dataProviderId,
                message.getAttributes().length,
                message.getToDataProviderId());

        // Step 1: Extract the columns that need to be moved
        Map<Integer, Set<String>> columnsToMove = this.extractColumnsForMove(message.getAttributes());

        // Step 2: Update our memory usage after removing columns
        this.updateMemoryUsage();

        // Step 3: Send the extracted columns to the destination DataProvider
        this.sendColumnsToDestination(columnsToMove, message.getExpectedMoveCount(),
                message.getToDataProviderId(), message.getToDataProviderLMP());

        // Step 4: Report our new status to DependencyMiner
        this.reportStatus();

        return this;
    }

    private Behavior<Message> handle(AcceptDataMessage message) {
        this.getContext().getLog().info(
                "DataProvider-{} accepting {} columns from DataProvider-{} (total {} values)",
                this.dataProviderId,
                message.getColumns().size(),
                message.getFromDataProviderId(),
                message.getColumns().values().stream().mapToInt(Set::size).sum());

        // Log details of each column being accepted
        for (Map.Entry<Integer, Set<String>> entry : message.getColumns().entrySet()) {
            this.getContext().getLog().debug(
                    "  Attribute {}: {} values",
                    entry.getKey(), entry.getValue().size());
        }

        // Add the received columns to our local storage
        this.acceptIncomingColumns(message.getColumns());

        // Inherit memory metrics from source DataProvider to avoid expensive
        // measurement
        if (message.getAvgValueSizeInBytes() > 0) {
            // If we haven't measured yet, inherit the source's metric
            if (this.usedMemoryMeasured == 0) {
                this.avgValueSizeInBytes = message.getAvgValueSizeInBytes();
                this.getContext().getLog().info(
                        "DataProvider-{} inherited avgValueSizeInBytes={} bytes from DataProvider-{}",
                        this.dataProviderId, this.avgValueSizeInBytes, message.getFromDataProviderId());
            }
            // Update predicted memory based on new value count
            this.usedMemoryPredicted = (long) Math.ceil(this.valueCount * this.avgValueSizeInBytes);
        } else {
            // Fallback: measure if source didn't provide metrics (shouldn't happen
            // normally)
            this.updateMemoryUsage();
        }

        // Track progress and report when all moves are complete
        this.receivedBatchCount++;
        this.getContext().getLog().info(
                "DataProvider-{} received {}/{} expected moves",
                this.dataProviderId, this.receivedBatchCount, message.getTotalExpectedMoves());

        if (this.receivedBatchCount == message.getTotalExpectedMoves()) {
            this.receivedBatchCount = 0;
            this.reportStatus();
        }

        return this;
    }

    private Behavior<Message> handle(RequestColumnMessage message) {
        List<String> columnData = this.sortedColumns != null
                ? new ArrayList<>(this.sortedColumns.get(message.getAttributeId())) // Validation phase - sorted
                : new ArrayList<>(this.columns.get(message.getAttributeId())); // Loading phase - unsorted

        // Send ColumnDataMessage via LargeMessageProxy to handle large payloads
        DependencyWorker.ColumnDataMessage response = new DependencyWorker.ColumnDataMessage(
                message.getAttributeId(), columnData);
        this.largeMessageProxy.tell(
                new LargeMessageProxy.SendMessage(response, message.getReplyToLMP()));

        return this;
    }

    private Behavior<Message> handle(TransitionToValidationPhaseMessage message) {
        this.getContext().getLog().info("DataProvider-{} received transition to validation phase message",
                this.dataProviderId);
        this.logDataProviderInfo();
        this.transitionToValidationPhase();
        return this;
    }

    private Behavior<Message> handle(RequestColumnStatisticsMessage message) {
        ColumnStatistics stats = this.columnStatistics.get(message.getAttributeId());
        if (stats == null) {
            this.getContext().getLog().warn("Statistics requested for unknown attribute {}", message.getAttributeId());
            return this;
        }

        message.getReplyTo().tell(new DependencyWorker.ColumnStatisticsMessage(stats));
        return this;
    }

    private Behavior<Message> handle(RequestBloomFilterMessage message) {
        de.ddm.structures.BloomFilter bloomFilter = this.bloomFilters.get(message.getAttributeId());
        if (bloomFilter == null) {
            this.getContext().getLog().warn("Bloom filter requested for unknown attribute {}",
                    message.getAttributeId());
            return this;
        }

        this.getContext().getLog().debug("DataProvider-{} serving Bloom filter for attribute {} (size: {} bytes)",
                this.dataProviderId, message.getAttributeId(), bloomFilter.getSizeInBytes());

        message.getReplyTo().tell(new DependencyWorker.BloomFilterResponseMessage(
                message.getAttributeId(), bloomFilter));
        return this;
    }

    /////////////////
    // Actor Utils //
    /////////////////

    /**
     * Extract columns from local storage for moving to another DataProvider.
     * Removes the columns from this DataProvider and updates value count.
     * 
     * @param attributes Array of attribute IDs to extract
     * @return Map of attribute ID to column data (Set of values)
     */
    private Map<Integer, Set<String>> extractColumnsForMove(int[] attributes) {
        Map<Integer, Set<String>> columnsToMove = new HashMap<>();

        for (int attribute : attributes) {
            Set<String> column = this.columns.remove(attribute);

            if (column != null) {
                // Column exists - move it and update value count
                columnsToMove.put(attribute, column);
                this.valueCount -= column.size();
            } else {
                // Column doesn't exist yet (data hasn't been loaded)
                // Send empty set as placeholder
                columnsToMove.put(attribute, new HashSet<>());
            }
        }

        return columnsToMove;
    }

    /**
     * Send extracted columns to the destination DataProvider.
     * 
     * @param columnsToMove     Map of columns to send
     * @param expectedMoveCount Total number of move operations expected
     * @param toDataProviderId  ID of the destination DataProvider
     * @param toDataProviderLMP LargeMessageProxy of the destination DataProvider
     */
    private void sendColumnsToDestination(
            Map<Integer, Set<String>> columnsToMove,
            int expectedMoveCount,
            int toDataProviderId,
            ActorRef<LargeMessageProxy.Message> toDataProviderLMP) {

        AcceptDataMessage acceptMessage = new AcceptDataMessage(
                columnsToMove,
                expectedMoveCount,
                this.dataProviderId, // We are the "from" DataProvider
                this.avgValueSizeInBytes // Pass our memory metric
        );

        this.largeMessageProxy.tell(
                new LargeMessageProxy.SendMessage(acceptMessage, toDataProviderLMP));
    }

    /**
     * Accept incoming columns from another DataProvider during rebalancing.
     * Adds the columns to local storage and updates value count.
     * 
     * @param incomingColumns Map of attribute ID to column data
     */
    private void acceptIncomingColumns(Map<Integer, Set<String>> incomingColumns) {
        for (Map.Entry<Integer, Set<String>> entry : incomingColumns.entrySet()) {
            int attributeId = entry.getKey();
            Set<String> columnData = entry.getValue();

            // Add the column to our storage
            this.columns.put(attributeId, columnData);

            // Update our total value count
            this.valueCount += columnData.size();
        }
    }

    /**
     * Transition from loading phase to validation phase by sorting columns
     */
    public void transitionToValidationPhase() {
        if (this.sortedColumns != null)
            return; // Already transitioned

        this.getContext().getLog().info("DataProvider-{} transitioning to validation phase - sorting {} columns",
                this.dataProviderId, this.columns != null ? this.columns.size() : 0);
        this.sortColumns();
    }

    private void sortColumns() {
        this.sortedColumns = new Int2ObjectArrayMap<>(this.columns.size());
        IntList attributes = new IntArrayList(this.columns.keySet());

        // Process each column atomically: remove, sort, build stats & BloomFilter
        for (int attribute : attributes) {
            // Remove column immediately to avoid race conditions
            Set<String> column = this.columns.remove(attribute);

            // Sort the column first
            List<String> sortedColumn = new ArrayList<>(column);
            Collections.sort(sortedColumn);

            // Build statistics from sorted column
            int cardinality = sortedColumn.size();
            boolean hasNull = sortedColumn.contains(null) || sortedColumn.contains("");

            String minValue = null;
            String maxValue = null;

            try {
                // Filter nulls for min/max computation
                List<String> validValues = new ArrayList<>();
                for (String s : sortedColumn) {
                    if (s != null && !s.isEmpty()) {
                        validValues.add(s);
                    }
                }

                if (!validValues.isEmpty()) {
                    minValue = validValues.get(0); // First element (min) from sorted list
                    maxValue = validValues.get(validValues.size() - 1); // Last element (max) from sorted list
                }
            } catch (Exception e) {
                // Gracefully handle if comparison fails
                this.getContext().getLog().error(
                        "DataProvider-{} error computing min/max for attribute {}: {}",
                        this.dataProviderId, attribute, e.getMessage());
            }

            this.columnStatistics.put(attribute, new ColumnStatistics(
                    attribute, cardinality, hasNull, minValue, maxValue));

            // Build Bloom filter from sorted column (1% false positive rate)
            BloomFilter bloomFilter = new BloomFilter(cardinality);
            for (String value : sortedColumn) {
                bloomFilter.add(value);
            }
            this.bloomFilters.put(attribute, bloomFilter);

            this.getContext().getLog().debug(
                    "DataProvider-{} built Bloom filter for attribute {}: {} elements, {} bytes, {} hash functions",
                    this.dataProviderId, attribute, cardinality,
                    bloomFilter.getSizeInBytes(), bloomFilter.getNumHashFunctions());

            // Store sorted column
            this.sortedColumns.put(attribute, sortedColumn);
        }

        this.columns = null; // Free memory from loading phase

        long totalBloomFilterSize = this.bloomFilters.values().stream()
                .mapToLong(de.ddm.structures.BloomFilter::getSizeInBytes)
                .sum();
        this.getContext().getLog().info(
                "DataProvider-{} completed transition: {} Bloom filters built, total size: {} KB",
                this.dataProviderId, this.bloomFilters.size(), totalBloomFilterSize / 1024);
    }

    /**
     * Log comprehensive information about this DataProvider's state
     */
    private void logDataProviderInfo() {
        var log = this.getContext().getLog();
        String prefix = "DataProvider-" + this.dataProviderId;

        int columnCount = this.columns == null ? 0 : this.columns.size();
        long usedMb = this.usedMemoryPredicted / 1024 / 1024;
        long availMb = this.availableMemory / 1024 / 1024;
        double memPercent = this.availableMemory == 0
                ? 0
                : (this.usedMemoryPredicted * 100.0) / this.availableMemory;

        log.info(
                "{} | Memory Usage: {}% ({}MB of {}MB) | Columns: {}, Unique values: {} | Avg value size: {} bytes",
                prefix,
                Math.round(memPercent * 10) / 10.0,
                usedMb,
                availMb,
                columnCount,
                this.valueCount,
                Math.round(this.avgValueSizeInBytes * 100) / 100.0);

        if (log.isDebugEnabled() && this.columns != null && !this.columns.isEmpty()) {
            String attrs = this.columns.int2ObjectEntrySet().stream()
                    .map(e -> e.getIntKey() + "=" + e.getValue().size())
                    .collect(java.util.stream.Collectors.joining(", "));
            log.debug("{} | Attribute unique values: [{}]", prefix, attrs);
        }
    }

    /**
     * Report current memory status to DependencyMiner
     */
    private void reportStatus() {
        // Calculate memory exhaustion
        boolean overloaded = this.usedMemoryPredicted >= this.availableMemory;

        // Answer with status
        this.dependencyMiner.tell(new DependencyMiner.StatusMessage(this.dataProviderId, false, overloaded));

        this.getContext().getLog().info("DataProvider-{} | Status: overloaded = {} ({} of {})", this.dataProviderId,
                overloaded,
                this.usedMemoryPredicted / 1024 / 1024,
                this.availableMemory / 1024 / 1024);
    }

    /**
     * Update memory usage measurements and predictions
     */
    private void updateMemoryUsage() {
        // Skip any memory measurement if the value count is and always was small
        if ((this.valueCount < VALUE_COUNT_BEFORE_MEMORY_MEASUREMENT) && (this.usedMemoryMeasured == 0))
            return;

        // Measure memory consumption if the memory was never measured before
        if (this.usedMemoryMeasured == 0) {
            this.usedMemoryMeasured = MemoryUtils.byteSizeOf(this.columns);
            this.usedMemoryPredicted = this.usedMemoryMeasured;
            this.avgValueSizeInBytes = (double) this.usedMemoryMeasured / (double) this.valueCount;
            return;
        }

        // Predict the memory consumption
        this.usedMemoryPredicted = (long) Math.ceil(this.valueCount * this.avgValueSizeInBytes);
    }
}
