package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.utils.LoggingInterceptor;
import de.ddm.utils.MemoryUtils;
import it.unimi.dsi.fastutil.ints.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

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
        int receivedColumns;
        int sourceDataProviderId;
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
        ActorRef<ResponseColumnMessage> replyTo;
    }

    /**
     * Response message containing requested column data
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ResponseColumnMessage implements Message {
        private static final long serialVersionUID = -7074598491023113482L;
        int attributeId;
        Set<String> columnData;
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
     * Welcome Message from DependencyMiner to set up the DataProvider with its ID
     * and managed attributes
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SetIdAndAttributesMessage implements Message {
        private static final long serialVersionUID = -9234567890123456L;
        int dataProviderId;
        int[] attributes;
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

        // dataProviderId will be set during SetIdAndAttributesMessage
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

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(SetIdAndAttributesMessage.class, this::handle)
                .onMessage(DataMessage.class, this::handle)
                .onMessage(MoveDataMessage.class, this::handle)
                .onMessage(AcceptDataMessage.class, this::handle)
                .onMessage(RequestColumnMessage.class, this::handle)
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

    private Behavior<Message> handle(SetIdAndAttributesMessage message) {
        // Initialize columns for the attributes we'll manage
        this.dataProviderId = message.getDataProviderId();
        for (int attribute : message.getAttributes()) {
            this.columns.put(attribute, new HashSet<>());
        }
        this.getContext().getLog().info("DataProvider-{} registered {} attributes, reporting status...",
                message.getAttributes().length, this.dataProviderId);
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

        // Extract local columns that should be shared
        Map<Integer, Set<String>> stolenColumns = new HashMap<>();
        for (int attribute : message.getAttributes()) {
            Set<String> stolenColumn = this.columns.remove(attribute);
            stolenColumns.put(attribute, stolenColumn);
            this.valueCount = this.valueCount - stolenColumn.size();
        }

        // Update memory usage
        this.updateMemoryUsage();

        // Send columns to target worker
        LargeMessageProxy.LargeMessage acceptDataMessage = new AcceptDataMessage(stolenColumns,
                message.getExpectedMoveCount(), message.getToDataProviderId());
        this.largeMessageProxy
                .tell(new LargeMessageProxy.SendMessage(acceptDataMessage, message.getToDataProviderLMP()));

        this.reportStatus();
        return this;
    }

    private Behavior<Message> handle(AcceptDataMessage message) {

        // Initialize this worker's ID
        this.dataProviderId = message.getSourceDataProviderId();

        // Accept the columns
        for (Map.Entry<Integer, Set<String>> entry : message.getColumns().entrySet()) {
            this.columns.put(entry.getKey().intValue(), entry.getValue());
            this.valueCount = this.valueCount - entry.getValue().size();
        }

        // Update memory usage
        this.updateMemoryUsage();

        // Update acceptMessageCounter and test if this was the last message to be
        // accepted, i.e., the stealing is done
        this.receivedBatchCount++;
        if (this.receivedBatchCount == message.getReceivedColumns()) {
            this.receivedBatchCount = 0;

            this.reportStatus();
        }
        return this;
    }

    private Behavior<Message> handle(RequestColumnMessage message) {

        Set<String> columnData = this.sortedColumns != null
                ? new HashSet<>(this.sortedColumns.get(message.getAttributeId())) // Validation phase - return sorted
                : this.columns.get(message.getAttributeId()); // Loading phase - return unsorted

        if (columnData == null) {
            this.getContext().getLog().warn("Column data not found for attribute {}",
                    message.getAttributeId());
            columnData = Collections.emptySet();
        }

        message.getReplyTo().tell(new ResponseColumnMessage(message.getAttributeId(), columnData));

        return this;
    }

    /////////////////
    // Actor Utils //
    /////////////////

    /**
     * Transition from loading phase to validation phase by sorting columns
     */
    public void transitionToValidationPhase() {
        if (this.sortedColumns != null)
            return; // Already transitioned

        this.getContext().getLog().info("DataProvider transitioning to validation phase");
        this.sortColumns();
    }

    private void sortColumns() {
        this.sortedColumns = new Int2ObjectArrayMap<>(this.columns.size());
        IntList attributes = new IntArrayList(this.columns.keySet());
        for (int attribute : attributes) {
            Set<String> column = this.columns.remove(attribute);
            List<String> sortedColumn = new ArrayList<>(column);
            Collections.sort(sortedColumn);
            this.sortedColumns.put(attribute, sortedColumn);
        }
        this.columns = null; // Free memory from loading phase
    }

    /**
     * Report current memory status to DependencyMiner
     */
    private void reportStatus() {
        // Calculate memory exhaustion
        boolean overloaded = this.availableMemory < this.usedMemoryPredicted;

        // Answer with status
        this.dependencyMiner.tell(new DependencyMiner.StatusMessage(this.dataProviderId, false, overloaded));

        this.getContext().getLog().info("DataProvider-{} Status: overloaded = {} ({} / {})", this.dataProviderId, overloaded,
                this.availableMemory / 1024 / 1024, this.usedMemoryPredicted / 1024 / 1024);
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
