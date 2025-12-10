package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.utils.LoggingInterceptor;
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

    public interface Message extends AkkaSerializable {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataMessage implements Message {
        private static final long serialVersionUID = 8586787707165511461L;
        int attribute;
        String[] column;
        int messagesToAccept;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MoveDataMessage implements Message {
        private static final long serialVersionUID = -1382228729107690967L;
        int[] attributes;
        ActorRef<DependencyWorker.Message> toWorker;
        int toWorkerId;
        ActorRef<LargeMessageProxy.Message> toWorkerLMP;
        int moveMessages;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AcceptDataMessage implements Message {
        private static final long serialVersionUID = -7984837187618402770L;
        Map<Integer, Set<String>> columns;
        int messagesToAccept;
        int workerId;
    }

    /**
     * Message to store column data for a specific attribute
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StoreColumnMessage implements Message {
        private static final long serialVersionUID = -1025000370692033146L;
        int attributeId;
        Set<String> columnData;
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


    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dataProvider";

    public static Behavior<Message> create() {
        return LoggingInterceptor.wrap(Behaviors.setup(DataProvider::new));
    }

    private DataProvider(ActorContext<Message> context) {
        super(context);
        this.columnStore = new Int2ObjectArrayMap<>();
        this.attributeToWorker = new Int2IntArrayMap();
        this.workerToAttributes = new Int2ObjectArrayMap<>();

        context.getLog().info("DataProvider actor initialized");
    }

    /////////////////
    // Actor State //
    /////////////////

    // Memory-related fields
    private final long availableMemory;
    private long usedMemoryMeasured;
    private long usedMemoryPredicted;
	private final static long valueCountWithoutMemoryMeasurement = 100000;

    // Data distribution fields
    private long valueCount;
	private double avgValueSize;
    private int acceptMessageCounter = 0;
    // Maps attribute ID to its column data
    private final Int2ObjectMap<Set<String>> columnStore;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(DataMessage.class, this::handle)
                .onMessage(MoveDataMessage.class, this::handle)
                .onMessage(AcceptDataMessage.class, this::handle)
                .onMessage(StoreColumnMessage.class, this::handle)
                .onMessage(RequestColumnMessage.class, this::handle)
                .onMessage(RegisterAttributesMessage.class, this::handle)
                .build();
    }

    	private Behavior<Message> handle(DataMessage message) {

		// Store data and update value counts
		Set<String> localColumn = this.columns.get(message.getAttribute());
		long columnValueCountBefore = localColumn.size();
		localColumn.addAll(Arrays.asList(message.getColumn()));
		long columnValueCountAfter = localColumn.size();
		this.valueCount = this.valueCount - columnValueCountBefore + columnValueCountAfter;

		// Update memory usage
		this.updateMemoryUsage();

		// Update acceptMessageCounter and test if this was the last message to be
		// accepted, i.e., the batch has arrived
		this.acceptMessageCounter++;
		if (this.acceptMessageCounter == message.getMessagesToAccept()) {
			this.acceptMessageCounter = 0;

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
				message.getMoveMessages(), message.getToWorkerId());
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(acceptDataMessage, message.getToWorkerLMP()));

		this.reportStatus();
		return this;
	}

	private Behavior<Message> handle(AcceptDataMessage message) {

		// Initialize this worker's ID
		this.workerId = message.getWorkerId();

		// Accept the columns
		for (Map.Entry<Integer, Set<String>> entry : message.getColumns().entrySet()) {
			this.columns.put(entry.getKey().intValue(), entry.getValue());
			this.valueCount = this.valueCount - entry.getValue().size();
		}

		// Update memory usage
		this.updateMemoryUsage();

		// Update acceptMessageCounter and test if this was the last message to be
		// accepted, i.e., the stealing is done
		this.acceptMessageCounter++;
		if (this.acceptMessageCounter == message.getMessagesToAccept()) {
			this.acceptMessageCounter = 0;

			this.reportStatus();
		}
		return this;
	}

    private Behavior<Message> handle(StoreColumnMessage message) {

        this.columnStore.put(message.getAttributeId(), message.getColumnData());

        this.getContext().getLog().debug("Stored column data for attribute {} ({} values)",
                message.getAttributeId(), message.getColumnData().size());

        return this;
    }

    private Behavior<Message> handle(RequestColumnMessage message) {

        Set<String> columnData = this.columnStore.get(message.getAttributeId());

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

    private void reportStatus() {
        // Calculate memory exhaustion
        boolean overloaded = this.availableMemory < this.usedMemoryPredicted;

        // Answer with status
        this.dependencyMiner.tell(new DependencyMiner.StatusMessage(this.workerId, false, overloaded));

        this.getContext().getLog().info("Status: overloaded = {} ({} / {})", overloaded,
                this.availableMemory / 1024 / 1024, this.usedMemoryPredicted / 1024 / 1024);
    }


    private void updateMemoryUsage() {
		// Skip any memory measurement if the value count is and always was small
		if ((this.valueCount < valueCountWithoutMemoryMeasurement) && (this.usedMemoryMeasured == 0))
			return;

		// Measure memory consumption if the memory was never measured before
		if (this.usedMemoryMeasured == 0) {
			this.usedMemoryMeasured = MemoryUtils.byteSizeOf(this.columns);
			this.usedMemoryPredicted = this.usedMemoryMeasured;
			this.avgValueSize = (double) this.usedMemoryMeasured / (double) this.valueCount;
			return;
		}

		// Predict the memory consumption
		this.usedMemoryPredicted = (long) Math.ceil(this.valueCount * this.avgValueSize);
		return;
	}
}

