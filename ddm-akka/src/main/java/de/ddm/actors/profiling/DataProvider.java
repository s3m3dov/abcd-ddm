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

    /**
     * Message to register a worker's attributes
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RegisterAttributesMessage implements Message {
        private static final long serialVersionUID = -5023342038453489045L;
        int workerId;
        int[] attributeIds;
    }

    /**
     * Message to query which worker owns a specific attribute
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QueryAttributeOwnerMessage implements Message {
        private static final long serialVersionUID = -1234567890123456786L;
        int attributeId;
        ActorRef<ResponseAttributeOwnerMessage> replyTo;
    }

    /**
     * Response message with attribute owner information
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ResponseAttributeOwnerMessage implements Message {
        private static final long serialVersionUID = -1234567890123456787L;
        int attributeId;
        int workerId;
    }

    /**
     * Message to get statistics about stored data
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GetStatisticsMessage implements Message {
        private static final long serialVersionUID = -1234567890123456788L;
        ActorRef<StatisticsResponseMessage> replyTo;
    }

    /**
     * Response with data statistics
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StatisticsResponseMessage implements Message {
        private static final long serialVersionUID = -1234567890123456789L;
        int totalAttributes;
        long totalValues;
        long memoryUsageBytes;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dataProvider";

    public static final ServiceKey<Message> dataProviderService = ServiceKey.create(Message.class,
            DEFAULT_NAME + "Service");

    public static Behavior<Message> create() {
        return LoggingInterceptor.wrap(Behaviors.setup(DataProvider::new));
    }

    private DataProvider(ActorContext<Message> context) {
        super(context);
        this.columnStore = new Int2ObjectArrayMap<>();
        this.attributeToWorker = new Int2IntArrayMap();
        this.workerToAttributes = new Int2ObjectArrayMap<>();

        // Register with Receptionist for service discovery
        context.getSystem().receptionist().tell(Receptionist.register(dataProviderService, context.getSelf()));

        context.getLog().info("DataProvider actor initialized and registered with Receptionist");
    }

    /////////////////
    // Actor State //
    /////////////////

    // Maps attribute ID to its column data
    private final Int2ObjectMap<Set<String>> columnStore;

    // Maps attribute ID to worker ID that owns it
    private final Int2IntMap attributeToWorker;

    // Maps worker ID to list of attributes it owns
    private final Int2ObjectMap<IntList> workerToAttributes;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StoreColumnMessage.class, this::handle)
                .onMessage(RequestColumnMessage.class, this::handle)
                .onMessage(RegisterAttributesMessage.class, this::handle)
                .onMessage(QueryAttributeOwnerMessage.class, this::handle)
                .onMessage(GetStatisticsMessage.class, this::handle)
                .build();
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

    private Behavior<Message> handle(RegisterAttributesMessage message) {

        int workerId = message.getWorkerId();
        int[] attributeIds = message.getAttributeIds();

        // Register attribute-to-worker mapping
        for (int attributeId : attributeIds) {
            this.attributeToWorker.put(attributeId, workerId);
        }

        // Register worker-to-attributes mapping
        IntList attributes = new IntArrayList(attributeIds);
        this.workerToAttributes.put(workerId, attributes);

        this.getContext().getLog().info("Registered worker {} with {} attributes",
                workerId, attributeIds.length);

        return this;
    }

    private Behavior<Message> handle(QueryAttributeOwnerMessage message) {

        int workerId = this.attributeToWorker.getOrDefault(message.getAttributeId(), -1);

        message.getReplyTo().tell(new ResponseAttributeOwnerMessage(
                message.getAttributeId(), workerId));

        return this;
    }

    private Behavior<Message> handle(GetStatisticsMessage message) {

        int totalAttributes = this.columnStore.size();
        long totalValues = this.columnStore.values().stream()
                .mapToLong(Set::size)
                .sum();

        // Rough memory estimation (this is approximate)
        long memoryUsageBytes = this.columnStore.values().stream()
                .mapToLong(set -> set.stream()
                        .mapToLong(s -> s.length() * 2L + 40) // rough estimate: 2 bytes per char + object overhead
                        .sum())
                .sum();

        message.getReplyTo().tell(new StatisticsResponseMessage(
                totalAttributes, totalValues, memoryUsageBytes));

        this.getContext().getLog().info("Statistics: {} attributes, {} values, ~{} MB",
                totalAttributes, totalValues, memoryUsageBytes / 1024 / 1024);

        return this;
    }
}
