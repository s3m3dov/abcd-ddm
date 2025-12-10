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

    // Maps attribute ID to its column data
    private final Int2ObjectMap<Set<String>> columnStore;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StoreColumnMessage.class, this::handle)
                .onMessage(RequestColumnMessage.class, this::handle)
                .onMessage(RegisterAttributesMessage.class, this::handle)
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

}
