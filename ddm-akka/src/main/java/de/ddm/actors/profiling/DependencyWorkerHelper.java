package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.serialization.AkkaSerializable;
import it.unimi.dsi.fastutil.ints.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

public class DependencyWorkerHelper extends AbstractBehavior<DependencyWorkerHelper.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ValidateLocalTaskMessage implements Message {
        private static final long serialVersionUID = -1234567890123456780L;
        IntList attributes1;
        IntList attributes2;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ValidateTaskMessage implements Message {
        private static final long serialVersionUID = -1234567890123456781L;
        int attribute;
        List<String> column;
        IntList localAttributes;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyWorkerHelper";

    public static Behavior<Message> create(Int2ObjectMap<List<String>> sortedColumns,
            ActorRef<DependencyWorkerCameo.Message> cameo) {
        return Behaviors.setup(context -> new DependencyWorkerHelper(context, sortedColumns, cameo));
    }

    private DependencyWorkerHelper(ActorContext<Message> context, Int2ObjectMap<List<String>> sortedColumns,
            ActorRef<DependencyWorkerCameo.Message> cameo) {
        super(context);
        this.sortedColumns = sortedColumns;
        this.cameo = cameo;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final Int2ObjectMap<List<String>> sortedColumns;
    private final ActorRef<DependencyWorkerCameo.Message> cameo;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ValidateLocalTaskMessage.class, this::handle)
                .onMessage(ValidateTaskMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ValidateLocalTaskMessage message) {
        IntList lhss = new IntArrayList();
        IntList rhss = new IntArrayList();

        // Validate all local attribute pairs
        for (int i = 0; i < message.getAttributes1().size(); i++) {
            int attr1 = message.getAttributes1().getInt(i);
            int attr2 = message.getAttributes2().getInt(i);

            List<String> column1 = this.sortedColumns.get(attr1);
            List<String> column2 = this.sortedColumns.get(attr2);

            // Check if attr1 ⊆ attr2 (inclusion dependency)
            if (isIncluded(column1, column2)) {
                lhss.add(attr1);
                rhss.add(attr2);
            }

            // Check if attr2 ⊆ attr1 (reverse inclusion dependency)
            if (isIncluded(column2, column1)) {
                lhss.add(attr2);
                rhss.add(attr1);
            }
        }

        // Send results to cameo
        this.cameo.tell(new DependencyWorkerCameo.ResultMessage(lhss.toIntArray(), rhss.toIntArray()));
        return this;
    }

    private Behavior<Message> handle(ValidateTaskMessage message) {
        IntList lhss = new IntArrayList();
        IntList rhss = new IntArrayList();

        List<String> remoteColumn = message.getColumn();

        // Validate remote column against local attributes
        for (int localAttribute : message.getLocalAttributes()) {
            List<String> localColumn = this.sortedColumns.get(localAttribute);

            // Check if localColumn ⊆ remoteColumn
            if (isIncluded(localColumn, remoteColumn)) {
                lhss.add(localAttribute);
                rhss.add(message.getAttribute());
            }

            // Check if remoteColumn ⊆ localColumn
            if (isIncluded(remoteColumn, localColumn)) {
                lhss.add(message.getAttribute());
                rhss.add(localAttribute);
            }
        }

        // Send results to cameo
        this.cameo.tell(new DependencyWorkerCameo.ResultMessage(lhss.toIntArray(), rhss.toIntArray()));
        return this;
    }

    /**
     * Check if all values in column1 are contained in column2 (both sorted).
     * This implements the inclusion dependency check: column1 ⊆ column2
     */
    private boolean isIncluded(List<String> column1, List<String> column2) {
        int i = 0, j = 0;

        while (i < column1.size() && j < column2.size()) {
            int cmp = column1.get(i).compareTo(column2.get(j));

            if (cmp == 0) {
                i++;
                j++;
            } else if (cmp < 0) {
                // Value in column1 not found in column2
                return false;
            } else {
                j++;
            }
        }

        // If we haven't processed all of column1, it's not included
        return i == column1.size();
    }
}
