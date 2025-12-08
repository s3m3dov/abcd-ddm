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

import java.util.List;

public class DependencyWorkerCameo extends AbstractBehavior<DependencyWorkerCameo.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ResultMessage implements Message {
        private static final long serialVersionUID = -1234567890123456789L;
        int[] lhss;
        int[] rhss;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyWorkerCameo";

    public static Behavior<Message> create(ActorRef<DependencyMiner.Message> dependencyMiner, int numHelpers,
            int workerId) {
        return Behaviors.setup(context -> new DependencyWorkerCameo(context, dependencyMiner, numHelpers, workerId));
    }

    private DependencyWorkerCameo(ActorContext<Message> context, ActorRef<DependencyMiner.Message> dependencyMiner,
            int numHelpers, int workerId) {
        super(context);
        this.dependencyMiner = dependencyMiner;
        this.numHelpers = numHelpers;
        this.workerId = workerId;
        this.receivedResults = 0;
        this.lhss = new IntArrayList();
        this.rhss = new IntArrayList();
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef<DependencyMiner.Message> dependencyMiner;
    private final int numHelpers;
    private final int workerId;
    private int receivedResults;
    private final IntList lhss;
    private final IntList rhss;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ResultMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ResultMessage message) {
        // Accumulate results from helpers
        for (int lhs : message.getLhss())
            this.lhss.add(lhs);
        for (int rhs : message.getRhss())
            this.rhss.add(rhs);

        this.receivedResults++;

        // When all helpers have reported, send combined results to miner
        if (this.receivedResults == this.numHelpers) {
            this.dependencyMiner.tell(new DependencyMiner.ResultMessage(
                    this.lhss.toIntArray(),
                    this.rhss.toIntArray(),
                    this.workerId));

            // Reset for next round
            this.lhss.clear();
            this.rhss.clear();
            this.receivedResults = 0;
        }

        return this;
    }
}
