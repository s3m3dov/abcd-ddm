package de.ddm.utils;

import akka.actor.typed.Behavior;
import akka.actor.typed.BehaviorInterceptor;
import akka.actor.typed.Signal;
import akka.actor.typed.TypedActorContext;
import akka.actor.typed.javadsl.Behaviors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingInterceptor {

    private static class LoggingBehaviorInterceptor<M> extends BehaviorInterceptor<M, M> {

        private static final Logger log = LoggerFactory.getLogger(LoggingInterceptor.class);

        @SuppressWarnings("unchecked")
        public LoggingBehaviorInterceptor() {
            super((Class<M>) Object.class);
        }

        @Override
        public Behavior<M> aroundReceive(TypedActorContext<M> ctx, M message, ReceiveTarget<M> target) {
            log.info("{} received {}: {}",
                    ctx.asJava().getSelf().path().name(),
                    message.getClass().getSimpleName(),
                    message);
            return target.apply(ctx, message);
        }

        @Override
        public Behavior<M> aroundSignal(TypedActorContext<M> ctx, Signal signal, SignalTarget<M> target) {
            log.info("{} received signal: {}",
                    ctx.asJava().getSelf().path().name(),
                    signal.getClass().getSimpleName());
            return target.apply(ctx, signal);
        }
    }

    public static <M> Behavior<M> wrap(Behavior<M> behavior) {
        return Behaviors.intercept(() -> new LoggingBehaviorInterceptor<>(), behavior);
    }
}