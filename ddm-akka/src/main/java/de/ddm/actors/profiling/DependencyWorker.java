package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.utils.LoggingInterceptor;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import de.ddm.structures.ColumnStatistics;
import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class SetWorkerIdMessage implements Message {
		private static final long serialVersionUID = -3456789012345678901L;
		int workerId;
	}

	/**
	 * Task assignment from DependencyMiner
	 */
	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ValidateTaskMessage implements Message {
		private static final long serialVersionUID = -2765506199949499328L;
		int lhsAttributeId;
		int rhsAttributeId;
		ActorRef<DataProvider.Message> rhsDataProvider;
	}

	/**
	 * Column data received from DataProvider (adapted from
	 * DataProvider.ResponseColumnMessage)
	 */
	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ColumnDataMessage implements Message {
		private static final long serialVersionUID = -1234567890123456782L;
		int attributeId;
		Set<String> columnData;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ColumnStatisticsMessage implements Message {
		private static final long serialVersionUID = 846284628462L;
		ColumnStatistics statistics;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create(ActorRef<DataProvider.Message> dataProvider) {
		return LoggingInterceptor.wrap(Behaviors.setup(context -> new DependencyWorker(context, dataProvider)));
	}

	private DependencyWorker(ActorContext<Message> context, ActorRef<DataProvider.Message> dataProvider) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class,
				ReceptionistListingMessage::new);
		context.getSystem().receptionist()
				.tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(
				LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false),
				LargeMessageProxy.DEFAULT_NAME);

		// Store DataProvider reference passed from Worker
		this.localDataProvider = dataProvider;

		context.getLog().info("DependencyWorker initialized with DataProvider: {}", dataProvider.path());
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private final ActorRef<DataProvider.Message> localDataProvider; // Local DataProvider on this node

	private ActorRef<DependencyMiner.Message> dependencyMiner;
	private int dependencyWorkerId = -1;

	// Current task state
	private ValidateTaskMessage currentTask = null;
	private Set<String> lhsData = null;
	private Set<String> rhsData = null;

	private ColumnStatistics lhsStatistics = null;
	private ColumnStatistics rhsStatistics = null;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(SetWorkerIdMessage.class, this::handle)
				.onMessage(ValidateTaskMessage.class, this::handle)
				.onMessage(ColumnStatisticsMessage.class, this::handle)
				.onMessage(ColumnDataMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {

		if (this.dependencyMiner != null)
			return this;

		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing()
				.getServiceInstances(DependencyMiner.dependencyMinerService);
		if (!dependencyMiners.isEmpty() && (this.dependencyMiner == null)) {
			this.dependencyMiner = dependencyMiners.iterator().next();
			this.dependencyMiner
					.tell(new DependencyMiner.DependencyWorkerRegistrationMessage(
							this.getContext().getSelf(),
							this.largeMessageProxy,
							this.localDataProvider)); // Include DataProvider reference
		}
		return this;
	}

	private Behavior<Message> handle(SetWorkerIdMessage message) {
		this.dependencyWorkerId = message.getWorkerId();
		this.getContext().getLog().info("DependencyWorker ID set to {}", this.dependencyWorkerId);
		return this;
	}

	private Behavior<Message> handle(ValidateTaskMessage message) {
		this.getContext().getLog().info("DependencyWorker-{} received validation task: {} ⊆ {}",
				this.dependencyWorkerId, message.getLhsAttributeId(), message.getRhsAttributeId());

		// Store current task
		this.currentTask = message;
		this.lhsData = null;
		this.rhsData = null;
		this.lhsStatistics = null;
		this.rhsStatistics = null;

		// Request statistics first
		this.localDataProvider.tell(new DataProvider.RequestColumnStatisticsMessage(
				message.getLhsAttributeId(), this.getContext().getSelf()));
		message.getRhsDataProvider().tell(new DataProvider.RequestColumnStatisticsMessage(
				message.getRhsAttributeId(), this.getContext().getSelf()));

		return this;
	}

	private Behavior<Message> handle(ColumnStatisticsMessage message) {
		if (this.currentTask == null)
			return this;

		if (message.getStatistics().getAttributeId() == this.currentTask.getLhsAttributeId()) {
			this.lhsStatistics = message.getStatistics();
		} else if (message.getStatistics().getAttributeId() == this.currentTask.getRhsAttributeId()) {
			this.rhsStatistics = message.getStatistics();
		}

		if (this.lhsStatistics != null && this.rhsStatistics != null) {
			this.checkStatisticsAndDecide();
		}

		return this;
	}

	private void checkStatisticsAndDecide() {
		Boolean lhsSubsetRhs = checkSubset(this.lhsStatistics, this.rhsStatistics);
		Boolean rhsSubsetLhs = checkSubset(this.rhsStatistics, this.lhsStatistics);

		// If BOTH are determined, report findings and skip data fetch
		if (lhsSubsetRhs != null && rhsSubsetLhs != null) {
			this.getContext().getLog().debug(
					"DependencyWorker-{} Pruning success: {} -> {}, {} -> {}",
					this.dependencyWorkerId,
					this.currentTask.getLhsAttributeId(), lhsSubsetRhs,
					this.currentTask.getRhsAttributeId(), rhsSubsetLhs);

			List<Integer> lhsList = new ArrayList<>();
			List<Integer> rhsList = new ArrayList<>();

			if (lhsSubsetRhs) {
				lhsList.add(this.currentTask.getLhsAttributeId());
				rhsList.add(this.currentTask.getRhsAttributeId());
			}

			if (rhsSubsetLhs) {
				lhsList.add(this.currentTask.getRhsAttributeId());
				rhsList.add(this.currentTask.getLhsAttributeId());
			}

			this.dependencyMiner.tell(new DependencyMiner.ResultMessage(
					lhsList.stream().mapToInt(Integer::intValue).toArray(),
					rhsList.stream().mapToInt(Integer::intValue).toArray(),
					this.dependencyWorkerId,
					true));

			// Complete task
			this.currentTask = null;
			this.lhsData = null;
			this.rhsData = null;
			this.lhsStatistics = null;
			this.rhsStatistics = null;

		} else {
			// Fallback to full data validation
			this.getContext().getLog().debug(
					"DependencyWorker-{} Pruning inconclusive for {} and {}, fetching data",
					this.dependencyWorkerId,
					this.currentTask.getLhsAttributeId(),
					this.currentTask.getRhsAttributeId());

			this.localDataProvider.tell(new DataProvider.RequestColumnMessage(
					this.currentTask.getLhsAttributeId(), this.getContext().getSelf(), this.largeMessageProxy));
			this.currentTask.getRhsDataProvider().tell(new DataProvider.RequestColumnMessage(
					this.currentTask.getRhsAttributeId(), this.getContext().getSelf(), this.largeMessageProxy));
		}
	}

	private Boolean checkSubset(ColumnStatistics A, ColumnStatistics B) {
		// 1. Cardinality Pruning
		// Effective cardinality of A excludes nulls because "null is subset of every
		// attr"
		int effCardinalityA = A.getCardinality() - (A.isHasNull() ? 1 : 0);
		// Cardinality of B must at least cover the non-null distinct values of A
		if (effCardinalityA > B.getCardinality()) {
			return false;
		}

		// 2. Min/Max Pruning
		if (A.getMinValue() != null) {
			// A has values
			if (B.getMinValue() == null) {
				// B is empty/nulls only, but A has values -> False
				return false;
			}
			// Both have values, check ranges
			if (A.getMinValue().compareTo(B.getMinValue()) < 0)
				return false;

			if (A.getMaxValue() != null && B.getMaxValue() != null) {
				if (A.getMaxValue().compareTo(B.getMaxValue()) > 0)
					return false;
			}
		}

		// Cannot prune
		return null;
	}

	private Behavior<Message> handle(ColumnDataMessage message) {
		// Store fetched column data
		if (message.getAttributeId() == this.currentTask.getLhsAttributeId()) {
			this.lhsData = message.getColumnData();
			this.getContext().getLog().debug("DependencyWorker-{} received LHS data for attribute {}",
					this.dependencyWorkerId, message.getAttributeId());
		} else if (message.getAttributeId() == this.currentTask.getRhsAttributeId()) {
			this.rhsData = message.getColumnData();
			this.getContext().getLog().debug("DependencyWorker-{} received RHS data for attribute {}",
					this.dependencyWorkerId, message.getAttributeId());
		}

		// When both ready, execute validation
		if (this.lhsData != null && this.rhsData != null) {
			this.executeValidation();
		}

		return this;
	}

	/////////////////
	// Actor Utils //
	/////////////////

	private void executeValidation() {
		// Check both directions since we have both datasets loaded
		boolean lhsSubsetRhs = this.rhsData.containsAll(this.lhsData); // lhs ⊆ rhs
		boolean rhsSubsetLhs = this.lhsData.containsAll(this.rhsData); // rhs ⊆ lhs

		// Build result arrays for discovered INDs
		List<Integer> lhsList = new ArrayList<>();
		List<Integer> rhsList = new ArrayList<>();

		if (lhsSubsetRhs) {
			lhsList.add(this.currentTask.getLhsAttributeId());
			rhsList.add(this.currentTask.getRhsAttributeId());
		}

		if (rhsSubsetLhs) {
			lhsList.add(this.currentTask.getRhsAttributeId());
			rhsList.add(this.currentTask.getLhsAttributeId());
		}

		// Debug logging
		this.getContext().getLog().debug(
				"DependencyWorker-{} bidirectional validation: {} ⊆ {}? {}, {} ⊆ {}? {} (LHS size: {}, RHS size: {})",
				this.dependencyWorkerId,
				this.currentTask.getLhsAttributeId(), this.currentTask.getRhsAttributeId(), lhsSubsetRhs,
				this.currentTask.getRhsAttributeId(), this.currentTask.getLhsAttributeId(), rhsSubsetLhs,
				this.lhsData.size(), this.rhsData.size());

		// Send results to DependencyMiner (may contain 0, 1, or 2 INDs)
		this.dependencyMiner.tell(new DependencyMiner.ResultMessage(
				lhsList.stream().mapToInt(Integer::intValue).toArray(),
				rhsList.stream().mapToInt(Integer::intValue).toArray(),
				this.dependencyWorkerId,
				false));

		// Clear current task (ready for next assignment)
		this.currentTask = null;
		this.lhsData = null;
		this.rhsData = null;
		this.lhsStatistics = null;
		this.rhsStatistics = null;
	}

}
