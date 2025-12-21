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
		boolean lhsToRhsTransitive;
		boolean rhsToLhsTransitive;
	}

	/**
	 * Column data received from DataProvider
	 */
	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ColumnDataMessage implements Message {
		private static final long serialVersionUID = -1234567890123456782L;
		int attributeId;
		List<String> columnData;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ColumnStatisticsMessage implements Message {
		private static final long serialVersionUID = 846284628462L;
		ColumnStatistics statistics;
	}

	/**
	 * Internal message for checking LHS ⊆ RHS direction
	 */
	@NoArgsConstructor
	public static class LhsToRhsCheckMessage implements Message {
		private static final long serialVersionUID = -1234567890123456781L;
	}

	/**
	 * Internal message for checking RHS ⊆ LHS direction
	 */
	@NoArgsConstructor
	public static class RhsToLhsCheckMessage implements Message {
		private static final long serialVersionUID = -1234567890123456782L;
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

	// Pruning type enum
	private enum PruningType {
		NONE, // Not pruned
		CARDINALITY, // Pruned by cardinality check
		RANGE, // Pruned by min/max range check
		NULL_ONLY, // Pruned by null-only check
		TRANSITIVITY // Pruned by transitivity (from DependencyMiner)
	}

	// Direction-specific state
	private enum DirectionState {
		PENDING, // Not yet started
		STATISTICS_PENDING, // Waiting for statistics
		PRUNED_TRUE, // Pruned as TRUE by statistics
		PRUNED_FALSE, // Pruned as FALSE by statistics
		DATA_PENDING, // Waiting for data fetch
		VALIDATED // Completed via data validation
	}

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private final ActorRef<DataProvider.Message> localDataProvider; // Local DataProvider on this node

	private ActorRef<DependencyMiner.Message> dependencyMiner;
	private int dependencyWorkerId = -1;

	// Current task state
	private ValidateTaskMessage currentTask = null;
	private List<String> lhsData = null;
	private List<String> rhsData = null;
	private int cachedLhsAttributeId = -1; // Track cached LHS attribute for reuse

	private ColumnStatistics lhsStatistics = null;
	private ColumnStatistics rhsStatistics = null;

	// Direction-specific state tracking
	private Boolean lhsToRhsResult = null; // Result for lhs ⊆ rhs
	private Boolean rhsToLhsResult = null; // Result for rhs ⊆ lhs
	private DirectionState lhsToRhsState = DirectionState.PENDING;
	private DirectionState rhsToLhsState = DirectionState.PENDING;
	private PruningType lhsToRhsPruningType = PruningType.NONE;
	private PruningType rhsToLhsPruningType = PruningType.NONE;

	// Data fetch tracking for this task
	private boolean lhsDataFetched = false;
	private boolean rhsDataFetched = false;

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
				.onMessage(LhsToRhsCheckMessage.class, this::handle)
				.onMessage(RhsToLhsCheckMessage.class, this::handle)
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
		this.getContext().getLog().debug(
				"DependencyWorker-{} received validation task: {} ⊆ {} (transitivity hints: LHS→RHS={}, RHS→LHS={})",
				this.dependencyWorkerId, message.getLhsAttributeId(), message.getRhsAttributeId(),
				message.isLhsToRhsTransitive(), message.isRhsToLhsTransitive());

		// Store current task
		this.currentTask = message;

		// Initialize direction states
		this.lhsToRhsResult = null;
		this.rhsToLhsResult = null;
		this.lhsToRhsState = DirectionState.STATISTICS_PENDING;
		this.rhsToLhsState = DirectionState.STATISTICS_PENDING;
		this.lhsToRhsPruningType = PruningType.NONE;
		this.rhsToLhsPruningType = PruningType.NONE;

		// Reset data fetch tracking
		this.lhsDataFetched = false;
		this.rhsDataFetched = false;

		// Process transitivity hints - mark transitive directions as already proven
		if (message.isLhsToRhsTransitive()) {
			this.lhsToRhsState = DirectionState.PRUNED_TRUE;
			this.lhsToRhsResult = true;
			this.lhsToRhsPruningType = PruningType.TRANSITIVITY;
			this.getContext().getLog().debug("  LHS→RHS: TRANSITIVE (already proven, skipping validation)");
		}

		if (message.isRhsToLhsTransitive()) {
			this.rhsToLhsState = DirectionState.PRUNED_TRUE;
			this.rhsToLhsResult = true;
			this.rhsToLhsPruningType = PruningType.TRANSITIVITY;
			this.getContext().getLog().debug("  RHS→LHS: TRANSITIVE (already proven, skipping validation)");
		}

		// Check if both directions are already transitive (task complete immediately)
		if (message.isLhsToRhsTransitive() && message.isRhsToLhsTransitive()) {
			this.getContext().getLog().debug(
					"DependencyWorker-{} both directions transitive - completing task immediately",
					this.dependencyWorkerId);
			checkTaskCompletion();
			return this;
		}

		// Check if we can reuse cached LHS data from previous task
		boolean canReuseLhsData = (this.lhsData != null &&
				this.cachedLhsAttributeId == message.getLhsAttributeId());
		boolean canReuseLhsStats = (this.lhsStatistics != null &&
				this.lhsStatistics.getAttributeId() == message.getLhsAttributeId());

		if (canReuseLhsData || canReuseLhsStats) {
			this.getContext().getLog().debug(
					"DependencyWorker-{} reusing cached LHS data/stats for attribute {} (data={}, stats={})",
					this.dependencyWorkerId, message.getLhsAttributeId(), canReuseLhsData, canReuseLhsStats);
		}

		// Clear RHS data (always changes between tasks)
		this.rhsData = null;
		this.rhsStatistics = null;

		// Clear LHS data only if attribute changed
		if (!canReuseLhsData) {
			this.lhsData = null;
			this.cachedLhsAttributeId = message.getLhsAttributeId();
		}
		if (!canReuseLhsStats) {
			this.lhsStatistics = null;
		}

		// Request statistics only for directions that need validation (not transitive)
		// LHS statistics: needed if LHS→RHS is not transitive
		if (!message.isLhsToRhsTransitive() && !canReuseLhsStats) {
			this.localDataProvider.tell(new DataProvider.RequestColumnStatisticsMessage(
					message.getLhsAttributeId(), this.getContext().getSelf()));
		}

		// RHS statistics: needed if RHS→LHS is not transitive
		if (!message.isRhsToLhsTransitive()) {
			message.getRhsDataProvider().tell(new DataProvider.RequestColumnStatisticsMessage(
					message.getRhsAttributeId(), this.getContext().getSelf()));
		}

		return this;
	}

	private Behavior<Message> handle(ColumnStatisticsMessage message) {
		if (this.currentTask == null)
			return this;

		ColumnStatistics stats = message.getStatistics();

		if (stats.getAttributeId() == this.currentTask.getLhsAttributeId()) {
			this.lhsStatistics = stats;
			this.getContext().getLog().debug(
					"DependencyWorker-{} received LHS statistics for attribute {}: cardinality={}, hasNull={}, min={}, max={}",
					this.dependencyWorkerId, stats.getAttributeId(), stats.getCardinality(),
					stats.isHasNull(), stats.getMinValue(), stats.getMaxValue());
		} else if (stats.getAttributeId() == this.currentTask.getRhsAttributeId()) {
			this.rhsStatistics = stats;
			this.getContext().getLog().debug(
					"DependencyWorker-{} received RHS statistics for attribute {}: cardinality={}, hasNull={}, min={}, max={}",
					this.dependencyWorkerId, stats.getAttributeId(), stats.getCardinality(),
					stats.isHasNull(), stats.getMinValue(), stats.getMaxValue());
		}

		if (this.lhsStatistics != null && this.rhsStatistics != null) {
			this.checkStatisticsAndDecide();
		}

		return this;
	}

	private void checkStatisticsAndDecide() {
		this.getContext().getLog().debug(
				"DependencyWorker-{} analyzing statistics for task: {} ⊆ {}",
				this.dependencyWorkerId,
				this.currentTask.getLhsAttributeId(),
				this.currentTask.getRhsAttributeId());

		// Check directions with pruning type tracking (skip if already transitive)
		PruningResult lhsToRhsCheck = null;
		PruningResult rhsToLhsCheck = null;

		// Only check statistics for non-transitive directions
		if (this.lhsToRhsState != DirectionState.PRUNED_TRUE) {
			lhsToRhsCheck = checkSubset(this.lhsStatistics, this.rhsStatistics);
		}

		if (this.rhsToLhsState != DirectionState.PRUNED_TRUE) {
			rhsToLhsCheck = checkSubset(this.rhsStatistics, this.lhsStatistics);
		}

		// Update direction states based on statistics results (only for non-transitive)
		if (lhsToRhsCheck != null) {
			if (lhsToRhsCheck.result != null) {
				// Conclusive result for lhs -> rhs
				this.lhsToRhsState = lhsToRhsCheck.result ? DirectionState.PRUNED_TRUE : DirectionState.PRUNED_FALSE;
				this.lhsToRhsResult = lhsToRhsCheck.result;
				this.lhsToRhsPruningType = lhsToRhsCheck.type;
				logDirectionResult(this.currentTask.getLhsAttributeId(), this.currentTask.getRhsAttributeId(),
						lhsToRhsCheck.result, lhsToRhsCheck.type, true);
			} else {
				// Inconclusive - needs data validation
				this.lhsToRhsState = DirectionState.DATA_PENDING;
				logDirectionResult(this.currentTask.getLhsAttributeId(), this.currentTask.getRhsAttributeId(),
						null, PruningType.NONE, true);
			}
		}

		if (rhsToLhsCheck != null) {
			if (rhsToLhsCheck.result != null) {
				// Conclusive result for rhs -> lhs
				this.rhsToLhsState = rhsToLhsCheck.result ? DirectionState.PRUNED_TRUE : DirectionState.PRUNED_FALSE;
				this.rhsToLhsResult = rhsToLhsCheck.result;
				this.rhsToLhsPruningType = rhsToLhsCheck.type;
				logDirectionResult(this.currentTask.getRhsAttributeId(), this.currentTask.getLhsAttributeId(),
						rhsToLhsCheck.result, rhsToLhsCheck.type, true);
			} else {
				// Inconclusive - needs data validation
				this.rhsToLhsState = DirectionState.DATA_PENDING;
				logDirectionResult(this.currentTask.getRhsAttributeId(), this.currentTask.getLhsAttributeId(),
						null, PruningType.NONE, true);
			}
		}

		// Check if both directions are conclusive (no data fetch needed)
		boolean lhsToRhsComplete = (this.lhsToRhsState == DirectionState.PRUNED_TRUE ||
				this.lhsToRhsState == DirectionState.PRUNED_FALSE);
		boolean rhsToLhsComplete = (this.rhsToLhsState == DirectionState.PRUNED_TRUE ||
				this.rhsToLhsState == DirectionState.PRUNED_FALSE);

		if (lhsToRhsComplete && rhsToLhsComplete) {
			this.getContext().getLog().debug(
					"DependencyWorker-{} both directions determined by statistics",
					this.dependencyWorkerId);
			// Both directions are conclusive, complete the task
			checkTaskCompletion();
		} else {
			// At least one direction needs data validation - send messages for pending
			// directions
			if (this.lhsToRhsState == DirectionState.DATA_PENDING) {
				this.getContext().getSelf().tell(new LhsToRhsCheckMessage());
			}

			if (this.rhsToLhsState == DirectionState.DATA_PENDING) {
				this.getContext().getSelf().tell(new RhsToLhsCheckMessage());
			}
		}
	}

	/**
	 * Log the result for a specific direction
	 */
	private void logDirectionResult(int fromAttr, int toAttr, Boolean result, PruningType type,
			boolean fromStatistics) {
		String source = fromStatistics ? "statistics" : "data fetch";
		if (result == null) {
			this.getContext().getLog().info(
					"DependencyWorker-{}: {} ⊆ {} = INCONCLUSIVE (needs data fetch)",
					this.dependencyWorkerId, fromAttr, toAttr);
		} else if (result) {
			this.getContext().getLog().info(
					"DependencyWorker-{}: {} ⊆ {} = TRUE ({}{})",
					this.dependencyWorkerId, fromAttr, toAttr, source,
					type != PruningType.NONE ? ", " + type : "");
		} else {
			this.getContext().getLog().info(
					"DependencyWorker-{}: {} ⊆ {} = FALSE ({}{})",
					this.dependencyWorkerId, fromAttr, toAttr, source,
					type != PruningType.NONE ? ", " + type : "");
		}
	}

	/**
	 * Clear current task state after completion
	 */
	private void clearTaskState() {
		this.currentTask = null;
		// Keep lhsData and lhsStatistics cached for potential reuse
		// Keep cachedLhsAttributeId to track what's cached
		this.rhsData = null;
		this.rhsStatistics = null;
		// Reset direction states
		this.lhsToRhsResult = null;
		this.rhsToLhsResult = null;
		this.lhsToRhsState = DirectionState.PENDING;
		this.rhsToLhsState = DirectionState.PENDING;
		this.lhsToRhsPruningType = PruningType.NONE;
		this.rhsToLhsPruningType = PruningType.NONE;
	}

	private record PruningResult(Boolean result, PruningType type) {
	}

	/**
	 * Check if column A is a subset of column B using statistics-based pruning.
	 * 
	 * @param A Statistics for the potential subset column
	 * @param B Statistics for the potential superset column
	 * @return PruningResult with result (true if A ⊆ B is proven, false if
	 *         disproven, null if inconclusive) and pruning type
	 */
	private PruningResult checkSubset(ColumnStatistics A, ColumnStatistics B) {
		// 0. Null-Only Column Check
		// A column with ONLY nulls/empty strings is NOT a valid IND with any column
		int effCardinalityA = A.getCardinality() - (A.isHasNull() ? 1 : 0);
		if (effCardinalityA == 0) {
			this.getContext().getLog().debug(
					"Null-only check: {} ⊆ {} = FALSE (attr {} is null-only)",
					A.getAttributeId(), B.getAttributeId(), A.getAttributeId());
			return new PruningResult(false, PruningType.NULL_ONLY);
		}

		// 1. Cardinality Pruning
		// Effective cardinality of A excludes nulls because "null is subset of every
		// attr"
		// Cardinality of B must at least cover the non-null distinct values of A
		if (effCardinalityA > B.getCardinality()) {
			this.getContext().getLog().debug(
					"Cardinality check: {} ⊆ {} = FALSE (effCard={}>{} > card={}={})",
					A.getAttributeId(), B.getAttributeId(),
					A.getAttributeId(), effCardinalityA,
					B.getAttributeId(), B.getCardinality());
			return new PruningResult(false, PruningType.CARDINALITY);
		}

		// 2. Min/Max Range Pruning
		if (A.getMinValue() != null) {
			// A has values
			if (B.getMinValue() == null) {
				// B is empty/nulls only, but A has values -> False
				this.getContext().getLog().debug(
						"Range check: {} ⊆ {} = FALSE ({} has values, {} is null-only)",
						A.getAttributeId(), B.getAttributeId(),
						A.getAttributeId(), B.getAttributeId());
				return new PruningResult(false, PruningType.RANGE);
			}

			// Both have values, check ranges
			if (A.getMinValue().compareTo(B.getMinValue()) < 0) {
				this.getContext().getLog().debug(
						"Range check: {} ⊆ {} = FALSE (min={}='{}' < min={}='{}')",
						A.getAttributeId(), B.getAttributeId(),
						A.getAttributeId(), A.getMinValue(),
						B.getAttributeId(), B.getMinValue());
				return new PruningResult(false, PruningType.RANGE);
			}

			if (A.getMaxValue() != null && B.getMaxValue() != null) {
				if (A.getMaxValue().compareTo(B.getMaxValue()) > 0) {
					this.getContext().getLog().debug(
							"Range check: {} ⊆ {} = FALSE (max={}='{}' > max={}='{}')",
							A.getAttributeId(), B.getAttributeId(),
							A.getAttributeId(), A.getMaxValue(),
							B.getAttributeId(), B.getMaxValue());
					return new PruningResult(false, PruningType.RANGE);
				}
			}
		}

		// Cannot prune - statistics are insufficient to determine the relationship
		this.getContext().getLog().debug(
				"No pruning applicable: {} ⊆ {} = INCONCLUSIVE",
				A.getAttributeId(), B.getAttributeId());
		return new PruningResult(null, PruningType.NONE);
	}

	private Behavior<Message> handle(LhsToRhsCheckMessage message) {
		if (this.currentTask == null) {
			this.getContext().getLog().warn("Received LhsToRhsCheckMessage but no current task");
			return this;
		}

		this.getContext().getLog().debug("DependencyWorker-{} handling LHS ⊆ RHS check", this.dependencyWorkerId);

		// Request data if needed
		boolean needLhsData = (this.lhsData == null
				|| this.cachedLhsAttributeId != this.currentTask.getLhsAttributeId());
		boolean needRhsData = (this.rhsData == null);

		if (needLhsData) {
			this.localDataProvider.tell(new DataProvider.RequestColumnMessage(
					this.currentTask.getLhsAttributeId(), this.getContext().getSelf(), this.largeMessageProxy));
			this.lhsDataFetched = true;
		}

		if (needRhsData) {
			this.currentTask.getRhsDataProvider().tell(new DataProvider.RequestColumnMessage(
					this.currentTask.getRhsAttributeId(), this.getContext().getSelf(), this.largeMessageProxy));
			this.rhsDataFetched = true;
		}

		// If we already have the data, validate immediately
		if (!needLhsData && !needRhsData) {
			validateDirection(this.currentTask.getLhsAttributeId(), this.currentTask.getRhsAttributeId());
		}

		return this;
	}

	private Behavior<Message> handle(RhsToLhsCheckMessage message) {
		if (this.currentTask == null) {
			this.getContext().getLog().warn("Received RhsToLhsCheckMessage but no current task");
			return this;
		}

		this.getContext().getLog().debug(
				"DependencyWorker-{} handling RHS ⊆ LHS check",
				this.dependencyWorkerId);

		// Request data if needed
		boolean needRhsData = (this.rhsData == null);
		boolean needLhsData = (this.lhsData == null
				|| this.cachedLhsAttributeId != this.currentTask.getLhsAttributeId());

		if (needRhsData) {
			this.currentTask.getRhsDataProvider().tell(new DataProvider.RequestColumnMessage(
					this.currentTask.getRhsAttributeId(), this.getContext().getSelf(), this.largeMessageProxy));
			this.rhsDataFetched = true;
		}

		if (needLhsData) {
			this.localDataProvider.tell(new DataProvider.RequestColumnMessage(
					this.currentTask.getLhsAttributeId(), this.getContext().getSelf(), this.largeMessageProxy));
			this.lhsDataFetched = true;
		}

		// If we already have the data, validate immediately
		if (!needRhsData && !needLhsData) {
			validateDirection(this.currentTask.getRhsAttributeId(), this.currentTask.getLhsAttributeId());
		}

		return this;
	}

	/**
	 * Validate a specific direction and update its state
	 */
	private void validateDirection(int fromAttr, int toAttr) {
		List<String> fromData = (fromAttr == this.currentTask.getLhsAttributeId()) ? this.lhsData : this.rhsData;
		List<String> toData = (toAttr == this.currentTask.getLhsAttributeId()) ? this.lhsData : this.rhsData;

		if (fromData == null || toData == null) {
			this.getContext().getLog().warn(
					"Cannot validate direction {} ⊆ {} - missing data",
					fromAttr, toAttr);
			return;
		}

		boolean result = isSortedSubset(fromData, toData);

		// Update the appropriate direction state
		if (fromAttr == this.currentTask.getLhsAttributeId() && toAttr == this.currentTask.getRhsAttributeId()) {
			this.lhsToRhsState = DirectionState.VALIDATED;
			this.lhsToRhsResult = result;
			logDirectionResult(fromAttr, toAttr, result, PruningType.NONE, false);
		} else if (fromAttr == this.currentTask.getRhsAttributeId() && toAttr == this.currentTask.getLhsAttributeId()) {
			this.rhsToLhsState = DirectionState.VALIDATED;
			this.rhsToLhsResult = result;
			logDirectionResult(fromAttr, toAttr, result, PruningType.NONE, false);
		}

		// Check if task is complete
		checkTaskCompletion();
	}

	/**
	 * Check if both directions are complete and send results if so
	 */
	private void checkTaskCompletion() {
		// Check if both directions have completed (either pruned or validated)
		boolean lhsToRhsComplete = (this.lhsToRhsState == DirectionState.PRUNED_TRUE ||
				this.lhsToRhsState == DirectionState.PRUNED_FALSE ||
				this.lhsToRhsState == DirectionState.VALIDATED);

		boolean rhsToLhsComplete = (this.rhsToLhsState == DirectionState.PRUNED_TRUE ||
				this.rhsToLhsState == DirectionState.PRUNED_FALSE ||
				this.rhsToLhsState == DirectionState.VALIDATED);

		if (!lhsToRhsComplete || !rhsToLhsComplete) {
			// Not yet complete
			return;
		}

		this.getContext().getLog().debug(
				"DependencyWorker-{} task complete: both directions validated",
				this.dependencyWorkerId);

		// Build result lists for discovered INDs
		List<Integer> lhsList = new ArrayList<>();
		List<Integer> rhsList = new ArrayList<>();

		// Add lhs ⊆ rhs if true
		if (Boolean.TRUE.equals(this.lhsToRhsResult)) {
			lhsList.add(this.currentTask.getLhsAttributeId());
			rhsList.add(this.currentTask.getRhsAttributeId());
		}

		// Add rhs ⊆ lhs if true
		if (Boolean.TRUE.equals(this.rhsToLhsResult)) {
			lhsList.add(this.currentTask.getRhsAttributeId());
			rhsList.add(this.currentTask.getLhsAttributeId());
		}

		// Send results to DependencyMiner with pruning type & data fetch counts
		this.dependencyMiner.tell(new DependencyMiner.ResultMessage(
				lhsList.stream().mapToInt(Integer::intValue).toArray(),
				rhsList.stream().mapToInt(Integer::intValue).toArray(),
				this.dependencyWorkerId,
				this.lhsToRhsPruningType.name(),
				this.rhsToLhsPruningType.name(),
				this.lhsDataFetched,
				this.rhsDataFetched));

		// Clear task state (ready for next assignment)
		clearTaskState();
	}

	private Behavior<Message> handle(ColumnDataMessage message) {
		if (this.currentTask == null) {
			return this;
		}

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

		// Check which directions can now be validated
		// Validate lhs -> rhs if it's pending data and we have both datasets
		if (this.lhsToRhsState == DirectionState.DATA_PENDING &&
				this.lhsData != null && this.rhsData != null) {
			validateDirection(this.currentTask.getLhsAttributeId(), this.currentTask.getRhsAttributeId());
		}

		// Validate rhs -> lhs if it's pending data and we have both datasets
		if (this.rhsToLhsState == DirectionState.DATA_PENDING &&
				this.rhsData != null && this.lhsData != null) {
			validateDirection(this.currentTask.getRhsAttributeId(), this.currentTask.getLhsAttributeId());
		}

		return this;
	}

	/////////////////
	// Actor Utils //
	/////////////////

	/**
	 * Check if subset is a subset of superset using binary search.
	 * Both lists must be sorted in ascending order.
	 */
	private boolean isSortedSubset(List<String> subset, List<String> superset) {
		// Empty set is subset of any set
		if (subset.isEmpty()) {
			return true;
		}

		// Non-empty set cannot be subset of empty set
		if (superset.isEmpty()) {
			return false;
		}

		// Check each element in subset using binary search
		for (String element : subset) {
			if (Collections.binarySearch(superset, element) < 0) {
				// Element not found in superset
				return false;
			}
		}

		return true;
	}

}
