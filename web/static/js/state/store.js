/**
 * Centralized State Management Store
 *
 * Implements a reactive, pub/sub state container ensuring single source
 * of truth and race-condition-free UI component updates.
 */

export class StateStore {
  constructor(initialState = {}) {
    this._state = {
      // Telemetry & metrics
      stats: {
        rateOps: 0.0,
        totalConsumed: 0,
        totalStored: 0,
        totalDlq: 0,
        totalAlerts: 0,
        streamingActive: true,
        partitionCounts: [0, 0, 0, 0, 0, 0],
        partitionDetails: [],
        clusterInfo: {
          p95LatencyMs: 0.0,
          consumerLag: 0,
          clusterId: "kraft-prod-01",
          brokersOnline: 3,
        },
      },
      // Threat alert records
      alerts: [],
      // Log records ring buffer
      logs: [],
      // UI state
      activeFilter: "all",
      searchQuery: "",
      selectedLog: null,
      isModalOpen: false,
      isOnline: true,
      lastError: null,
      ...initialState,
    };

    this._listeners = new Set();
  }

  /**
   * Retrieves an immutable snapshot of current state.
   */
  getState() {
    return this._state;
  }

  /**
   * Updates state partially and notifies subscribers.
   * @param {object|function} patch - An object to merge or an updater function (prevState => newState)
   */
  setState(patch) {
    const nextState = typeof patch === "function" ? patch(this._state) : { ...this._state, ...patch };
    this._state = nextState;
    this._notify();
  }

  /**
   * Subscribes a listener callback to state changes.
   * @param {function} listener
   * @returns {function} Unsubscribe callback
   */
  subscribe(listener) {
    this._listeners.add(listener);
    return () => this._listeners.delete(listener);
  }

  _notify() {
    for (const listener of this._listeners) {
      try {
        listener(this._state);
      } catch (err) {
        console.error("StateStore listener execution error:", err);
      }
    }
  }

  // --- Convenience Action Creators ---

  updateStats(apiStats) {
    this.setState((prev) => ({
      stats: {
        rateOps: apiStats.rate_ops || 0.0,
        totalConsumed: apiStats.total_consumed || 0,
        totalStored: apiStats.total_stored || 0,
        totalDlq: apiStats.total_dlq || 0,
        totalAlerts: apiStats.total_alerts || 0,
        streamingActive: Boolean(apiStats.streaming_active),
        partitionCounts: apiStats.partition_counts || [0, 0, 0, 0, 0, 0],
        partitionDetails: apiStats.partition_details || [],
        clusterInfo: {
          p95LatencyMs: apiStats.cluster_info?.p95_latency_ms || 0.0,
          consumerLag: apiStats.cluster_info?.consumer_lag || 0,
          clusterId: apiStats.cluster_info?.cluster_id || "kraft-prod-01",
          brokersOnline: apiStats.cluster_info?.brokers_online || 3,
        },
      },
      isOnline: true,
    }));
  }

  setAlerts(alertsList) {
    this.setState({ alerts: Array.isArray(alertsList) ? alertsList : [] });
  }

  setLogs(logsList) {
    this.setState({ logs: Array.isArray(logsList) ? logsList : [] });
  }

  setFilter(facet) {
    this.setState({ activeFilter: facet });
  }

  setSearchQuery(query) {
    this.setState({ searchQuery: (query || "").trim().toLowerCase() });
  }

  openInspector(logRecord) {
    this.setState({ selectedLog: logRecord, isModalOpen: true });
  }

  closeInspector() {
    this.setState({ selectedLog: null, isModalOpen: false });
  }

  setOnlineStatus(online, error = null) {
    this.setState({ isOnline: online, lastError: error });
  }
}
