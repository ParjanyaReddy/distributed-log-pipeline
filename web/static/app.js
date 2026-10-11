/**
 * Apex Stream - SOC Console Application Entrypoint
 *
 * Coordinates state store, API client, modular UI components,
 * and background telemetry synchronization loops.
 */

import { StateStore } from "./js/state/store.js";
import { ApiClient } from "./js/services/apiClient.js";
import { ToastNotifier } from "./js/components/Toast.js";
import { KpiMetricsComponent } from "./js/components/KpiMetrics.js";
import { PartitionMatrixComponent } from "./js/components/PartitionMatrix.js";
import { ThreatRadarComponent } from "./js/components/ThreatRadar.js";
import { LogTableComponent } from "./js/components/LogTable.js";
import { InspectorModalComponent } from "./js/components/InspectorModal.js";
import { AttackPanelComponent } from "./js/components/AttackPanel.js";

document.addEventListener("DOMContentLoaded", () => {
  // 1. Initialize Core Services and State Store
  const store = new StateStore();
  const apiClient = new ApiClient();
  const toast = new ToastNotifier();

  // 2. Instantiate Reusable UI Components
  const kpiComponent = new KpiMetricsComponent(store);
  const matrixComponent = new PartitionMatrixComponent(store);
  const threatRadarComponent = new ThreatRadarComponent(store);
  const logTableComponent = new LogTableComponent(store);
  const inspectorModalComponent = new InspectorModalComponent(store);
  const attackPanelComponent = new AttackPanelComponent(store, apiClient, toast);

  // Expose for browser debugging and automated E2E tests
  window.__APEX_STORE__ = store;
  window.__APEX_API__ = apiClient;
  window.__APEX_TOAST__ = toast;

  // 3. Telemetry Fetch Routines with Graceful Offline Recovery
  let wasOffline = false;

  async function pollStats() {
    try {
      const stats = await apiClient.fetchStats();
      store.updateStats(stats);
      if (wasOffline) {
        toast.success("Reconnected to pipeline backend.");
        wasOffline = false;
      }
    } catch (err) {
      if (!wasOffline) {
        toast.error(`Stats telemetry offline: ${err.message}`);
        wasOffline = true;
      }
      store.setOnlineStatus(false, err.message);
    }
  }

  async function pollAlerts() {
    try {
      const alerts = await apiClient.fetchAlerts();
      store.setAlerts(alerts);
    } catch (err) {
      console.debug("Alerts fetch error:", err);
    }
  }

  async function pollLogs() {
    try {
      const logs = await apiClient.fetchLogs();
      store.setLogs(logs);
    } catch (err) {
      console.debug("Logs fetch error:", err);
    }
  }

  // 4. Initial Synchronization
  pollStats();
  pollAlerts();
  pollLogs();

  // 5. Polling Intervals
  setInterval(pollStats, 1000);
  setInterval(pollAlerts, 1200);
  setInterval(pollLogs, 800);
});
