/**
 * Telemetry and KPI Metrics Component
 *
 * Subscribes to the central store and updates telemetry cards and the topology ribbon.
 */

import { formatNumber } from "../utils/formatters.js";

export class KpiMetricsComponent {
  constructor(store) {
    this.store = store;
    this.dom = {
      kpiRate: document.getElementById("kpi-rate"),
      kpiLatency: document.getElementById("kpi-latency"),
      kpiLag: document.getElementById("kpi-lag"),
      kpiStored: document.getElementById("kpi-stored"),
      kpiDlq: document.getElementById("kpi-dlq"),
      flowProdRate: document.getElementById("flow-prod-rate"),
      flowAlertCount: document.getElementById("flow-alert-count"),
      statusPulse: document.getElementById("status-pulse"),
      pipelineStatusText: document.getElementById("pipeline-status-text"),
      btnTrafficLabel: document.getElementById("btn-traffic-label"),
    };

    this.render(this.store.getState());
    this.store.subscribe((state) => this.render(state));
  }

  render(state) {
    const { stats, isOnline } = state;
    if (!stats) return;

    // Rate
    if (this.dom.kpiRate) {
      this.dom.kpiRate.textContent = (stats.rateOps || 0.0).toFixed(1);
    }
    if (this.dom.flowProdRate) {
      this.dom.flowProdRate.textContent = `~${(stats.rateOps || 0).toFixed(0)} ev/s`;
    }

    // Storage and DLQ
    if (this.dom.kpiStored) {
      this.dom.kpiStored.textContent = formatNumber(stats.totalStored);
    }
    if (this.dom.kpiDlq) {
      this.dom.kpiDlq.textContent = formatNumber(stats.totalDlq);
    }

    // Alerts count badge in flow banner
    if (this.dom.flowAlertCount) {
      this.dom.flowAlertCount.textContent = `${stats.totalAlerts || 0} Alerts`;
    }

    // Latency and Lag
    if (this.dom.kpiLatency && stats.clusterInfo) {
      this.dom.kpiLatency.textContent = (stats.clusterInfo.p95LatencyMs || 0.0).toFixed(1);
    }
    if (this.dom.kpiLag && stats.clusterInfo) {
      this.dom.kpiLag.textContent = formatNumber(stats.clusterInfo.consumerLag);
    }

    // Pulse and Stream Status
    if (this.dom.statusPulse && this.dom.pipelineStatusText && this.dom.btnTrafficLabel) {
      if (!isOnline) {
        this.dom.statusPulse.style.backgroundColor = "var(--rose-critical)";
        this.dom.statusPulse.style.boxShadow = "0 0 8px var(--rose-critical)";
        this.dom.pipelineStatusText.textContent = "OFFLINE (RETRYING)";
        this.dom.btnTrafficLabel.textContent = "Stream Disconnected";
      } else if (stats.streamingActive) {
        this.dom.statusPulse.style.backgroundColor = "var(--emerald-success)";
        this.dom.statusPulse.style.boxShadow = "0 0 8px var(--emerald-success)";
        this.dom.pipelineStatusText.textContent = "STREAMING LIVE";
        this.dom.btnTrafficLabel.textContent = "Pause Stream";
      } else {
        this.dom.statusPulse.style.backgroundColor = "var(--amber-warning)";
        this.dom.statusPulse.style.boxShadow = "0 0 8px var(--amber-warning)";
        this.dom.pipelineStatusText.textContent = "STREAM PAUSED";
        this.dom.btnTrafficLabel.textContent = "Resume Stream";
      }
    }
  }
}
