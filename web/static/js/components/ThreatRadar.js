/**
 * SIEM Threat Radar & Incident Alerts Component
 */

import { formatTimestamp } from "../utils/formatters.js";
import { getMitreDetails } from "../utils/mitre.js";

export class ThreatRadarComponent {
  constructor(store) {
    this.store = store;
    this.dom = {
      badgeAlertCount: document.getElementById("badge-alert-count"),
      alertStream: document.getElementById("alert-stream"),
    };

    this.render(this.store.getState());
    this.store.subscribe((state) => this.render(state));
  }

  render(state) {
    const { alerts } = state;
    if (!this.dom.alertStream) return;

    if (this.dom.badgeAlertCount) {
      this.dom.badgeAlertCount.textContent = `${(alerts || []).length} INCIDENTS`;
    }

    if (!alerts || alerts.length === 0) {
      this.dom.alertStream.innerHTML = `
        <div style="color: var(--text-dim); text-align: center; padding: 48px 16px; font-size: 11px; font-family: var(--font-mono);">
          Awaiting streaming alerts. Baseline traffic is clean.<br>Trigger a MITRE ATT&amp;CK scenario above to evaluate detectors.
        </div>
      `;
      return;
    }

    let html = "";
    alerts.forEach((alert) => {
      const sev = (alert.severity || "high").toLowerCase();
      const isCrit = sev === "critical";
      const sevClass = isCrit ? "sev-crit" : "sev-high";
      const cardClass = isCrit ? "critical" : "";
      const mitre = getMitreDetails(alert.anomaly_type || alert.type);
      const time = formatTimestamp(alert.ts);
      const client = alert.client_id || "cl-unknown";

      let details = "";
      if (alert.details) {
        if (typeof alert.details === "object") {
          details = JSON.stringify(alert.details).replace(/[{}"]/g, " ");
        } else {
          details = String(alert.details);
        }
      }

      html += `
        <div class="alert-card-item ${cardClass}">
          <div class="alert-card-top">
            <div class="alert-badges-group">
              <span class="badge-tag ${sevClass}">${sev.toUpperCase()}</span>
              <span class="badge-tag rule-tag">${mitre.code}</span>
              <span class="badge-tag rule-tag">${mitre.rule}</span>
            </div>
            <span class="alert-timestamp">${time}</span>
          </div>
          <div class="alert-body-desc">
            <strong>${mitre.name}</strong> &mdash; ${details}
          </div>
          <div class="alert-footer-meta">
            <span>Client: <strong>${client}</strong></span>
            <span>Detector: <strong>${alert.anomaly_type || alert.type || "heuristic"}</strong></span>
          </div>
        </div>
      `;
    });

    this.dom.alertStream.innerHTML = html;
  }
}
