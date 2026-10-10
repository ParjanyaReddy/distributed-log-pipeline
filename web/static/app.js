document.addEventListener("DOMContentLoaded", () => {
  // Elements
  const statRate = document.getElementById("stat-rate");
  const statConsumed = document.getElementById("stat-consumed");
  const statAlerts = document.getElementById("stat-alerts");
  const statStored = document.getElementById("stat-stored");
  const statDlq = document.getElementById("stat-dlq");
  
  const liveIndicator = document.getElementById("live-indicator");
  const streamStatusText = document.getElementById("stream-status-text");
  const btnToggleTraffic = document.getElementById("btn-toggle-traffic");
  
  const alertFeed = document.getElementById("alert-feed");
  const alertCounter = document.getElementById("alert-counter");
  const terminalLogs = document.getElementById("terminal-logs");
  const logSearch = document.getElementById("log-search");
  
  const attackButtons = document.querySelectorAll(".btn-attack");
  
  let currentLogs = [];
  let filterQuery = "";
  let isStreaming = true;

  // Format numbers with commas
  function formatNum(num) {
    return (num || 0).toLocaleString();
  }

  // Format bytes
  function formatBytes(bytes) {
    if (!bytes || bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
  }

  // Format ISO timestamp to local time string
  function formatTime(isoStr) {
    try {
      const d = new Date(isoStr);
      return d.toTimeString().split(" ")[0];
    } catch {
      return isoStr || "--:--:--";
    }
  }

  // Fetch Stats
  async function fetchStats() {
    try {
      const res = await fetch("/api/stats");
      if (!res.ok) return;
      const data = await res.json();

      statRate.textContent = (data.rate_ops || 0).toFixed(1);
      statConsumed.textContent = formatNum(data.total_consumed);
      statAlerts.textContent = formatNum(data.total_alerts);
      statStored.textContent = formatNum(data.total_stored);
      statDlq.textContent = formatNum(data.total_dlq);

      isStreaming = data.streaming_active;
      if (isStreaming) {
        liveIndicator.style.backgroundColor = "var(--green-ok)";
        liveIndicator.style.boxShadow = "0 0 8px var(--green-ok)";
        streamStatusText.textContent = "STREAM LIVE";
        btnToggleTraffic.textContent = "Pause Stream";
      } else {
        liveIndicator.style.backgroundColor = "var(--amber-warn)";
        liveIndicator.style.boxShadow = "0 0 8px var(--amber-warn)";
        streamStatusText.textContent = "STREAM PAUSED";
        btnToggleTraffic.textContent = "Resume Stream";
      }

      if (data.partition_counts && Array.isArray(data.partition_counts)) {
        data.partition_counts.forEach((count, idx) => {
          const el = document.getElementById(`part-${idx}`);
          if (el) el.textContent = formatNum(count);
        });
      }
    } catch (e) {
      console.debug("Failed fetching stats", e);
    }
  }

  // Fetch Alerts
  async function fetchAlerts() {
    try {
      const res = await fetch("/api/alerts");
      if (!res.ok) return;
      const alerts = await res.json();

      alertCounter.textContent = `${alerts.length} ALERTS`;

      if (alerts.length === 0) {
        alertFeed.innerHTML = `
          <div style="color: var(--text-dim); text-align: center; padding: 40px 10px; font-size: 0.85rem;">
            No anomalies detected yet. Stream is clean or trigger an attack simulation above.
          </div>
        `;
        return;
      }

      let html = "";
      alerts.forEach(alert => {
        const sevClass = (alert.severity || "high").toLowerCase() === "critical" ? "critical" : "high";
        const sevLabel = (alert.severity || "high").toUpperCase();
        const typeName = (alert.anomaly_type || alert.type || "ANOMALY").replace(/_/g, " ").toUpperCase();
        const client = alert.client_id || "unknown";
        const time = formatTime(alert.ts);
        
        let details = "";
        if (alert.details) {
          if (typeof alert.details === "object") {
            details = JSON.stringify(alert.details);
          } else {
            details = alert.details;
          }
        }

        html += `
          <div class="alert-card ${sevClass}">
            <div class="alert-header">
              <div class="alert-type-badge">
                <span>⚠️ ${typeName}</span>
                <span class="badge-sev ${sevClass}">${sevLabel}</span>
              </div>
              <span class="alert-time">${time}</span>
            </div>
            <div class="alert-details">
              Client <span class="alert-client">${client}</span> &mdash; ${details}
            </div>
          </div>
        `;
      });

      alertFeed.innerHTML = html;
    } catch (e) {
      console.debug("Failed fetching alerts", e);
    }
  }

  // Render logs
  function renderLogs() {
    let filtered = currentLogs;
    if (filterQuery) {
      filtered = currentLogs.filter(log => {
        const text = `${log.client_id} ${log.ip_address} ${log.path} ${log.category} ${log.status_code}`.toLowerCase();
        return text.includes(filterQuery);
      });
    }

    if (filtered.length === 0) {
      terminalLogs.innerHTML = `
        <div style="color: var(--text-dim); padding: 20px; text-align: center;">
          ${filterQuery ? "No logs matching current filter query." : "Waiting for incoming log events..."}
        </div>
      `;
      return;
    }

    let html = "";
    filtered.forEach(log => {
      const time = formatTime(log.ts);
      const isOk = log.status_code >= 200 && log.status_code < 400;
      const statusClass = isOk ? "ok" : "err";
      const sizeStr = formatBytes(log.response_bytes || log.bytes_out || 0);

      html += `
        <div class="log-entry">
          <span class="log-ts">${time}</span>
          <span class="log-client">${log.client_id || "sys"}</span>
          <span class="log-status ${statusClass}">${log.status_code || 200}</span>
          <span class="log-url" title="${log.path || '/'}">${log.path || "/"}</span>
          <span class="log-cat">${log.category || "api"}</span>
          <span class="log-bytes">${sizeStr}</span>
        </div>
      `;
    });

    terminalLogs.innerHTML = html;
  }

  // Fetch Logs
  async function fetchLogs() {
    try {
      const res = await fetch("/api/logs");
      if (!res.ok) return;
      currentLogs = await res.json();
      renderLogs();
    } catch (e) {
      console.debug("Failed fetching logs", e);
    }
  }

  // Attack trigger buttons
  attackButtons.forEach(btn => {
    btn.addEventListener("click", async () => {
      const attackType = btn.getAttribute("data-attack");
      btn.style.opacity = "0.6";
      btn.style.pointerEvents = "none";

      try {
        const res = await fetch("/api/simulate/attack", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ type: attackType })
        });
        const data = await res.json();
        console.log("Attack injected:", data);

        // Immediate refresh to catch the event fast
        await fetchAlerts();
        await fetchLogs();
        await fetchStats();
      } catch (err) {
        console.error("Attack injection error:", err);
      } finally {
        setTimeout(() => {
          btn.style.opacity = "1";
          btn.style.pointerEvents = "auto";
        }, 800);
      }
    });
  });

  // Toggle Stream
  btnToggleTraffic.addEventListener("click", async () => {
    try {
      await fetch("/api/traffic/toggle", { method: "POST" });
      await fetchStats();
    } catch (e) {
      console.error("Error toggling traffic", e);
    }
  });

  // Filter input
  logSearch.addEventListener("input", (e) => {
    filterQuery = e.target.value.trim().toLowerCase();
    renderLogs();
  });

  // Polling loop
  setInterval(fetchStats, 1000);
  setInterval(fetchAlerts, 1200);
  setInterval(fetchLogs, 800);

  // Initial fetch
  fetchStats();
  fetchAlerts();
  fetchLogs();
});
