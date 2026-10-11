/**
 * Real-Time Log Viewer Table Component
 */

import { formatBytes, formatTimestamp } from "../utils/formatters.js";

export class LogTableComponent {
  constructor(store) {
    this.store = store;
    this.dom = {
      logTableRows: document.getElementById("log-table-rows"),
      logSearchInput: document.getElementById("log-search-input"),
      filterChips: document.querySelectorAll(".filter-chip"),
    };

    this._bindEvents();
    this.render(this.store.getState());
    this.store.subscribe((state) => this.render(state));
  }

  _bindEvents() {
    if (this.dom.logSearchInput) {
      this.dom.logSearchInput.addEventListener("input", (e) => {
        this.store.setSearchQuery(e.target.value);
      });
    }

    if (this.dom.filterChips) {
      this.dom.filterChips.forEach((chip) => {
        chip.addEventListener("click", () => {
          this.dom.filterChips.forEach((c) => c.classList.remove("active"));
          chip.classList.add("active");
          const filter = chip.getAttribute("data-filter") || "all";
          this.store.setFilter(filter);
        });
      });
    }
  }

  _applyFilterAndSearch(logs, activeFilter, searchQuery) {
    let result = logs || [];

    // Filter chip facet
    if (activeFilter === "2xx") {
      result = result.filter((l) => l.status_code >= 200 && l.status_code < 400);
    } else if (activeFilter === "4xx") {
      result = result.filter((l) => l.status_code >= 400 && l.status_code < 500);
    } else if (activeFilter === "5xx") {
      result = result.filter((l) => l.status_code >= 500);
    } else if (activeFilter === "auth") {
      result = result.filter((l) => (l.category || "").toLowerCase() === "authentication");
    } else if (activeFilter === "api") {
      result = result.filter((l) => (l.category || "").toLowerCase() === "api");
    }

    // Search query matching
    if (searchQuery) {
      result = result.filter((l) => {
        const haystack = `${l.client_id || ""} ${l.ip_address || l.src_ip || ""} ${l.path || l.url || ""} ${l.category || ""} ${l.status_code || l.status || ""} ${l.method || ""}`.toLowerCase();
        return haystack.includes(searchQuery);
      });
    }

    return result;
  }

  render(state) {
    if (!this.dom.logTableRows) return;

    const { logs, activeFilter, searchQuery } = state;
    const filtered = this._applyFilterAndSearch(logs, activeFilter, searchQuery);

    if (filtered.length === 0) {
      this.dom.logTableRows.innerHTML = `
        <div style="color: var(--text-dim); text-align: center; padding: 36px; font-size: 11px; font-family: var(--font-mono);">
          ${searchQuery ? "No records matched your search query." : "Waiting for log stream buffer..."}
        </div>
      `;
      return;
    }

    let html = "";
    filtered.forEach((log, idx) => {
      const time = formatTimestamp(log.ts);
      const shard = `P-${log.partition !== undefined ? log.partition : 0}`;
      const status = log.status_code || log.status || 200;
      const isOk = status >= 200 && status < 400;
      const statusClass = isOk ? "ok" : "err";
      const statusText = `${status} ${isOk ? "OK" : "ERR"}`;
      const methodPath = `${log.method || "GET"} ${log.path || log.url || "/"}`;
      const bytes = formatBytes(log.response_bytes || log.bytes_out || 0);
      const client = log.client_id || "system";

      html += `
        <div class="log-row-item" data-idx="${idx}" title="Click to inspect raw JSON record">
          <div class="col-ts">${time}</div>
          <div class="col-shard">${shard}</div>
          <div class="col-status ${statusClass}">${statusText}</div>
          <div class="col-cat">${log.category || "api"}</div>
          <div class="col-path">${methodPath}</div>
          <div class="col-client">${client}</div>
          <div class="col-bytes">${bytes}</div>
        </div>
      `;
    });

    this.dom.logTableRows.innerHTML = html;

    // Attach row inspection listeners
    this.dom.logTableRows.querySelectorAll(".log-row-item").forEach((row) => {
      row.addEventListener("click", () => {
        const idx = parseInt(row.getAttribute("data-idx"), 10);
        const record = filtered[idx];
        if (record) {
          this.store.openInspector(record);
        }
      });
    });
  }
}
