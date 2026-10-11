/**
 * Centralized API Client Service
 *
 * Provides typed, resilient HTTP fetching with timeout handling,
 * error boundaries, and unified response unmarshaling.
 */

export class ApiClient {
  constructor(baseUrl = "") {
    this.baseUrl = baseUrl;
    this.timeoutMs = 6000;
  }

  async _request(endpoint, options = {}) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await fetch(`${this.baseUrl}${endpoint}`, {
        ...options,
        signal: controller.signal,
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          ...(options.headers || {}),
        },
      });

      clearTimeout(timer);

      if (!response.ok) {
        let errorDetails = `HTTP error ${response.status}`;
        try {
          const errJson = await response.json();
          errorDetails = errJson?.error?.message || errJson?.message || errorDetails;
        } catch {
          // Response body was not JSON
        }
        throw new Error(errorDetails);
      }

      return await response.json();
    } catch (err) {
      clearTimeout(timer);
      if (err.name === "AbortError") {
        throw new Error(`Request to ${endpoint} timed out after ${this.timeoutMs}ms`);
      }
      throw err;
    }
  }

  /**
   * Fetches live cluster and throughput statistics.
   */
  async fetchStats() {
    return this._request("/api/v1/stats");
  }

  /**
   * Fetches latest anomaly detection alerts.
   * @param {number} [limit=30]
   */
  async fetchAlerts(limit = 30) {
    return this._request(`/api/v1/alerts?limit=${limit}`);
  }

  /**
   * Fetches latest log events ring buffer.
   * @param {number} [limit=50]
   */
  async fetchLogs(limit = 50) {
    return this._request(`/api/v1/logs?limit=${limit}`);
  }

  /**
   * Triggers an adversary attack simulation.
   * @param {string} attackType - "brute_force" | "volume_spike" | "rare_domain" | "beaconing"
   */
  async triggerAttack(attackType) {
    return this._request("/api/v1/simulate/attack", {
      method: "POST",
      body: JSON.stringify({ type: attackType }),
    });
  }

  /**
   * Toggles streaming traffic on or off.
   */
  async toggleTraffic() {
    return this._request("/api/v1/traffic/toggle", {
      method: "POST",
      body: JSON.stringify({}),
    });
  }

  /**
   * Health check endpoint.
   */
  async checkHealth() {
    return this._request("/api/v1/health");
  }
}
