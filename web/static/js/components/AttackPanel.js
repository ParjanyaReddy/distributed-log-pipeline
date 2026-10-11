/**
 * Adversary Simulation & Traffic Control Panel Component
 */

export class AttackPanelComponent {
  constructor(store, apiClient, toast) {
    this.store = store;
    this.apiClient = apiClient;
    this.toast = toast;
    this.dom = {
      attackButtons: document.querySelectorAll(".btn-adversary"),
      btnToggleTraffic: document.getElementById("btn-toggle-traffic"),
    };

    this._bindEvents();
  }

  _bindEvents() {
    // Attack scenario buttons
    if (this.dom.attackButtons) {
      this.dom.attackButtons.forEach((btn) => {
        btn.addEventListener("click", async () => {
          const attackType = btn.getAttribute("data-attack");
          btn.classList.add("injecting");

          try {
            const result = await this.apiClient.triggerAttack(attackType);
            this.toast.warning(`Scenario '${attackType}' injected into pipeline.`);

            // Rapid refresh to reflect injected events in UI immediately
            const [alerts, logs, stats] = await Promise.all([
              this.apiClient.fetchAlerts(),
              this.apiClient.fetchLogs(),
              this.apiClient.fetchStats(),
            ]);

            this.store.setAlerts(alerts);
            this.store.setLogs(logs);
            this.store.updateStats(stats);
          } catch (err) {
            console.error("Attack simulation failed:", err);
            this.toast.error(`Attack injection failed: ${err.message}`);
          } finally {
            setTimeout(() => btn.classList.remove("injecting"), 600);
          }
        });
      });
    }

    // Stream pause/resume button
    if (this.dom.btnToggleTraffic) {
      this.dom.btnToggleTraffic.addEventListener("click", async () => {
        try {
          const res = await this.apiClient.toggleTraffic();
          const stats = await this.apiClient.fetchStats();
          this.store.updateStats(stats);
          if (res.streaming_active) {
            this.toast.success("Stream traffic resumed.");
          } else {
            this.toast.info("Stream traffic paused.");
          }
        } catch (err) {
          console.error("Toggle traffic failed:", err);
          this.toast.error(`Traffic toggle error: ${err.message}`);
        }
      });
    }
  }
}
