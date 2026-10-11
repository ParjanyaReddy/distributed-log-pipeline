/**
 * Partition Shard Matrix Component
 *
 * Visualizes live Kafka partition distribution across the 6 topic shards.
 */

import { formatNumber } from "../utils/formatters.js";

export class PartitionMatrixComponent {
  constructor(store) {
    this.store = store;
    this.render(this.store.getState());
    this.store.subscribe((state) => this.render(state));
  }

  render(state) {
    const { stats } = state;
    if (!stats || !Array.isArray(stats.partitionCounts)) return;

    const totalConsumed = Math.max(stats.totalConsumed || 1, 1);

    stats.partitionCounts.forEach((count, i) => {
      const countEl = document.getElementById(`shard-${i}-count`);
      const fillEl = document.getElementById(`shard-${i}-fill`);
      const pctEl = document.getElementById(`shard-${i}-pct`);

      if (countEl) countEl.textContent = formatNumber(count);
      const sharePct = ((count / totalConsumed) * 100).toFixed(1);
      if (pctEl) pctEl.textContent = `${sharePct}%`;
      if (fillEl) fillEl.style.width = `${Math.min(Math.max(sharePct * 1.5, 5), 100)}%`;
    });
  }
}
