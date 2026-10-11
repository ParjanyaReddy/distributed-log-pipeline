/**
 * Data and Telemetry Formatting Utilities
 */

/**
 * Formats a number with comma grouping.
 * @param {number} n
 * @returns {string}
 */
export function formatNumber(n) {
  return (n || 0).toLocaleString();
}

/**
 * Formats a byte quantity into human-readable units (B, KB, MB, GB).
 * @param {number} b
 * @returns {string}
 */
export function formatBytes(b) {
  if (!b || b === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(b) / Math.log(1024));
  return (b / Math.pow(1024, i)).toFixed(1) + " " + units[i];
}

/**
 * Formats an ISO timestamp into high-precision HH:MM:SS.mmm format.
 * @param {string|Date} iso
 * @returns {string}
 */
export function formatTimestamp(iso) {
  if (!iso) return "--:--:--.---";
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return String(iso);
    const timePart = d.toTimeString().split(" ")[0];
    const ms = String(d.getMilliseconds()).padStart(3, "0");
    return `${timePart}.${ms}`;
  } catch {
    return String(iso);
  }
}
