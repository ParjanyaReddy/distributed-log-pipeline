/**
 * Toast Notification Component
 *
 * Provides non-intrusive, auto-dismissing visual toasts for errors and notifications.
 */

export class ToastNotifier {
  constructor() {
    this._container = document.getElementById("toast-container");
    if (!this._container) {
      this._container = document.createElement("div");
      this._container.id = "toast-container";
      this._container.className = "toast-container";
      document.body.appendChild(this._container);
    }
  }

  /**
   * Displays a toast notification.
   * @param {string} message
   * @param {"info"|"success"|"warning"|"error"} type
   * @param {number} [durationMs=4000]
   */
  show(message, type = "info", durationMs = 4000) {
    const toast = document.createElement("div");
    toast.className = `toast-item toast-${type}`;

    const iconMap = {
      info: "ℹ️",
      success: "✓",
      warning: "⚠️",
      error: "✕",
    };

    toast.innerHTML = `
      <div class="toast-icon">${iconMap[type] || "•"}</div>
      <div class="toast-msg">${this._escape(message)}</div>
      <button class="toast-close" aria-label="Close">&times;</button>
    `;

    const closeBtn = toast.querySelector(".toast-close");
    const removeToast = () => {
      toast.classList.add("toast-hiding");
      setTimeout(() => {
        if (toast.parentNode) toast.parentNode.removeChild(toast);
      }, 300);
    };

    closeBtn.addEventListener("click", removeToast);
    this._container.appendChild(toast);

    // Trigger enter animation
    requestAnimationFrame(() => toast.classList.add("toast-visible"));

    if (durationMs > 0) {
      setTimeout(removeToast, durationMs);
    }
  }

  error(message, durationMs = 5000) {
    this.show(message, "error", durationMs);
  }

  success(message, durationMs = 3500) {
    this.show(message, "success", durationMs);
  }

  warning(message, durationMs = 4000) {
    this.show(message, "warning", durationMs);
  }

  info(message, durationMs = 3500) {
    this.show(message, "info", durationMs);
  }

  _escape(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }
}
