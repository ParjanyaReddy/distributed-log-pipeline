/**
 * Raw Payload Inspector Modal Component
 */

export class InspectorModalComponent {
  constructor(store) {
    this.store = store;
    this.dom = {
      modal: document.getElementById("inspector-modal"),
      btnClose: document.getElementById("btn-close-inspector"),
      jsonContent: document.getElementById("inspector-json-content"),
    };

    this._bindEvents();
    this.render(this.store.getState());
    this.store.subscribe((state) => this.render(state));
  }

  _bindEvents() {
    if (this.dom.btnClose) {
      this.dom.btnClose.addEventListener("click", () => {
        this.store.closeInspector();
      });
    }

    if (this.dom.modal) {
      this.dom.modal.addEventListener("click", (e) => {
        if (e.target === this.dom.modal) {
          this.store.closeInspector();
        }
      });
    }

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && this.store.getState().isModalOpen) {
        this.store.closeInspector();
      }
    });
  }

  render(state) {
    if (!this.dom.modal || !this.dom.jsonContent) return;

    if (state.isModalOpen && state.selectedLog) {
      this.dom.jsonContent.textContent = JSON.stringify(state.selectedLog, null, 2);
      this.dom.modal.classList.add("active");
    } else {
      this.dom.modal.classList.remove("active");
    }
  }
}
