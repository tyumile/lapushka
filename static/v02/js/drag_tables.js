(() => {
  const initDragTable = (tableId, formId, orderInputId) => {
    const table = document.getElementById(tableId);
    if (!table) return;
    let dragEl = null;
    table.querySelectorAll("tbody tr").forEach((row) => {
      row.addEventListener("dragstart", () => { dragEl = row; });
      row.addEventListener("dragover", (e) => e.preventDefault());
      row.addEventListener("drop", (e) => {
        e.preventDefault();
        if (!dragEl || dragEl === row) return;
        const tbody = table.querySelector("tbody");
        const rows = Array.from(tbody.children);
        if (rows.indexOf(dragEl) < rows.indexOf(row)) tbody.insertBefore(dragEl, row.nextSibling);
        else tbody.insertBefore(dragEl, row);
      });
    });
    if (!formId || !orderInputId) return;
    const form = document.getElementById(formId);
    const orderInput = document.getElementById(orderInputId);
    if (!form || !orderInput) return;
    form.addEventListener("submit", () => {
      orderInput.value = Array.from(table.querySelectorAll("tbody tr")).map((r) => r.dataset.idx).join(",");
    });
  };

  initDragTable("docPlanTable", "docPlanSaveForm", "rows_order");
  initDragTable("docsTable", "editForm", "rows_order");

  const toggle = document.getElementById("onlyNeedsToggle");
  if (toggle) {
    const filterRows = () => {
      document.querySelectorAll("tr[data-status]").forEach((row) => {
        const st = (row.getAttribute("data-status") || "").toLowerCase();
        row.style.display = toggle.checked && !st.startsWith("needs_") ? "none" : "";
      });
    };
    toggle.addEventListener("change", filterRows);
  }
})();
