(() => {
  const forms = document.querySelectorAll("form[data-disable-on-submit='true']");
  forms.forEach((form) => {
    form.addEventListener("submit", () => {
      const btn = form.querySelector("button[type='submit'], button:not([type])");
      if (btn) {
        btn.disabled = true;
        const busyText = form.getAttribute("data-busy-text") || "Выполняется...";
        btn.dataset.originalText = btn.textContent;
        btn.textContent = busyText;
      }
      const targetId = form.getAttribute("data-status-target");
      if (targetId) {
        const el = document.getElementById(targetId);
        if (el) {
          el.textContent = form.getAttribute("data-running-text") || "Статус: выполняется...";
        }
      }
    });
  });
})();
