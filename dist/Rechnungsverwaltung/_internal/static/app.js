/**
 * Rechnungsverwaltung — Interaktions-JS
 * Drag & Drop Upload, Dateiname-Anzeige, Flash-Auto-Hide, UI-Zoom
 */

const ZOOM_STORAGE_KEY = "rechnungsverwaltung.ui_zoom_percent";
const ZOOM_DEFAULT_PERCENT = 100;
const ZOOM_MIN_PERCENT = 70;
const ZOOM_MAX_PERCENT = 170;
const ZOOM_STEP_PERCENT = 10;
const SIDEBAR_STORAGE_KEY = "rechnungsverwaltung.sidebar_collapsed";

function rowNavigate(event, url) {
    if (!url) return;

    const target = event && event.target ? event.target : null;
    if (target && target.closest("[data-no-row-nav]")) {
        return;
    }
    if (target && target.closest("a, button, input, select, textarea, label, summary, [role='button']")) {
        return;
    }

    const selection = window.getSelection ? String(window.getSelection()) : "";
    if (selection.trim().length > 0) {
        return;
    }

    window.location.href = url;
}

window.rowNavigate = rowNavigate;

function buildNextUrlWithScroll(rawUrl, contextElement) {
    const fallback = window.location.pathname + window.location.search;
    const source = rawUrl && rawUrl.trim().length > 0 ? rawUrl.trim() : fallback;
    try {
        const url = new URL(source, window.location.origin);
        const y = Math.max(0, Math.round(window.scrollY || window.pageYOffset || 0));
        url.searchParams.set("__scroll", String(y));

        const tableShell =
            (contextElement && contextElement.closest
                ? contextElement.closest(".sticky-table-shell")
                : null) || document.querySelector(".sticky-table-shell");
        if (tableShell) {
            url.searchParams.set("__table_scroll_top", String(Math.max(0, Math.round(tableShell.scrollTop || 0))));
            url.searchParams.set("__table_scroll_left", String(Math.max(0, Math.round(tableShell.scrollLeft || 0))));
        }

        return url.pathname + url.search + url.hash;
    } catch (_error) {
        return source;
    }
}

function restoreScrollFromQuery() {
    let url;
    try {
        url = new URL(window.location.href);
    } catch (_error) {
        return;
    }

    const rawScroll = url.searchParams.get("__scroll");
    const rawTableTop = url.searchParams.get("__table_scroll_top");
    const rawTableLeft = url.searchParams.get("__table_scroll_left");

    const y = Number.parseInt(rawScroll || "", 10);
    const tableTop = Number.parseInt(rawTableTop || "", 10);
    const tableLeft = Number.parseInt(rawTableLeft || "", 10);

    function restoreWindowScroll() {
        if (!Number.isNaN(y) && y >= 0) {
            window.scrollTo({ top: y, left: 0, behavior: "auto" });
        }
    }

    function restoreTableScroll() {
        const tableShell = document.querySelector(".sticky-table-shell");
        if (!tableShell) return;
        if (!Number.isNaN(tableTop) && tableTop >= 0) {
            tableShell.scrollTop = tableTop;
        }
        if (!Number.isNaN(tableLeft) && tableLeft >= 0) {
            tableShell.scrollLeft = tableLeft;
        }
    }

    if (!Number.isNaN(y) || !Number.isNaN(tableTop) || !Number.isNaN(tableLeft)) {
        requestAnimationFrame(function () {
            restoreWindowScroll();
            restoreTableScroll();
            setTimeout(function () {
                restoreWindowScroll();
                restoreTableScroll();
            }, 30);
            setTimeout(function () {
                restoreWindowScroll();
                restoreTableScroll();
            }, 120);
        });
    }

    url.searchParams.delete("__scroll");
    url.searchParams.delete("__table_scroll_top");
    url.searchParams.delete("__table_scroll_left");
    const cleaned = url.pathname + (url.search ? url.search : "") + url.hash;
    window.history.replaceState({}, "", cleaned);
}

function clampZoomPercent(value) {
    return Math.min(ZOOM_MAX_PERCENT, Math.max(ZOOM_MIN_PERCENT, value));
}

function getStoredZoomPercent() {
    const rawValue = window.localStorage.getItem(ZOOM_STORAGE_KEY);
    const parsed = Number.parseInt(rawValue, 10);
    if (Number.isNaN(parsed)) {
        return ZOOM_DEFAULT_PERCENT;
    }
    return clampZoomPercent(parsed);
}

function applyZoomPercent(zoomPercent) {
    const clampedPercent = clampZoomPercent(zoomPercent);
    const baseFontSizePx = 18;
    const scaledFontSizePx = (baseFontSizePx * clampedPercent) / 100;
    document.documentElement.style.fontSize = scaledFontSizePx + "px";
    document.documentElement.dataset.zoomPercent = String(clampedPercent);
    window.localStorage.setItem(ZOOM_STORAGE_KEY, String(clampedPercent));
}

function adjustZoomPercent(deltaPercent) {
    applyZoomPercent(getStoredZoomPercent() + deltaPercent);
}

function shouldHandleZoomShortcut(event) {
    return (event.metaKey || event.ctrlKey) && !event.altKey;
}

function handleZoomShortcut(event) {
    if (!shouldHandleZoomShortcut(event)) return;

    const key = event.key;
    if (key === "+" || key === "=" || key === "Add") {
        event.preventDefault();
        adjustZoomPercent(ZOOM_STEP_PERCENT);
        return;
    }
    if (key === "-" || key === "_" || key === "Subtract") {
        event.preventDefault();
        adjustZoomPercent(-ZOOM_STEP_PERCENT);
        return;
    }
    if (key === "0") {
        event.preventDefault();
        applyZoomPercent(ZOOM_DEFAULT_PERCENT);
    }
}

function getStoredSidebarCollapsed() {
    return window.localStorage.getItem(SIDEBAR_STORAGE_KEY) === "1";
}

function applySidebarCollapsed(collapsed) {
    const shell = document.querySelector(".app-shell");
    const toggle = document.querySelector("[data-sidebar-toggle]");
    if (!shell || !toggle) return;

    shell.classList.toggle("sidebar-collapsed", collapsed);
    toggle.setAttribute("aria-expanded", String(!collapsed));
    toggle.setAttribute("aria-label", collapsed ? "Seitenleiste ausklappen" : "Seitenleiste einklappen");

    const icon = toggle.querySelector(".material-symbols-outlined");
    if (icon) {
        icon.textContent = collapsed ? "keyboard_double_arrow_right" : "keyboard_double_arrow_left";
    }

    window.localStorage.setItem(SIDEBAR_STORAGE_KEY, collapsed ? "1" : "0");
}

function initSidebarToggle() {
    const toggle = document.querySelector("[data-sidebar-toggle]");
    if (!toggle) return;

    applySidebarCollapsed(getStoredSidebarCollapsed());
    toggle.addEventListener("click", function () {
        const shell = document.querySelector(".app-shell");
        if (!shell) return;
        applySidebarCollapsed(!shell.classList.contains("sidebar-collapsed"));
    });
}

function setTableFocusMode(active) {
    const shell = document.querySelector(".app-shell");
    const page = document.querySelector("[data-list-focus-page]");
    if (!shell || !page) return;

    shell.classList.toggle("table-focus-mode", active);
    page.classList.toggle("table-focus-active", active);

    const toggles = page.querySelectorAll("[data-table-focus-toggle]");
    toggles.forEach(function (toggle) {
        toggle.setAttribute("aria-pressed", String(active));
        toggle.setAttribute("title", active ? "Normale Ansicht wiederherstellen" : "Nur Tabelle anzeigen");

        const icon = toggle.querySelector("[data-table-focus-icon]");
        if (icon) {
            icon.textContent = active ? "fullscreen_exit" : "fullscreen";
        }

        const label = toggle.querySelector("[data-table-focus-label]");
        if (label) {
            label.textContent = active ? "Normale Ansicht" : "Nur Tabelle";
        }
    });
}

function initTableFocusMode() {
    const page = document.querySelector("[data-list-focus-page]");
    if (!page) return;

    const toggles = page.querySelectorAll("[data-table-focus-toggle]");
    toggles.forEach(function (toggle) {
        toggle.addEventListener("click", function () {
            const shell = document.querySelector(".app-shell");
            if (!shell) return;
            setTableFocusMode(!shell.classList.contains("table-focus-mode"));
        });
    });

    document.addEventListener("keydown", function (event) {
        const shell = document.querySelector(".app-shell");
        if (!shell || !shell.classList.contains("table-focus-mode")) return;
        if (event.key === "Escape") {
            setTableFocusMode(false);
        }
    });
}

function initInvoiceAmountEditor() {
    const editor = document.querySelector("[data-amount-editor]");
    if (!editor) return;

    const toggle = editor.querySelector("[data-amount-editor-toggle]");
    const popover = editor.querySelector("[data-amount-editor-popover]");
    const form = editor.querySelector("[data-amount-editor-form]");
    const cancel = editor.querySelector("[data-amount-editor-cancel]");
    const amountInput = form ? form.querySelector("input[name='amount_gross']") : null;

    if (!toggle || !popover || !form) return;

    function setOpen(open) {
        const isOpen = Boolean(open);
        popover.hidden = !isOpen;
        editor.classList.toggle("amount-editor-open", isOpen);
        toggle.setAttribute("aria-expanded", String(isOpen));
        if (isOpen && amountInput) {
            requestAnimationFrame(function () {
                amountInput.focus();
                amountInput.select();
            });
        }
    }

    toggle.addEventListener("click", function () {
        setOpen(popover.hidden);
    });

    if (cancel) {
        cancel.addEventListener("click", function () {
            setOpen(false);
        });
    }

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape" && !popover.hidden) {
            setOpen(false);
        }
    });

    document.addEventListener("click", function (event) {
        if (popover.hidden) return;
        if (!editor.contains(event.target)) {
            setOpen(false);
        }
    });
}

function initInlineStatusEditors() {
    const cells = document.querySelectorAll("[data-inline-status-cell]");
    if (!cells.length) return;

    function setOpen(cell, open) {
        const trigger = cell.querySelector("[data-inline-status-trigger]");
        const editor = cell.querySelector("[data-inline-status-editor]");
        if (!trigger || !editor) return;
        const isOpen = Boolean(open);
        editor.hidden = !isOpen;
        cell.classList.toggle("inline-status-open", isOpen);
        trigger.setAttribute("aria-expanded", String(isOpen));
    }

    function closeAll(exceptCell) {
        cells.forEach(function (cell) {
            if (exceptCell && cell === exceptCell) return;
            setOpen(cell, false);
        });
    }

    cells.forEach(function (cell) {
        const trigger = cell.querySelector("[data-inline-status-trigger]");
        const editor = cell.querySelector("[data-inline-status-editor]");
        const form = cell.querySelector(".inline-status-form");
        const cancel = cell.querySelector("[data-inline-status-cancel]");
        if (!trigger || !editor) return;

        trigger.addEventListener("click", function (event) {
            event.preventDefault();
            event.stopPropagation();
            const shouldOpen = editor.hidden;
            closeAll(cell);
            setOpen(cell, shouldOpen);
        });

        if (cancel) {
            cancel.addEventListener("click", function (event) {
                event.preventDefault();
                event.stopPropagation();
                setOpen(cell, false);
            });
        }

        if (form) {
            form.addEventListener("submit", function () {
                const nextInput = form.querySelector("input[name='next']");
                if (nextInput) {
                    nextInput.value = buildNextUrlWithScroll(nextInput.value, form);
                }
            });
        }

        editor.addEventListener("click", function (event) {
            event.stopPropagation();
        });
    });

    document.addEventListener("click", function (event) {
        if (event.target && event.target.closest("[data-inline-status-cell]")) {
            return;
        }
        closeAll();
    });

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape") {
            closeAll();
        }
    });
}

function initInlineReminderEditors() {
    const cells = document.querySelectorAll("[data-inline-reminder-cell]");
    if (!cells.length) return;

    function setOpen(cell, open) {
        const trigger = cell.querySelector("[data-inline-reminder-trigger]");
        const editor = cell.querySelector("[data-inline-reminder-editor]");
        if (!trigger || !editor) return;
        const isOpen = Boolean(open);
        editor.hidden = !isOpen;
        cell.classList.toggle("inline-reminder-open", isOpen);
        trigger.setAttribute("aria-expanded", String(isOpen));
    }

    function closeAll(exceptCell) {
        cells.forEach(function (cell) {
            if (exceptCell && cell === exceptCell) return;
            setOpen(cell, false);
        });
    }

    cells.forEach(function (cell) {
        const trigger = cell.querySelector("[data-inline-reminder-trigger]");
        const editor = cell.querySelector("[data-inline-reminder-editor]");
        const form = cell.querySelector(".inline-reminder-form");
        const cancel = cell.querySelector("[data-inline-reminder-cancel]");
        if (!trigger || !editor) return;

        trigger.addEventListener("click", function (event) {
            event.preventDefault();
            event.stopPropagation();
            const shouldOpen = editor.hidden;
            closeAll(cell);
            setOpen(cell, shouldOpen);
        });

        if (cancel) {
            cancel.addEventListener("click", function (event) {
                event.preventDefault();
                event.stopPropagation();
                setOpen(cell, false);
            });
        }

        if (form) {
            const statusSelect = form.querySelector("select[name='reminder_status']");
            const dateInput = form.querySelector("input[name='reminder_date']");

            if (statusSelect && dateInput) {
                const syncDateField = function () {
                    const hasReminder = Boolean((statusSelect.value || "").trim());
                    dateInput.disabled = !hasReminder;
                    dateInput.classList.toggle("is-disabled", !hasReminder);
                    if (!hasReminder) {
                        dateInput.value = "";
                    }
                };
                syncDateField();
                statusSelect.addEventListener("change", syncDateField);
            }

            form.addEventListener("submit", function () {
                const nextInput = form.querySelector("input[name='next']");
                if (nextInput) {
                    nextInput.value = buildNextUrlWithScroll(nextInput.value, form);
                }
            });
        }

        editor.addEventListener("click", function (event) {
            event.stopPropagation();
        });
    });

    document.addEventListener("click", function (event) {
        if (event.target && event.target.closest("[data-inline-reminder-cell]")) {
            return;
        }
        closeAll();
    });

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape") {
            closeAll();
        }
    });
}

function initInlineRemarkEditors() {
    const cells = document.querySelectorAll("[data-inline-remark-cell]");
    if (!cells.length) return;

    function setOpen(cell, open) {
        const trigger = cell.querySelector("[data-inline-remark-trigger]");
        const editor = cell.querySelector("[data-inline-remark-editor]");
        if (!trigger || !editor) return;
        const isOpen = Boolean(open);
        editor.hidden = !isOpen;
        cell.classList.toggle("inline-remark-open", isOpen);
        trigger.setAttribute("aria-expanded", String(isOpen));
    }

    function closeAll(exceptCell) {
        cells.forEach(function (cell) {
            if (exceptCell && cell === exceptCell) return;
            setOpen(cell, false);
        });
    }

    cells.forEach(function (cell) {
        const trigger = cell.querySelector("[data-inline-remark-trigger]");
        const editor = cell.querySelector("[data-inline-remark-editor]");
        const form = cell.querySelector(".inline-remark-form");
        const cancel = cell.querySelector("[data-inline-remark-cancel]");
        const input = cell.querySelector(".inline-remark-input");
        if (!trigger || !editor) return;

        trigger.addEventListener("click", function (event) {
            event.preventDefault();
            event.stopPropagation();
            const shouldOpen = editor.hidden;
            closeAll(cell);
            setOpen(cell, shouldOpen);
            if (shouldOpen && input) {
                requestAnimationFrame(function () {
                    input.focus();
                });
            }
        });

        if (cancel) {
            cancel.addEventListener("click", function (event) {
                event.preventDefault();
                event.stopPropagation();
                setOpen(cell, false);
            });
        }

        if (form) {
            form.addEventListener("submit", function () {
                const nextInput = form.querySelector("input[name='next']");
                if (nextInput) {
                    nextInput.value = buildNextUrlWithScroll(nextInput.value, form);
                }
            });
        }

        editor.addEventListener("click", function (event) {
            event.stopPropagation();
        });
    });

    document.addEventListener("click", function (event) {
        if (event.target && event.target.closest("[data-inline-remark-cell]")) {
            return;
        }
        closeAll();
    });

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape") {
            closeAll();
        }
    });
}

function normalizeHexColor(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    if (/^#[0-9a-fA-F]{6}$/.test(raw)) {
        return raw.toLowerCase();
    }
    if (/^#[0-9a-fA-F]{3}$/.test(raw)) {
        const r = raw[1];
        const g = raw[2];
        const b = raw[3];
        return ("#" + r + r + g + g + b + b).toLowerCase();
    }
    return "";
}

function hexToRgb(hex) {
    const normalized = normalizeHexColor(hex);
    if (!normalized) return null;
    return {
        r: Number.parseInt(normalized.slice(1, 3), 16),
        g: Number.parseInt(normalized.slice(3, 5), 16),
        b: Number.parseInt(normalized.slice(5, 7), 16),
    };
}

function statusStyleFromHex(hex) {
    const rgb = hexToRgb(hex);
    if (!rgb) return "";
    return "background: rgba(" + rgb.r + ", " + rgb.g + ", " + rgb.b + ", 0.18);"
        + "color: rgb(" + rgb.r + ", " + rgb.g + ", " + rgb.b + ");"
        + "border-color: rgba(" + rgb.r + ", " + rgb.g + ", " + rgb.b + ", 0.36);";
}

function initSettingsStatusManager() {
    const root = document.querySelector("[data-status-manager]");
    if (!root) return;

    const presetToHex = {
        grau: "#6b7280",
        gruen: "#16a34a",
        rot: "#dc2626",
        orange: "#ea580c",
        blau: "#2563eb",
        lila: "#7c3aed",
        gelb: "#ca8a04",
        tuerkis: "#0d9488",
    };

    function resolveColorToHex(value) {
        const hex = normalizeHexColor(value);
        if (hex) return hex;
        const key = String(value || "").trim().toLowerCase();
        return presetToHex[key] || "";
    }

    const dataEl = document.getElementById("settings-status-data");
    let initial = {};
    if (dataEl) {
        try {
            initial = JSON.parse(dataEl.textContent || "{}");
        } catch (_error) {
            initial = {};
        }
    }

    const invoiceColors = initial && typeof initial.invoiceColors === "object" && initial.invoiceColors
        ? initial.invoiceColors
        : {};
    const paymentColors = initial && typeof initial.paymentColors === "object" && initial.paymentColors
        ? initial.paymentColors
        : {};

    function buildItems(statuses, colorMap) {
        const result = [];
        (Array.isArray(statuses) ? statuses : []).forEach(function (statusName) {
            const name = String(statusName || "").trim();
            if (!name) return;
            result.push({
                name: name,
                color: resolveColorToHex(colorMap[name]),
            });
        });
        return result;
    }

    const state = {
        invoice: buildItems(initial.invoiceStatuses, invoiceColors),
        payment: buildItems(initial.paymentStatuses, paymentColors),
    };

    const customInvoiceKeys = new Set(state.invoice.map(function (item) { return item.name.toLowerCase(); }));
    const customPaymentKeys = new Set(state.payment.map(function (item) { return item.name.toLowerCase(); }));

    const preservedInvoiceColors = {};
    Object.keys(invoiceColors || {}).forEach(function (statusName) {
        if (!customInvoiceKeys.has(String(statusName || "").toLowerCase())) {
            preservedInvoiceColors[statusName] = invoiceColors[statusName];
        }
    });

    const preservedPaymentColors = {};
    Object.keys(paymentColors || {}).forEach(function (statusName) {
        if (!customPaymentKeys.has(String(statusName || "").toLowerCase())) {
            preservedPaymentColors[statusName] = paymentColors[statusName];
        }
    });

    const hiddenInvoiceStatuses = document.querySelector("[data-hidden-target='invoice-statuses']");
    const hiddenPaymentStatuses = document.querySelector("[data-hidden-target='payment-statuses']");
    const hiddenInvoiceColors = document.querySelector("[data-hidden-target='invoice-colors']");
    const hiddenPaymentColors = document.querySelector("[data-hidden-target='payment-colors']");

    const invoiceList = root.querySelector("[data-custom-status-list='invoice']");
    const paymentList = root.querySelector("[data-custom-status-list='payment']");

    const overlay = document.querySelector("[data-status-overlay]");
    const openBtn = root.querySelector("[data-status-overlay-open]");
    const closeBtns = overlay ? overlay.querySelectorAll("[data-status-overlay-close]") : [];
    const targetInputs = overlay ? overlay.querySelectorAll("[data-status-target]") : [];
    const nameInput = overlay ? overlay.querySelector("[data-status-name-input]") : null;
    const colorInput = overlay ? overlay.querySelector("[data-status-color-input]") : null;
    const colorValue = overlay ? overlay.querySelector("[data-status-color-value]") : null;
    const addBtn = overlay ? overlay.querySelector("[data-status-add-confirm]") : null;

    function toHiddenLines(items) {
        return items.map(function (item) { return item.name; }).join("\n");
    }

    function toColorLines(kind, items) {
        const preserved = kind === "invoice" ? preservedInvoiceColors : preservedPaymentColors;
        const lines = Object.keys(preserved).map(function (statusName) {
            return String(statusName || "").trim() + "=" + String(preserved[statusName] || "").trim();
        }).filter(function (line) {
            return line.length > 1 && line.indexOf("=") > 0;
        });

        return lines.concat(items
            .filter(function (item) { return normalizeHexColor(item.color); })
            .map(function (item) { return item.name + "=" + normalizeHexColor(item.color); }))
            .join("\n");
    }

    function syncHiddenFields() {
        if (hiddenInvoiceStatuses) hiddenInvoiceStatuses.value = toHiddenLines(state.invoice);
        if (hiddenPaymentStatuses) hiddenPaymentStatuses.value = toHiddenLines(state.payment);
        if (hiddenInvoiceColors) hiddenInvoiceColors.value = toColorLines("invoice", state.invoice);
        if (hiddenPaymentColors) hiddenPaymentColors.value = toColorLines("payment", state.payment);
    }

    function updateColorValueLabel() {
        if (!colorInput || !colorValue) return;
        colorValue.textContent = normalizeHexColor(colorInput.value) || "#2563eb";
    }

    function renderList(kind, container) {
        if (!container) return;
        container.innerHTML = "";
        const items = kind === "invoice" ? state.invoice : state.payment;
        if (!items.length) {
            const empty = document.createElement("p");
            empty.className = "settings-custom-status-empty";
            empty.textContent = "Noch keine zusätzlichen Status.";
            container.appendChild(empty);
            return;
        }

        items.forEach(function (item, index) {
            const row = document.createElement("div");
            row.className = "settings-custom-status-item";

            const badge = document.createElement("span");
            badge.className = "status-badge";
            badge.textContent = item.name;
            const style = statusStyleFromHex(item.color);
            if (style) badge.setAttribute("style", style);
            row.appendChild(badge);

            const picker = document.createElement("input");
            picker.type = "color";
            picker.className = "settings-custom-status-color";
            picker.value = normalizeHexColor(item.color) || "#2563eb";
            picker.title = "Farbe ändern";
            picker.addEventListener("input", function () {
                item.color = normalizeHexColor(picker.value);
                const nextStyle = statusStyleFromHex(item.color);
                if (nextStyle) {
                    badge.setAttribute("style", nextStyle);
                } else {
                    badge.removeAttribute("style");
                }
                syncHiddenFields();
            });
            row.appendChild(picker);

            const removeBtn = document.createElement("button");
            removeBtn.type = "button";
            removeBtn.className = "settings-custom-status-remove";
            removeBtn.textContent = "✕";
            removeBtn.title = "Status entfernen";
            removeBtn.addEventListener("click", function () {
                items.splice(index, 1);
                renderAll();
            });
            row.appendChild(removeBtn);

            container.appendChild(row);
        });
    }

    function renderAll() {
        renderList("invoice", invoiceList);
        renderList("payment", paymentList);
        syncHiddenFields();
    }

    function setOverlayOpen(open) {
        if (!overlay) return;
        const isOpen = Boolean(open);
        overlay.hidden = !isOpen;
        if (isOpen && nameInput) {
            requestAnimationFrame(function () {
                nameInput.focus();
                nameInput.select();
            });
        }
    }

    if (openBtn && overlay) {
        openBtn.addEventListener("click", function () {
            setOverlayOpen(true);
        });
    }

    closeBtns.forEach(function (btn) {
        btn.addEventListener("click", function () {
            setOverlayOpen(false);
        });
    });

    if (overlay) {
        overlay.addEventListener("click", function (event) {
            if (event.target && event.target.matches("[data-status-overlay-backdrop]")) {
                setOverlayOpen(false);
            }
        });
    }

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape" && overlay && !overlay.hidden) {
            setOverlayOpen(false);
        }
    });

    if (colorInput) {
        colorInput.addEventListener("input", updateColorValueLabel);
        updateColorValueLabel();
    }

    if (addBtn && nameInput) {
        addBtn.addEventListener("click", function () {
            const name = String(nameInput.value || "").trim();
            const color = normalizeHexColor(colorInput ? colorInput.value : "");
            if (!name) {
                alert("Bitte einen Statusnamen eingeben.");
                return;
            }
            if (name.length > 60) {
                alert("Statusname darf maximal 60 Zeichen haben.");
                return;
            }

            let target = "invoice";
            targetInputs.forEach(function (input) {
                if (input.checked) target = input.value;
            });

            const list = target === "payment" ? state.payment : state.invoice;
            const existing = list.find(function (item) {
                return item.name.toLowerCase() === name.toLowerCase();
            });
            if (existing) {
                existing.color = color || existing.color;
            } else {
                list.push({ name: name, color: color });
            }

            nameInput.value = "";
            renderAll();
            setOverlayOpen(false);
        });
    }

    renderAll();
}

document.addEventListener("DOMContentLoaded", function () {
    restoreScrollFromQuery();
    applyZoomPercent(getStoredZoomPercent());
    document.addEventListener("keydown", handleZoomShortcut);
    initSidebarToggle();
    initTableFocusMode();
    initInvoiceAmountEditor();
    initInlineStatusEditors();
    initInlineReminderEditors();
    initInlineRemarkEditors();
    initSettingsStatusManager();

    // === Drag & Drop + File Select for Upload ===
    const forms = ["rechnungen", "sparkasse", "voba_kraichgau", "voba_pur"];

    forms.forEach(function (name) {
        const fileInput = document.getElementById("file-" + name);
        const dropzone = document.getElementById("dropzone-" + name);
        const filenameSpan = document.getElementById("filename-" + name);
        const submitBtn = document.getElementById("btn-" + name);

        if (!fileInput || !dropzone) return;

        // Show filename when file is selected
        fileInput.addEventListener("change", function () {
            if (fileInput.files.length > 0) {
                filenameSpan.textContent = "📎 " + fileInput.files[0].name;
                submitBtn.disabled = false;
            }
        });

        // Drag & Drop events
        dropzone.addEventListener("dragover", function (e) {
            e.preventDefault();
            dropzone.classList.add("drag-over");
        });

        dropzone.addEventListener("dragleave", function () {
            dropzone.classList.remove("drag-over");
        });

        dropzone.addEventListener("drop", function (e) {
            e.preventDefault();
            dropzone.classList.remove("drag-over");
            if (e.dataTransfer.files.length > 0) {
                fileInput.files = e.dataTransfer.files;
                filenameSpan.textContent = "📎 " + e.dataTransfer.files[0].name;
                submitBtn.disabled = false;
            }
        });
    });

    // === Auto-hide flash messages after 8 seconds ===
    var flashes = document.querySelectorAll(".flash");
    flashes.forEach(function (flash) {
        setTimeout(function () {
            flash.style.transition = "opacity 0.5s ease";
            flash.style.opacity = "0";
            setTimeout(function () { flash.remove(); }, 500);
        }, 8000);
    });
});
