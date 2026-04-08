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
