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

document.addEventListener("DOMContentLoaded", function () {
    applyZoomPercent(getStoredZoomPercent());
    document.addEventListener("keydown", handleZoomShortcut);
    initSidebarToggle();
    initTableFocusMode();

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
