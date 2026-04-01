/**
 * Rechnungsverwaltung — Interaktions-JS
 * Drag & Drop Upload, Dateiname-Anzeige, Flash-Auto-Hide, UI-Zoom
 */

const ZOOM_STORAGE_KEY = "rechnungsverwaltung.ui_zoom_percent";
const ZOOM_DEFAULT_PERCENT = 100;
const ZOOM_MIN_PERCENT = 70;
const ZOOM_MAX_PERCENT = 170;
const ZOOM_STEP_PERCENT = 10;

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

document.addEventListener("DOMContentLoaded", function () {
    applyZoomPercent(getStoredZoomPercent());
    document.addEventListener("keydown", handleZoomShortcut);

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
