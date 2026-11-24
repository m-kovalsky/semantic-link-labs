"""The Semantic Explorer: browse, inspect and edit a semantic model.

An anywidget-based recreation of the Tools app's model explorer. The model tree
covers tables, columns, measures, hierarchies (and levels), calculation items,
partitions, relationships, expressions, functions, perspectives, roles and
cultures. Selecting an object shows its properties, expressions (with DAX / M
syntax highlighting), annotations, extended properties, translations and
perspective membership. In read-write mode, changes are staged locally (with
undo / redo and a change history) and only written to the model on Save.
"""

import json
import re
from typing import Dict, List, Optional, Tuple
from uuid import UUID
from sempy._utils._log import log

_WIDGET_CSS = """
.slls-sx {
    __SLLS_LIGHT_VARS__
    __SLLS_SYNTAX_VARS__
    --sx-radius: 12px;
    --sx-radius-sm: 8px;
    --sx-warn: #d97706;
    --sx-warn-soft: rgba(217, 119, 6, 0.14);
    --sx-success: #16a34a;
    --sx-success-soft: rgba(22, 163, 74, 0.12);
    --sx-mono: "SF Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    /* Relationship diagram highlights, key roles and data-source borders. */
    --sx-hl-mm: #d946ef; --sx-hl-bidi: #f59e0b; --sx-hl-lim: #f43f5e; --sx-hl-nonint: #f97316; --sx-hl-sf: #8b5cf6;
    --sx-key-pk: #d97706; --sx-key-fk: #0ea5e9;
    --sx-src-0: #3b82f6; --sx-src-1: #22c55e; --sx-src-2: #f59e0b; --sx-src-3: #a855f7; --sx-src-4: #ec4899;
    --sx-src-5: #14b8a6; --sx-src-6: #ef4444; --sx-src-7: #6366f1; --sx-src-8: #84cc16; --sx-src-9: #f97316;
    /* Object-type hues for the dependency node graph. */
    --sx-cat-measure: #059669; --sx-cat-column: #2563eb; --sx-cat-hierarchy: #7c3aed;
    --sx-cat-item: #d97706; --sx-cat-table: #ea580c; --sx-cat-role: #e11d48; --sx-cat-other: #64748b;
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text",
        "Helvetica Neue", Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    color: var(--ui-text);
    background: var(--ui-bg-solid);
    border: 1px solid var(--ui-border);
    border-radius: var(--sx-radius);
    box-shadow: var(--ui-shadow-md);
    box-sizing: border-box;
    /* Do not inherit word breaking from the notebook host. */
    word-break: normal;
    overflow-wrap: normal;
    width: 100%;
    display: flex;
    flex-direction: column;
    position: relative;
    overflow: hidden;
    font-size: 13px;
}
.slls-sx.slls-sx-dark {
    --sx-cat-measure: #34d399; --sx-cat-column: #60a5fa; --sx-cat-hierarchy: #a78bfa;
    --sx-cat-item: #fbbf24; --sx-cat-table: #fb923c; --sx-cat-role: #fb7185; --sx-cat-other: #94a3b8;
}
.slls-sx.slls-sx-dark { __SLLS_DARK_VARS__ --sx-warn: #f59e0b; --sx-warn-soft: rgba(245, 158, 11, 0.18); --sx-success: #22c55e; --sx-success-soft: rgba(34, 197, 94, 0.16); }
@media (prefers-color-scheme: dark) {
    .slls-sx.slls-sx-auto { __SLLS_DARK_VARS__ }
}
.slls-sx *, .slls-sx *::before, .slls-sx *::after { box-sizing: border-box; }
.slls-sx svg { display: block; flex-shrink: 0; }
.slls-sx button { font-family: inherit; }

/* Full screen (native, CSS overlay, or hosted in the app shell) */
.slls-sx.slls-sx-fs, .slls-sx:fullscreen, .slls-sx.slls-app-fs-tool {
    height: 100vh; border-radius: 0; border: none; box-shadow: none;
}
.slls-sx.slls-sx-fs .slls-sx-body, .slls-sx:fullscreen .slls-sx-body,
.slls-sx.slls-app-fs-tool .slls-sx-body { flex: 1 1 0; height: auto; min-height: 0; }

/* Header */
.slls-sx-header {
    display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
    padding: 14px 18px; border-bottom: 1px solid var(--ui-border);
}
.slls-sx-headicon {
    display: inline-flex; align-items: center; justify-content: center;
    width: 40px; height: 40px; flex: 0 0 auto;
    border: 1px solid var(--ui-border); border-radius: 10px;
    background: var(--ui-surface-2); color: var(--ui-accent);
}
.slls-sx-headicon svg { width: 27px; height: 27px; stroke-width: 1.5; }
.slls-sx-titlewrap { display: flex; flex-direction: column; min-width: 0; }
.slls-sx-title { font-size: 20px; font-weight: 600; letter-spacing: -0.01em; line-height: 1.15; }
.slls-sx-subtitle {
    font-size: 12.5px; color: var(--ui-text-secondary); margin-top: 2px;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 460px;
}
.slls-sx-subtitle b { color: var(--ui-text); font-weight: 500; }
.slls-sx-sep { margin: 0 6px; color: var(--ui-text-tertiary); }
.slls-sx-head-group {
    display: flex; align-items: center; gap: 6px; padding-left: 10px;
    border-left: 1px solid var(--ui-border);
}
.slls-sx-head-spacer { flex: 1 1 auto; }

/* Generic buttons */
.slls-sx-btn {
    appearance: none; display: inline-flex; align-items: center; justify-content: center; gap: 6px;
    height: 32px; padding: 0 12px; border-radius: var(--sx-radius-sm);
    border: 1px solid var(--ui-border-strong); background: var(--ui-bg);
    color: var(--ui-text); font-size: 12.5px; font-weight: 500; cursor: pointer; white-space: nowrap;
    transition: background 120ms ease, border-color 120ms ease, color 120ms ease, opacity 120ms ease;
}
.slls-sx-btn:hover:not(:disabled) { background: var(--ui-bg-hover); }
.slls-sx-btn:disabled { opacity: 0.45; cursor: not-allowed; }
.slls-sx-btn svg { width: 15px; height: 15px; }
.slls-sx-btn.icon { width: 32px; padding: 0; }
.slls-sx-btn.primary { background: var(--ui-accent); border-color: var(--ui-accent); color: var(--ui-on-accent); }
.slls-sx-btn.primary:hover:not(:disabled) { background: var(--ui-accent-hover); }
.slls-sx-btn.danger { color: var(--ui-danger-text); border-color: var(--ui-danger-border); }
.slls-sx-btn.danger:hover:not(:disabled) { background: var(--ui-danger-bg); }
.slls-sx-btn.small { height: 26px; padding: 0 9px; font-size: 12px; }
.slls-sx-btn.small.icon { width: 26px; padding: 0; }
.slls-sx-btn.errors.has { color: var(--ui-danger-text); border-color: var(--ui-danger-border); background: var(--ui-danger-bg); }
.slls-sx-iconbtn {
    appearance: none; display: inline-flex; align-items: center; justify-content: center;
    width: 26px; height: 26px; border-radius: 6px; border: none; background: transparent;
    color: var(--ui-text-secondary); cursor: pointer; transition: background 120ms ease, color 120ms ease;
}
.slls-sx-iconbtn:hover:not(:disabled) { background: var(--ui-surface-2); color: var(--ui-text); }
.slls-sx-iconbtn:disabled { opacity: 0.4; cursor: not-allowed; }
.slls-sx-iconbtn.on { color: var(--ui-accent); }
.slls-sx-iconbtn svg { width: 15px; height: 15px; }

/* Read / write switch */
.slls-sx-mode {
    appearance: none; display: inline-flex; align-items: center; gap: 7px; height: 32px; padding: 0 8px;
    border: none; background: transparent; color: var(--ui-text-secondary); font-size: 12.5px;
    font-weight: 500; cursor: pointer; border-radius: var(--sx-radius-sm);
}
.slls-sx-mode:hover { color: var(--ui-text); }
.slls-sx-mode svg { width: 14px; height: 14px; }
.slls-sx-mode.write svg { color: var(--sx-warn); }
.slls-sx-track {
    position: relative; width: 28px; height: 16px; border-radius: 999px; background: var(--ui-border-strong);
    transition: background 150ms ease; flex-shrink: 0;
}
.slls-sx-track::after {
    content: ""; position: absolute; top: 2px; left: 2px; width: 12px; height: 12px; border-radius: 50%;
    background: var(--ui-on-accent); box-shadow: var(--ui-shadow-sm); transition: transform 150ms ease;
}
.slls-sx-mode.write .slls-sx-track { background: var(--sx-warn); }
.slls-sx-mode.write .slls-sx-track::after { transform: translateX(12px); }
/* True / false property toggle */
.slls-sx-switch {
    appearance: none; display: inline-flex; align-items: center; gap: 8px; padding: 2px 0;
    border: none; background: transparent; color: var(--ui-text-secondary); font: inherit; font-size: 12.5px;
    cursor: pointer;
}
.slls-sx-switch.on { color: var(--ui-text); }
.slls-sx-switch.on .slls-sx-track { background: var(--ui-accent); }
.slls-sx-switch.on .slls-sx-track::after { transform: translateX(12px); }
.slls-sx-switch:focus-visible { outline: none; }
.slls-sx-switch:focus-visible .slls-sx-track { box-shadow: 0 0 0 3px var(--ui-accent-soft); }

/* Picker */
.slls-sx-picker { padding: 26px 24px; display: flex; justify-content: center; }
.slls-sx-picker-card {
    width: 100%; max-width: 640px; padding: 20px; border: 1px solid var(--ui-border);
    border-radius: var(--sx-radius); background: var(--ui-bg-tertiary);
}
.slls-sx-picker-title { font-size: 15px; font-weight: 600; }
.slls-sx-picker-sub { font-size: 12.5px; color: var(--ui-text-secondary); margin: 4px 0 16px; }
.slls-sx-picker-row { display: flex; gap: 12px; flex-wrap: wrap; }
.slls-sx-field { display: flex; flex-direction: column; gap: 5px; flex: 1 1 220px; min-width: 0; }
.slls-sx-field > label {
    font-size: 11px; font-weight: 600; letter-spacing: 0.04em; text-transform: uppercase;
    color: var(--ui-text-tertiary); padding-left: 2px;
}
.slls-sx-picker-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 16px; }
.slls-sx-picker-top { display: flex; align-items: center; gap: 10px; flex-shrink: 0; }
.slls-sx-picker-note { margin-top: 10px; font-size: 12px; color: var(--ui-text-secondary); }
.slls-sx-picker-note.error { color: var(--ui-danger-text); }
.slls-sx-picker-error {
    margin-top: 10px; padding: 8px 12px; font-size: 12.5px; border-radius: 8px;
    border: 1px solid var(--ui-danger-border); background: var(--ui-danger-bg); color: var(--ui-danger-text);
}
/* Picker actions use the pill buttons shared by the other tools' pickers. */
.slls-sx-picker-actions .slls-sx-btn {
    height: auto; padding: 7px 16px; border-radius: 999px; font-size: 13.5px;
    background: var(--ui-surface); border-color: var(--ui-border-strong);
}
.slls-sx-picker-actions .slls-sx-btn:hover:not(:disabled) { background: var(--ui-surface-2); border-color: var(--ui-text-tertiary); }
.slls-sx-picker-actions .slls-sx-btn.primary { background: var(--ui-accent); border-color: var(--ui-accent); color: var(--ui-on-accent); }
.slls-sx-picker-actions .slls-sx-btn.primary:hover:not(:disabled) { background: var(--ui-accent-hover); border-color: var(--ui-accent-hover); }
.slls-sx-picker-toprow { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
.slls-sx-picker-head { display: flex; align-items: flex-start; gap: 6px; min-width: 0; }
.slls-sx-picker-back { margin: -3px 0 0 -6px; flex-shrink: 0; }
.slls-sx-picker-card.slls-sx-view-picker { max-width: 760px; }
.slls-sx-picker-row.three .slls-sx-field { flex-basis: 180px; }
/* Picker text boxes line up with the searchable selects beside them. */
.slls-sx-picker-row .slls-sx-field > .slls-sx-input { padding: 10px 12px; font-size: 14px; border-radius: 10px; }

/* Landing: semantic model or semantic view */
.slls-sx-picker-card.slls-sx-landing { max-width: 720px; }
.slls-sx-choices { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 12px; }
.slls-sx-choice {
    appearance: none; display: flex; flex-direction: column; gap: 8px; padding: 16px; text-align: left;
    border: 1px solid var(--ui-border-strong); border-radius: var(--sx-radius); background: var(--ui-bg);
    color: var(--ui-text); font: inherit; cursor: pointer;
    transition: border-color 120ms ease, box-shadow 120ms ease, background 120ms ease;
}
.slls-sx-choice:hover { border-color: var(--ui-accent); background: var(--ui-bg-hover); }
.slls-sx-choice:focus-visible { outline: none; border-color: var(--ui-accent); box-shadow: 0 0 0 3px var(--ui-accent-soft); }
.slls-sx button.slls-sx-choice:not(:disabled):active { transform: none; }
.slls-sx-choice-head { display: flex; align-items: center; gap: 10px; font-size: 14px; font-weight: 600; }
.slls-sx-choice-head .slls-sx-pill { margin-left: 0; }
.slls-sx-choice-desc { font-size: 12.5px; line-height: 1.45; color: var(--ui-text-secondary); }
.slls-sx-choice-meta { margin-top: auto; padding-top: 2px; display: flex; align-items: center; gap: 4px; font-size: 12.5px; font-weight: 500; color: var(--ui-accent); }
.slls-sx-choice-meta svg { width: 14px; height: 14px; }

/* Semantic view explorer */
.slls-sx-vtree { padding: 4px 0; }
.slls-sx-vtree .slls-sx-row { position: relative; }
.slls-sx-issue { display: flex; align-items: flex-start; gap: 8px; padding: 6px 2px; font-size: 12.5px; border-bottom: 1px solid var(--ui-border); }
.slls-sx-issue:last-child { border-bottom: none; }
.slls-sx-issue > span:nth-child(2) { flex: 1 1 auto; min-width: 0; overflow-wrap: anywhere; }
.slls-sx-issue svg { width: 14px; height: 14px; }
.slls-sx-issue .slls-sx-ricon { margin-top: 1px; }
.slls-sx-issue.error { color: var(--ui-danger-text); }
.slls-sx-issue.error .slls-sx-ricon { color: var(--ui-danger-text); }
.slls-sx-issue.warning .slls-sx-ricon { color: var(--sx-warn); }
.slls-sx-issue .file { flex-shrink: 0; max-width: 40%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-subitem.on { background: var(--ui-accent-soft); }
.slls-sx-subitem.muted { color: var(--ui-text-tertiary); font-style: italic; }
.slls-sx-files-name { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-subitem .meta { white-space: nowrap; flex-shrink: 0; }
.slls-sx-verrors { border: 1px solid var(--ui-danger-border); border-radius: var(--sx-radius); background: var(--ui-danger-bg); padding: 2px 10px; }
.slls-sx-vnote { margin: 0; }
.slls-sx-vsource { width: 100%; }
.slls-sx-unpub { display: inline-flex; color: var(--sx-warn); }
.slls-sx-unpub svg { width: 12px; height: 12px; }
.slls-sx-unpub-text { color: var(--sx-warn); font-weight: 500; }
/* Save + options split button */
.slls-sx-split { display: inline-flex; }
.slls-sx-split .slls-sx-split-main { border-top-right-radius: 0; border-bottom-right-radius: 0; }
.slls-sx-split .slls-sx-split-more { width: 20px; min-width: 20px; padding: 0; margin-left: -1px; border-top-left-radius: 0; border-bottom-left-radius: 0; }
.slls-sx-split .slls-sx-split-more svg { width: 12px; height: 12px; }
.slls-sx-vcols td:nth-child(2) { white-space: nowrap; color: var(--ui-text-secondary); }
pre.slls-sx-dialect {
    margin: 0 0 10px; padding: 8px 10px; border: 1px solid var(--ui-border); border-radius: var(--sx-radius-sm);
    background: var(--ui-bg-tertiary); font-family: var(--sx-mono); font-size: 12px; white-space: pre-wrap; overflow-wrap: anywhere;
}
.slls-sx-modal-body.slls-sx-files { display: flex; padding: 0; overflow: hidden; }
.slls-sx-files-list { width: 260px; flex-shrink: 0; overflow: auto; padding: 6px 8px 10px; border-right: 1px solid var(--ui-border); }
.slls-sx-files-group { padding: 8px 8px 4px; }
.slls-sx-files-label { font-size: 11px; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; color: var(--ui-text-tertiary); }
.slls-sx-files-folder { font-size: 11.5px; color: var(--ui-text-tertiary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-files-none { padding: 2px 8px 6px; }
.slls-sx-files-pane { flex: 1 1 auto; min-width: 0; display: flex; flex-direction: column; gap: 8px; padding: 12px 16px 14px; }
.slls-sx-files-pane .slls-sx-picker-note, .slls-sx-files-pane .slls-sx-code-error { margin: 0; }
.slls-sx-files-head { display: flex; align-items: baseline; gap: 10px; min-width: 0; font-size: 12.5px; }
.slls-sx-files-head b { font-weight: 600; flex-shrink: 0; }
.slls-sx-files-head span { color: var(--ui-text-tertiary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-files-code {
    flex: 1 1 auto; min-height: 0; overflow: auto; padding: 10px 12px;
    border: 1px solid var(--ui-border); border-radius: var(--sx-radius-sm); background: var(--ui-bg-tertiary);
}

/* Body */
.slls-sx-body { display: flex; height: 700px; min-height: 0; padding: 12px; gap: 0; }
.slls-sx-center {
    flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center;
    gap: 10px; color: var(--ui-text-secondary); text-align: center; padding: 30px;
}
.slls-sx-center svg { width: 28px; height: 28px; }
.slls-sx-spin { animation: slls-sx-spin 0.9s linear infinite; }
@keyframes slls-sx-spin { to { transform: rotate(360deg); } }

/* Tree pane */
.slls-sx-tree-pane {
    display: flex; flex-direction: column; flex-shrink: 0; min-height: 0;
    border: 1px solid var(--ui-border); border-radius: var(--sx-radius) 0 0 var(--sx-radius);
    background: var(--ui-bg);
}
.slls-sx-tree-pane.collapsed { width: 46px !important; align-items: center; padding: 8px 0; gap: 8px; }
.slls-sx-pane-head {
    display: flex; align-items: center; justify-content: space-between; gap: 6px;
    padding: 8px 10px 8px 14px; border-bottom: 1px solid var(--ui-border);
}
.slls-sx-pane-label {
    font-size: 11px; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase;
    color: var(--ui-text-tertiary);
}
.slls-sx-pane-tools { display: flex; align-items: center; gap: 2px; }
.slls-sx-pane-tools .sl-reload-btn { width: 26px; height: 26px; }
.slls-sx-filters { display: flex; flex-direction: column; gap: 6px; padding: 8px; border-bottom: 1px solid var(--ui-border); }
.slls-sx-search { position: relative; display: flex; align-items: center; }
.slls-sx-search > span {
    position: absolute; left: 10px; color: var(--ui-text-tertiary); pointer-events: none; display: inline-flex;
}
.slls-sx-search > span svg { width: 14px; height: 14px; }
.slls-sx-input, .slls-sx-select, .slls-sx-textarea {
    width: 100%; min-width: 0; font-family: inherit; font-size: 12.5px; color: var(--ui-text);
    background: var(--ui-bg); border: 1px solid var(--ui-border-strong); border-radius: var(--sx-radius-sm);
    padding: 6px 9px; outline: none; transition: border-color 120ms ease, box-shadow 120ms ease;
}
.slls-sx-input:focus, .slls-sx-select:focus, .slls-sx-textarea:focus {
    border-color: var(--ui-accent); box-shadow: 0 0 0 3px var(--ui-accent-soft);
}
.slls-sx-input:disabled, .slls-sx-select:disabled { opacity: 0.6; }
.slls-sx-search .slls-sx-input { padding-left: 30px; }
.slls-sx-textarea { resize: vertical; min-height: 54px; line-height: 1.45; }
.slls-sx-filter-row { display: flex; align-items: center; gap: 6px; color: var(--ui-text-tertiary); }
.slls-sx-filter-row svg { width: 14px; height: 14px; }
.slls-sx-typefilter { position: relative; }
.slls-sx-typebtn {
    appearance: none; width: 100%; display: flex; align-items: center; justify-content: space-between; gap: 6px;
    height: 30px; padding: 0 9px; font-size: 12.5px; color: var(--ui-text-secondary); cursor: pointer;
    background: var(--ui-bg); border: 1px solid var(--ui-border-strong); border-radius: var(--sx-radius-sm);
}
.slls-sx-typebtn > span { display: inline-flex; align-items: center; gap: 6px; }
.slls-sx-typebtn svg { width: 14px; height: 14px; }
.slls-sx-menu {
    position: absolute; z-index: 30; min-width: 220px; max-height: 380px; overflow: auto; padding: 4px;
    background: var(--ui-bg-solid); border: 1px solid var(--ui-border-strong); border-radius: 10px;
    box-shadow: var(--ui-shadow-lg);
}
.slls-sx-typefilter .slls-sx-menu { left: 0; right: 0; top: calc(100% + 4px); }
.slls-sx-menu-head {
    display: flex; align-items: center; justify-content: space-between; padding: 4px 8px;
    font-size: 10.5px; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; color: var(--ui-text-tertiary);
}
.slls-sx-menu-head button { border: none; background: none; color: var(--ui-accent); font-size: 11.5px; font-weight: 600; cursor: pointer; }
.slls-sx-menu-item {
    appearance: none; width: 100%; display: flex; align-items: center; gap: 8px; padding: 6px 8px;
    border: none; border-radius: 6px; background: transparent; color: var(--ui-text); font-size: 12.5px;
    text-align: left; cursor: pointer;
}
.slls-sx-menu-item:hover:not(:disabled) { background: var(--ui-surface-2); }
.slls-sx-menu-item:disabled { opacity: 0.45; cursor: not-allowed; }
.slls-sx-menu-item svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }
.slls-sx-menu-item.danger, .slls-sx-menu-item.danger svg { color: var(--ui-danger-text); }
.slls-sx-menu-item input { margin: 0; accent-color: var(--ui-accent); }
.slls-sx-menu-sep { height: 1px; margin: 4px 6px; background: var(--ui-border); }

.slls-sx-tree { flex: 1 1 0; min-height: 0; overflow: auto; position: relative; }
.slls-sx-tree-spacer { width: 1px; }
.slls-sx-row {
    position: absolute; left: 0; right: 0;
    display: flex; align-items: center; gap: 5px; height: 26px; padding-right: 10px; cursor: pointer;
    user-select: none; font-size: 12.5px; white-space: nowrap;
}
.slls-sx-row:hover { background: var(--ui-surface-2); }
.slls-sx-row.selected { background: var(--ui-accent-soft); }
.slls-sx-row.flash { box-shadow: inset 0 0 0 1px var(--sx-warn); }
.slls-sx-caret {
    width: 18px; height: 18px; display: inline-flex; align-items: center; justify-content: center;
    color: var(--ui-text-tertiary); flex-shrink: 0; transition: transform 120ms ease;
}
.slls-sx-caret svg { width: 11px; height: 11px; }
.slls-sx-caret.open { transform: rotate(90deg); }
.slls-sx-ricon { display: inline-flex; color: var(--ui-text-secondary); flex-shrink: 0; }
.slls-sx-ricon svg { width: 14px; height: 14px; }
.slls-sx-rlabel { overflow: hidden; text-overflow: ellipsis; font-weight: 500; min-width: 0; }
.slls-sx-row.muted .slls-sx-rlabel { color: var(--ui-text-tertiary); font-style: italic; }
.slls-sx-row.translated .slls-sx-rlabel { color: var(--ui-accent); }
.slls-sx-row.deleted .slls-sx-rlabel { text-decoration: line-through; color: var(--ui-danger-text); }
.slls-sx-row.new .slls-sx-rlabel { color: var(--sx-success); }
.slls-sx-row.folder .slls-sx-rlabel { font-weight: 600; }
.slls-sx-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--sx-warn); flex-shrink: 0; }
.slls-sx-trail { margin-left: auto; padding-left: 8px; color: var(--ui-text-tertiary); font-size: 11.5px; font-variant-numeric: tabular-nums; }
.slls-sx-err { color: var(--ui-danger-text); display: inline-flex; }
.slls-sx-err svg { width: 12px; height: 12px; }
.slls-sx-empty { padding: 14px; color: var(--ui-text-tertiary); font-size: 12.5px; text-align: center; }

.slls-sx-drag { width: 10px; flex-shrink: 0; cursor: col-resize; position: relative; }
.slls-sx-drag::after {
    content: ""; position: absolute; top: 0; bottom: 0; left: 4px; width: 2px; border-radius: 2px;
    background: transparent; transition: background 120ms ease;
}
.slls-sx-drag:hover::after, .slls-sx-drag.active::after { background: var(--ui-accent); }

/* Detail pane */
.slls-sx-detail {
    flex: 1 1 0; min-width: 0; display: flex; flex-direction: column; min-height: 0;
    border: 1px solid var(--ui-border); border-radius: 0 var(--sx-radius) var(--sx-radius) 0;
    background: var(--ui-bg-tertiary);
}
.slls-sx-detail-head {
    display: flex; align-items: center; gap: 10px; padding: 12px 14px; border-bottom: 1px solid var(--ui-border);
    background: var(--ui-bg);
    border-radius: 0 var(--sx-radius) 0 0;
}
.slls-sx-objicon {
    width: 34px; height: 34px; border-radius: 9px; display: inline-flex; align-items: center; justify-content: center;
    background: var(--ui-accent-soft); color: var(--ui-accent); flex-shrink: 0;
}
.slls-sx-objicon svg { width: 17px; height: 17px; }
.slls-sx-objtitle { display: flex; flex-direction: column; min-width: 0; line-height: 1.25; }
.slls-sx-objname { font-size: 14px; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-objtype { font-size: 12px; color: var(--ui-text-secondary); }
.slls-sx-pill {
    display: inline-flex; align-items: center; height: 18px; padding: 0 7px; margin-left: 6px; border-radius: 999px;
    font-size: 10.5px; font-weight: 600; vertical-align: 1px;
    background: var(--ui-surface-2); border: 1px solid var(--ui-border); color: var(--ui-text-secondary);
}
.slls-sx-pill.warn { background: var(--sx-warn-soft); border-color: transparent; color: var(--sx-warn); }
.slls-sx-pill.new { background: var(--sx-success-soft); border-color: transparent; color: var(--sx-success); }
.slls-sx-pill.danger { background: var(--ui-danger-bg); border-color: transparent; color: var(--ui-danger-text); }
.slls-sx-stats { display: flex; gap: 14px; margin-left: auto; padding-left: 10px; }
.slls-sx-stat { display: flex; align-items: baseline; gap: 4px; }
.slls-sx-stat b { font-size: 14px; font-weight: 600; font-variant-numeric: tabular-nums; }
.slls-sx-stat span { font-size: 10.5px; letter-spacing: 0.05em; text-transform: uppercase; color: var(--ui-text-tertiary); }
.slls-sx-detail-actions { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; padding: 8px 14px; border-bottom: 1px solid var(--ui-border); }
.slls-sx-detail-actions .slls-sx-propsearch { margin-left: auto; width: 210px; }
.slls-sx-detail-body { flex: 1 1 0; min-height: 0; overflow: auto; padding: 12px 14px 18px; display: flex; flex-direction: column; gap: 10px; }

.slls-sx-card { border: 1px solid var(--ui-border); border-radius: var(--sx-radius); background: var(--ui-bg); }
.slls-sx-card-head { display: flex; align-items: center; gap: 6px; padding-right: 12px; }
.slls-sx-card-toggle {
    appearance: none; flex: 1 1 auto; min-width: 0; display: flex; align-items: center; gap: 6px;
    padding: 9px 0 9px 12px; border: none; background: transparent; color: var(--ui-text-tertiary);
    cursor: pointer; text-align: left; font-family: inherit;
    font-size: 11px; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase;
}
.slls-sx-card-toggle:hover { color: var(--ui-text); }
/* Full-width controls keep the shared press darkening but not its shrink: a
   wide button scaled about its centre pulls its edges (where the caret sits)
   out from under the pointer, so the release misses and the click is lost. */
.slls-sx button.slls-sx-card-toggle:not(:disabled):active,
.slls-sx button.slls-sx-menu-item:not(:disabled):active,
.slls-sx button.slls-sx-typebtn:not(:disabled):active { transform: none; }
.slls-sx-card-toggle .slls-sx-caret { width: 14px; height: 14px; }
.slls-sx-card-toggle .slls-sx-count { text-transform: none; letter-spacing: 0; font-weight: 500; }
.slls-sx-card-actions { margin-left: auto; display: flex; align-items: center; gap: 6px; }
.slls-sx-card-body { padding: 0 12px 11px; }
.slls-sx-props { display: grid; grid-template-columns: minmax(130px, 34%) 1fr; gap: 3px 12px; align-items: center; }
.slls-sx-plabel {
    font-size: 12.5px; color: var(--ui-text-secondary); display: flex; align-items: center; gap: 5px;
    min-height: 28px; overflow: hidden;
}
.slls-sx-plabel span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-pvalue { min-width: 0; font-size: 12.5px; overflow-wrap: anywhere; min-height: 28px; display: flex; align-items: center; }
.slls-sx-pvalue.long { white-space: pre-wrap; }
.slls-sx-pvalue .none { color: var(--ui-text-tertiary); font-style: italic; }
.slls-sx-pvalue .yes { color: var(--sx-success); font-weight: 500; }
.slls-sx-pvalue .no { color: var(--ui-text-tertiary); }
.slls-sx-pvalue.error { color: var(--ui-danger-text); }
.slls-sx-pvalue .slls-sx-input, .slls-sx-pvalue .slls-sx-select, .slls-sx-pvalue .slls-sx-textarea { padding: 4px 8px; }
.slls-sx-pvalue a, .slls-sx-link { color: var(--ui-accent); cursor: pointer; text-decoration: none; }
.slls-sx-pvalue a:hover, .slls-sx-link:hover { text-decoration: underline; }

.slls-sx-rel {
    display: flex; align-items: center; gap: 10px; flex-wrap: wrap; padding: 12px; margin-bottom: 2px;
    border: 1px solid var(--ui-border); border-radius: var(--sx-radius); background: var(--ui-bg);
}
.slls-sx-rel-end { display: flex; flex-direction: column; min-width: 0; flex: 1 1 160px; }
.slls-sx-rel-end b { font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-rel-end span { font-size: 12px; color: var(--ui-text-secondary); }
.slls-sx-rel-mid { display: flex; flex-direction: column; align-items: center; color: var(--ui-text-tertiary); font-size: 11.5px; font-weight: 600; }
.slls-sx-rel-mid svg { width: 18px; height: 18px; }

/* Code editor */
.slls-sx-code {
    position: relative; border: 1px solid var(--ui-border-strong); border-radius: var(--sx-radius-sm);
    background: var(--ui-bg-tertiary); overflow: auto; min-height: 64px; max-height: 520px;
}
.slls-sx-code-inner { position: relative; min-height: 100%; }
.slls-sx-modal .slls-sx-code { max-height: none; }
.slls-sx-code.focus { border-color: var(--ui-accent); box-shadow: 0 0 0 3px var(--ui-accent-soft); }
.slls-sx-code pre, .slls-sx-code textarea {
    margin: 0; padding: 10px 12px; font-family: var(--sx-mono); font-size: 12px; line-height: 1.55;
    white-space: pre-wrap; overflow-wrap: anywhere; tab-size: 4; border: none; letter-spacing: normal;
}
.slls-sx-code pre { color: var(--ui-text); pointer-events: none; min-height: 100%; }
.slls-sx-code textarea {
    position: absolute; inset: 0; width: 100%; height: 100%; resize: none; outline: none;
    background: transparent; color: transparent; caret-color: var(--ui-text); overflow: hidden;
}
.slls-sx-code textarea::selection { background: var(--ui-accent-soft); color: transparent; }
.slls-sx-code.readonly textarea { display: none; }
.slls-sx-code.readonly pre { pointer-events: auto; user-select: text; }
.slls-sx-code-empty { color: var(--ui-text-tertiary); font-style: italic; }
.slls-sx-tk-keyword, .slls-sx-tk-function { color: var(--ui-syntax-keyword); }
.slls-sx-tk-variable { color: var(--ui-syntax-variable); }
.slls-sx-tk-number { color: var(--ui-syntax-number); }
.slls-sx-tk-virtual_column { color: var(--ui-syntax-virtual-column); }
.slls-sx-tk-string { color: var(--ui-syntax-string); }
.slls-sx-tk-comment { color: var(--ui-syntax-comment); font-style: italic; }
.slls-sx-tk-operator, .slls-sx-tk-punctuation { color: var(--ui-syntax-operator); }
.slls-sx-code-error { margin-top: 6px; font-size: 12px; color: var(--ui-danger-text); }

/* Key / value lists */
.slls-sx-kv { display: grid; grid-template-columns: minmax(120px, 34%) 1fr auto; gap: 4px 10px; align-items: center; }
.slls-sx-kv .k { font-size: 12.5px; color: var(--ui-text-secondary); overflow-wrap: anywhere; }
.slls-sx-kv .v { font-size: 12px; font-family: var(--sx-mono); overflow-wrap: anywhere; white-space: pre-wrap; }
.slls-sx-kv-empty { font-size: 12.5px; color: var(--ui-text-tertiary); font-style: italic; }
.slls-sx-checks { display: flex; flex-wrap: wrap; gap: 6px; }
.slls-sx-check {
    display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; border-radius: 999px; font-size: 12.5px;
    border: 1px solid var(--ui-border-strong); background: var(--ui-bg); color: var(--ui-text-secondary); cursor: default;
}
.slls-sx-check.on { border-color: var(--ui-accent); background: var(--ui-accent-soft); color: var(--ui-accent); }
.slls-sx-check.editable { cursor: pointer; }
.slls-sx-check input { margin: 0; accent-color: var(--ui-accent); }
.slls-sx-grid { width: 100%; border-collapse: collapse; font-size: 12.5px; }
.slls-sx-grid th {
    text-align: left; font-size: 10.5px; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase;
    color: var(--ui-text-tertiary); padding: 4px 6px; border-bottom: 1px solid var(--ui-border);
}
.slls-sx-grid td { padding: 4px 6px; border-bottom: 1px solid var(--ui-border); vertical-align: middle; overflow-wrap: anywhere; }
.slls-sx-grid tr:last-child td { border-bottom: none; }
.slls-sx-sublist { display: flex; flex-direction: column; gap: 2px; }
.slls-sx-subitem {
    display: flex; align-items: center; gap: 8px; padding: 5px 8px; border-radius: 6px; cursor: pointer; font-size: 12.5px;
}
.slls-sx-subitem:hover { background: var(--ui-surface-2); }
.slls-sx-subitem svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }
.slls-sx-subitem .meta { margin-left: auto; color: var(--ui-text-tertiary); font-size: 11.5px; }
.slls-sx-rls { display: flex; flex-direction: column; gap: 10px; }
.slls-sx-rls-head { display: flex; align-items: center; gap: 6px; font-size: 12.5px; font-weight: 600; margin-bottom: 4px; }
.slls-sx-rls-head svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }

/* Status, modal, toast */
.slls-sx-status { padding: 8px 18px; font-size: 12.5px; border-top: 1px solid var(--ui-border); display: none; align-items: center; gap: 10px; }
.slls-sx-status.show { display: flex; }
.slls-sx-status.success { background: var(--sx-success-soft); color: var(--sx-success); }
.slls-sx-status.error { background: var(--ui-danger-bg); color: var(--ui-danger-text); }
.slls-sx-status.info { background: var(--ui-surface-2); color: var(--ui-text-secondary); }
.slls-sx-status span { flex: 1; min-width: 0; overflow-wrap: anywhere; }
.slls-sx-footer { padding: 6px 18px 10px; }
.slls-sx-overlay {
    position: absolute; inset: 0; z-index: 50; display: flex; align-items: center; justify-content: center;
    background: rgba(0, 0, 0, 0.32); padding: 24px;
}
.slls-sx-modal {
    width: min(760px, 100%); max-height: 100%; display: flex; flex-direction: column;
    background: var(--ui-bg-solid); border: 1px solid var(--ui-border-strong); border-radius: 14px;
    box-shadow: var(--ui-shadow-lg); overflow: hidden;
}
.slls-sx-modal.wide { width: min(1100px, 100%); height: 100%; }
.slls-sx-modal-head { display: flex; align-items: center; gap: 10px; padding: 12px 16px; border-bottom: 1px solid var(--ui-border); }
.slls-sx-modal-head svg { width: 16px; height: 16px; }
.slls-sx-modal-title { font-size: 14px; font-weight: 600; flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-modal-body { flex: 1 1 auto; min-height: 0; overflow: auto; padding: 14px 16px; }
.slls-sx-modal-foot { display: flex; justify-content: flex-end; gap: 8px; padding: 10px 16px; border-top: 1px solid var(--ui-border); }
.slls-sx-modal-body pre.slls-sx-tmdl {
    margin: 0; font-family: var(--sx-mono); font-size: 12px; line-height: 1.55; white-space: pre; tab-size: 4;
}
.slls-sx-modal .slls-sx-code { height: 100%; }
.slls-sx-modal .slls-sx-code textarea { overflow: auto; }
.slls-sx-tabs { display: flex; gap: 4px; padding: 0 16px; border-bottom: 1px solid var(--ui-border); }
.slls-sx-tab {
    appearance: none; border: none; background: none; padding: 9px 10px; font-size: 12.5px; font-weight: 500;
    color: var(--ui-text-secondary); cursor: pointer; border-bottom: 2px solid transparent; margin-bottom: -1px;
}
.slls-sx-tab.active { color: var(--ui-accent); border-bottom-color: var(--ui-accent); }
.slls-sx-dep-row { display: flex; align-items: center; gap: 6px; height: 26px; font-size: 12.5px; white-space: nowrap; }
.slls-sx-dep-row .slls-sx-ricon svg { width: 14px; height: 14px; }
.slls-sx-dep-row .meta { color: var(--ui-text-tertiary); font-size: 11.5px; }
.slls-sx-change { display: flex; align-items: flex-start; gap: 10px; padding: 8px 0; border-bottom: 1px solid var(--ui-border); }
.slls-sx-change:last-child { border-bottom: none; }
.slls-sx-change-body { flex: 1; min-width: 0; }
.slls-sx-change-title { font-size: 12.5px; font-weight: 500; }
.slls-sx-change-detail { font-size: 12px; color: var(--ui-text-secondary); margin-top: 2px; overflow-wrap: anywhere; white-space: pre-wrap; max-height: 72px; overflow: hidden; }
.slls-sx-section-title {
    font-size: 11px; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; color: var(--ui-text-tertiary);
    margin: 4px 0 6px;
}
.slls-sx-section-title:not(:first-child) { margin-top: 18px; }
.slls-sx-ctx { position: fixed; z-index: 2147483600; }
.slls-sx-counts .sep { margin: 0 3px; opacity: 0.6; }

/* Field parameter manager */
.slls-sx-modal.slls-sx-fpm-box { width: 100%; height: 100%; }
.slls-sx-modal .slls-sx-fpm-body { padding: 0; overflow: hidden; display: flex; }
.slls-sx-fpm { display: flex; flex: 1 1 auto; min-width: 0; min-height: 0; }
.slls-sx-fpm-side {
    width: 260px; flex-shrink: 0; display: flex; flex-direction: column; min-height: 0;
    border-right: 1px solid var(--ui-border);
}
.slls-sx-fpm-side-head {
    display: flex; align-items: center; justify-content: space-between; gap: 6px;
    padding: 8px 10px 8px 14px; border-bottom: 1px solid var(--ui-border);
}
.slls-sx-fpm-list { flex: 1 1 0; min-height: 0; overflow: auto; padding: 6px; display: flex; flex-direction: column; gap: 2px; }
.slls-sx-fpm-item {
    appearance: none; width: 100%; display: flex; align-items: center; gap: 8px; padding: 6px 8px; flex-shrink: 0;
    border: 1px solid transparent; border-radius: 8px; background: transparent; color: var(--ui-text);
    font: inherit; font-size: 12.5px; text-align: left; cursor: pointer;
}
.slls-sx-fpm-item svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }
.slls-sx-fpm-item:hover { background: var(--ui-surface-2); }
.slls-sx-fpm-item.active { background: var(--ui-accent-soft); }
.slls-sx-fpm-item.draft { border: 1px dashed var(--ui-accent); color: var(--ui-accent); }
.slls-sx-fpm-item.draft svg { color: var(--ui-accent); }
.slls-sx-fpm-item.new .name { color: var(--sx-success); }
.slls-sx-fpm-item .name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 500; }
.slls-sx-fpm-item .count { font-size: 11px; color: var(--ui-text-tertiary); font-variant-numeric: tabular-nums; }
.slls-sx-fpm-main { flex: 1 1 auto; min-width: 0; min-height: 0; display: flex; flex-direction: column; }
.slls-sx-fpm-toolbar {
    display: flex; align-items: center; gap: 10px; flex-wrap: wrap; padding: 10px 16px;
    border-bottom: 1px solid var(--ui-border);
}
.slls-sx-fpm-title-icon { display: inline-flex; color: var(--ui-accent); }
.slls-sx-fpm-title-icon svg { width: 17px; height: 17px; }
.slls-sx-fpm-name { width: 260px; }
.slls-sx-fpm-name-text { font-size: 14px; font-weight: 600; }
.slls-sx .slls-sx-input.invalid { border-color: var(--ui-danger); }
.slls-sx-fpm-status { font-size: 12px; }
.slls-sx-fpm-status.success { color: var(--sx-success); }
.slls-sx-fpm-status.error { color: var(--ui-danger-text); }
.slls-sx-fpm-stage { margin-left: auto; }
.slls-sx-fpm-grid {
    flex: 1 1 0; min-height: 0; display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
    gap: 12px; padding: 14px 16px;
}
.slls-sx-fpm-pane {
    display: flex; flex-direction: column; gap: 8px; min-height: 0; padding: 10px;
    border: 1px solid var(--ui-border); border-radius: var(--sx-radius); background: var(--ui-bg);
}
.slls-sx-fpm-pane-head { display: flex; align-items: center; gap: 8px; }
.slls-sx-fpm-link { margin-left: auto; display: inline-flex; align-items: center; gap: 4px; font-size: 12px; font-weight: 500; }
.slls-sx-fpm-link svg { width: 12px; height: 12px; }
.slls-sx-fpm-note { font-size: 11.5px; line-height: 1.4; color: var(--ui-text-secondary); }
.slls-sx-fpm-note.error { color: var(--ui-danger-text); font-weight: 500; }
.slls-sx-fpm-scroll { flex: 1 1 0; min-height: 0; overflow: auto; display: flex; flex-direction: column; gap: 3px; }
.slls-sx-fpm-row {
    appearance: none; width: 100%; display: flex; align-items: center; gap: 6px; padding: 5px 8px; flex-shrink: 0;
    border: none; border-radius: 6px; background: transparent; color: var(--ui-text);
    font: inherit; font-size: 12.5px; text-align: left; cursor: pointer;
}
.slls-sx-fpm-row:hover:not(:disabled) { background: var(--ui-surface-2); }
.slls-sx-fpm-row svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }
.slls-sx-fpm-row .slls-sx-caret { width: 16px; height: 16px; }
.slls-sx-fpm-row.table .name { font-weight: 500; }
.slls-sx-fpm-row.item { padding-left: 30px; }
.slls-sx-fpm-row .name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-fpm-row .meta { font-size: 11.5px; color: var(--ui-text-tertiary); }
.slls-sx-fpm-row.added { cursor: default; }
.slls-sx-fpm-row.added .name { color: var(--ui-text-tertiary); }
.slls-sx-fpm-obj {
    display: flex; align-items: center; gap: 6px; padding: 5px 6px; flex-shrink: 0;
    border: 1px solid var(--ui-border); border-radius: 8px; background: var(--ui-bg-tertiary);
}
.slls-sx-fpm-obj > .slls-sx-ricon svg { width: 14px; height: 14px; }
.slls-sx-fpm-obj .idx { width: 18px; text-align: center; font-size: 11px; color: var(--ui-text-tertiary); font-variant-numeric: tabular-nums; }
.slls-sx-fpm-obj .main { flex: 1; min-width: 0; display: flex; flex-direction: column; }
.slls-sx-fpm-label {
    min-width: 0; padding: 2px 5px; border: 1px solid transparent; border-radius: 5px; background: transparent;
    color: var(--ui-text); font: inherit; font-size: 12.5px; font-weight: 500; outline: none;
}
.slls-sx-fpm-label:hover, .slls-sx-fpm-label:focus { border-color: var(--ui-border-strong); background: var(--ui-bg); }
.slls-sx-fpm-obj .ref {
    padding: 0 5px; font-family: var(--sx-mono); font-size: 11px; color: var(--ui-text-tertiary);
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.slls-sx-fpm-obj .ref.broken { color: var(--ui-danger-text); }
.slls-sx .slls-sx-fpm-hier { width: 120px; flex-shrink: 0; padding: 4px 7px; }
.slls-sx-fpm-readonly { flex: 1 1 0; min-height: 0; overflow: auto; padding: 14px 16px; display: flex; flex-direction: column; gap: 6px; }
.slls-sx-fpm-info {
    display: flex; align-items: center; gap: 8px; margin-bottom: 6px; padding: 8px 12px; font-size: 12.5px;
    border: 1px solid var(--ui-border); border-radius: 8px; background: var(--ui-surface-2); color: var(--ui-text-secondary);
}
.slls-sx-fpm-info svg { width: 15px; height: 15px; }
.slls-sx-fpm-obj.readonly { gap: 10px; padding: 8px 10px; }
.slls-sx-fpm-obj.readonly .label { flex: 1; min-width: 0; font-size: 13px; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-fpm-obj.readonly .ref { padding: 0; font-size: 11.5px; max-width: 45%; }
.slls-sx-fpm-main .slls-sx-center a { font-size: 12.5px; }

/* DAXLib package manager */
.slls-sx-modal .slls-sx-dlm-body { padding: 0; overflow: hidden; display: flex; flex-direction: column; }
.slls-sx-dlm-toolbar {
    display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
    padding: 8px 16px; border-bottom: 1px solid var(--ui-border);
}
.slls-sx-dlm-toolbar .slls-sx-search { width: 320px; max-width: 100%; }
.slls-sx-dlm-toolbar .slls-sx-select { width: 180px; padding: 6px 8px; }
.slls-sx-dlm-scroll { flex: 1 1 0; min-height: 0; overflow: auto; }
.slls-sx-dlm-wrap { max-width: 860px; margin: 0 auto; padding: 14px 16px 18px; display: flex; flex-direction: column; gap: 12px; }
.slls-sx-dlm-wrap .slls-sx-tabs { padding: 0; }
.slls-sx-tab { display: inline-flex; align-items: center; gap: 6px; }
.slls-sx-tab svg { width: 14px; height: 14px; }
.slls-sx-tab .slls-sx-pill { margin-left: 0; }
.slls-sx-dlm-list { display: flex; flex-direction: column; border: 1px solid var(--ui-border); border-radius: 10px; overflow: hidden; }
.slls-sx-dlm-row {
    display: flex; align-items: flex-start; gap: 10px; padding: 10px 12px;
    background: var(--ui-bg); border-bottom: 1px solid var(--ui-border);
}
.slls-sx-dlm-row:last-child { border-bottom: none; }
.slls-sx-dlm-row.installed { align-items: center; }
.slls-sx-dlm-open {
    appearance: none; flex: 1 1 auto; min-width: 0; display: flex; align-items: flex-start; gap: 10px;
    padding: 2px 4px; margin: -2px -4px; border: none; border-radius: 6px; background: transparent;
    color: inherit; font: inherit; text-align: left; cursor: pointer; transform: none !important;
}
.slls-sx-dlm-row.installed .slls-sx-dlm-open { align-items: center; }
.slls-sx-dlm-open:hover:not(:disabled) { background: var(--ui-surface-2); }
.slls-sx-dlm-open:disabled { opacity: 0.55; cursor: progress; }
.slls-sx-dlm-icon { width: 20px; height: 20px; flex-shrink: 0; display: inline-flex; align-items: center; justify-content: center; color: var(--ui-text-secondary); }
.slls-sx-dlm-icon svg { width: 16px; height: 16px; }
.slls-sx-dlm-icon img { width: 20px; height: 20px; border-radius: 4px; object-fit: contain; }
.slls-sx-dlm-main { flex: 1 1 auto; min-width: 0; display: flex; flex-direction: column; gap: 3px; }
.slls-sx-dlm-title { display: flex; align-items: center; gap: 6px; min-width: 0; }
.slls-sx-dlm-id { font-family: var(--sx-mono); font-size: 12.5px; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-dlm-row.installed .slls-sx-dlm-id { flex: 1 1 auto; }
.slls-sx-dlm-desc {
    font-size: 12px; line-height: 1.45; color: var(--ui-text-secondary);
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
}
.slls-sx-dlm-meta { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 14px; font-size: 11.5px; color: var(--ui-text-tertiary); }
.slls-sx-dlm-meta span { display: inline-flex; align-items: center; gap: 4px; }
.slls-sx-dlm-meta svg { width: 12px; height: 12px; }
.slls-sx-dlm-tags { display: flex; flex-wrap: wrap; gap: 4px; }
.slls-sx-dlm-tag {
    padding: 0 6px; border: 1px solid var(--ui-border); border-radius: 5px; background: var(--ui-surface-2);
    font-size: 10.5px; line-height: 17px; color: var(--ui-text-secondary);
}
.slls-sx-dlm-side { display: flex; flex-direction: column; align-items: flex-end; gap: 5px; flex-shrink: 0; padding-left: 6px; }
.slls-sx-dlm-ver {
    padding: 0 6px; border-radius: 5px; background: var(--ui-surface-2); font-size: 11px; line-height: 18px;
    color: var(--ui-text-secondary); font-variant-numeric: tabular-nums; white-space: nowrap; flex-shrink: 0;
}
.slls-sx-dlm-count { font-size: 11.5px; color: var(--ui-text-tertiary); white-space: nowrap; flex-shrink: 0; }
.slls-sx-dlm-dl { display: inline-flex; align-items: center; gap: 4px; font-size: 11.5px; color: var(--ui-text-tertiary); }
.slls-sx-dlm-dl svg { width: 12px; height: 12px; }
.slls-sx-dlm-owners { display: flex; gap: 4px; }
.slls-sx-dlm-owners a { display: inline-flex; color: var(--ui-text-tertiary); font-size: 11px; text-decoration: none; }
.slls-sx-dlm-owners a:hover { color: var(--ui-text); }
.slls-sx-dlm-owners img { width: 16px; height: 16px; border-radius: 50%; object-fit: cover; }
.slls-sx-dlm-ok { display: inline-flex; align-items: center; gap: 4px; font-size: 11.5px; color: var(--sx-success); flex-shrink: 0; }
.slls-sx-dlm-ok svg { width: 13px; height: 13px; }
.slls-sx-dlm-card {
    display: flex; flex-direction: column; gap: 10px; padding: 14px 16px;
    border: 1px solid var(--ui-border); border-radius: 10px; background: var(--ui-bg);
}
.slls-sx-dlm-card-head { display: flex; flex-wrap: wrap; align-items: flex-start; justify-content: space-between; gap: 10px; }
.slls-sx-dlm-name { font-size: 15px; font-weight: 600; }
.slls-sx-dlm-by { font-size: 12px; color: var(--ui-text-secondary); }
.slls-sx-dlm-actions { display: flex; align-items: center; gap: 8px; }
.slls-sx-dlm-actions label { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; font-weight: 500; color: var(--ui-text-secondary); }
.slls-sx-dlm-actions .slls-sx-select { width: auto; padding: 5px 8px; }
.slls-sx-dlm-text { margin: 0; font-size: 12.5px; line-height: 1.5; color: var(--ui-text-secondary); }
.slls-sx-dlm-note { margin: 0; font-size: 11.5px; line-height: 1.45; color: var(--ui-text-tertiary); }
.slls-sx-dlm-note code { padding: 0 4px; border-radius: 4px; background: var(--ui-surface-2); font-family: var(--sx-mono); }
.slls-sx-dlm-status { font-size: 12px; }
.slls-sx-dlm-status.success { color: var(--sx-success); }
.slls-sx-dlm-status.error { color: var(--ui-danger-text); }
.slls-sx-dlm-fn { border-bottom: 1px solid var(--ui-border); background: var(--ui-bg); }
.slls-sx-dlm-fn:last-child { border-bottom: none; }
.slls-sx-dlm-fn-head {
    appearance: none; width: 100%; display: flex; align-items: center; gap: 8px; padding: 8px 12px;
    border: none; background: transparent; color: inherit; font: inherit; text-align: left; cursor: pointer; transform: none !important;
}
.slls-sx-dlm-fn-head:hover { background: var(--ui-surface-2); }
.slls-sx-dlm-fn-head svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }
.slls-sx-dlm-fn-head .slls-sx-caret { width: 16px; height: 16px; }
.slls-sx-dlm-fn-head .slls-sx-caret svg { width: 11px; height: 11px; color: var(--ui-text-tertiary); }
.slls-sx-dlm-fn-head .name { flex: 1; min-width: 0; font-family: var(--sx-mono); font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-dlm-fn pre {
    margin: 0; max-height: 320px; overflow: auto; padding: 10px 12px; border-top: 1px solid var(--ui-border);
    background: var(--ui-bg-tertiary); font-family: var(--sx-mono); font-size: 12px; line-height: 1.55; white-space: pre; tab-size: 4;
}
.slls-sx-dlm-section { font-size: 11px; font-weight: 600; letter-spacing: 0.04em; text-transform: uppercase; color: var(--ui-text-secondary); }
.slls-sx-dlm-foot { font-size: 11.5px; color: var(--ui-text-tertiary); }
.slls-sx-dlm-error { display: flex; flex-direction: column; align-items: center; gap: 8px; padding: 40px 16px; text-align: center; color: var(--ui-danger-text); font-size: 12.5px; }
.slls-sx-dlm-error svg { width: 26px; height: 26px; }
.slls-sx-dlm-browse { display: inline-flex; align-items: center; gap: 5px; text-decoration: none; }
.slls-sx-dlm-browse svg { width: 13px; height: 13px; }

/* Shared tool windows */
.slls-sx-modal .slls-sx-tool-body { padding: 0; overflow: hidden; display: flex; flex-direction: column; }
.slls-sx-tool .slls-sx-modal-title { flex: 0 1 auto; }
.slls-sx-modal-sub {
    flex: 1 1 auto; min-width: 0; font-size: 12px; color: var(--ui-text-secondary);
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.slls-sx-tool-pending { margin-left: 0; height: 22px; }
.slls-sx-tool-scroll { flex: 1 1 0; min-height: 0; overflow: auto; }
.slls-sx-tool-wrap { max-width: 920px; margin: 0 auto; padding: 14px 16px 18px; display: flex; flex-direction: column; gap: 10px; }
.slls-sx-tool-card {
    display: flex; flex-direction: column; gap: 10px; padding: 14px 16px;
    border: 1px solid var(--ui-border); border-radius: 10px; background: var(--ui-bg);
}
.slls-sx-tool-card .slls-sx-section-title { margin: 0; }
.slls-sx-tool-field { display: flex; flex-direction: column; gap: 4px; min-width: 0; font-size: 11.5px; font-weight: 500; color: var(--ui-text-secondary); }
.slls-sx-tool-field > span { display: inline-flex; align-items: center; gap: 5px; }
.slls-sx-tool-field .slls-sx-select, .slls-sx-tool-field .slls-sx-input { font-size: 12.5px; padding: 6px 8px; }
.slls-sx-tool-toolbar {
    display: flex; align-items: center; gap: 8px; flex-wrap: wrap; flex-shrink: 0;
    padding: 8px 16px; border-bottom: 1px solid var(--ui-border);
}
.slls-sx-tool-toolbar .slls-sx-search { width: 260px; max-width: 100%; }
.slls-sx-tool-toolbar .slls-sx-tool-field { flex-direction: row; align-items: center; gap: 6px; }
.slls-sx-tool-toolbar .slls-sx-tool-field .slls-sx-select { width: auto; min-width: 120px; }
.slls-sx-tool-typefilter { width: 190px; }
.slls-sx-tool-typefilter .slls-sx-menu { min-width: 220px; right: auto; }
.slls-sx-tool-typefilter .slls-sx-menu-head { gap: 10px; }
.slls-sx-tool-typefilter .slls-sx-menu-head span { flex: 1; }
.slls-sx-tool-count { margin-left: auto; font-size: 12px; color: var(--ui-text-tertiary); font-variant-numeric: tabular-nums; }
.slls-sx-tool-caret {
    appearance: none; width: 18px; height: 18px; flex-shrink: 0; display: inline-flex; align-items: center; justify-content: center;
    padding: 0; border: none; border-radius: 4px; background: transparent; color: var(--ui-text-tertiary); cursor: pointer;
}
.slls-sx-tool-caret:hover { background: var(--ui-surface-2); color: var(--ui-text); }
.slls-sx-tool-caret .slls-sx-caret svg { width: 9px; height: 9px; }
.slls-sx-tool-check { width: 14px; height: 14px; margin: 0; flex-shrink: 0; accent-color: var(--ui-accent); display: inline-block; }
.slls-sx-tool-hidden { display: inline-flex; color: var(--ui-text-tertiary); }
.slls-sx-tool-hidden svg { width: 13px; height: 13px; }
.slls-sx-tool-error {
    display: flex; align-items: center; gap: 8px; margin: 8px 16px 0; padding: 8px 12px; font-size: 12.5px;
    border: 1px solid var(--ui-danger-border); border-radius: 8px; background: var(--ui-danger-bg); color: var(--ui-danger-text);
}
.slls-sx-tool-error svg { width: 15px; height: 15px; flex-shrink: 0; }
.slls-sx-tool-label { font-size: 11.5px; font-weight: 600; color: var(--ui-text-secondary); }
.slls-sx-mono { font-family: var(--sx-mono); font-size: 12px; }
.slls-sx-btn.on { border-color: var(--ui-accent); color: var(--ui-accent); background: var(--ui-accent-soft); }
.slls-sx-iconbtn.danger:hover:not(:disabled) { color: var(--ui-danger-text); background: var(--ui-danger-bg); }
.slls-sx-tool .slls-sx-dot { display: inline-block; }

/* Relationship manager */
.slls-sx-rm-form { display: flex; flex-direction: column; gap: 8px; }
.slls-sx-rm-side { display: grid; grid-template-columns: 44px minmax(0, 1fr) minmax(0, 1.3fr) 120px; align-items: end; gap: 10px; }
.slls-sx-rm-side-label { padding-bottom: 8px; font-size: 12px; font-weight: 600; color: var(--ui-text-secondary); }
.slls-sx-rm-types { font-size: 12px; color: var(--ui-text-secondary); }
.slls-sx-rm-types.warn { color: var(--sx-warn); font-weight: 500; }
.slls-sx-rm-bottom { display: flex; align-items: flex-end; gap: 14px; flex-wrap: wrap; }
.slls-sx-rm-bottom .slls-sx-tool-field:first-child { width: 220px; }
.slls-sx-rm-bottom .slls-sx-switch { height: 30px; }
.slls-sx-rm-bottom .slls-sx-btn { margin-left: auto; }
.slls-sx-rm-list { display: flex; flex-direction: column; gap: 6px; }
.slls-sx-rm-new {
    display: flex; align-items: center; gap: 10px; padding: 7px 10px; font-size: 12.5px;
    border: 1px solid var(--sx-warn); border-radius: 8px; background: var(--sx-warn-soft);
}
.slls-sx-rm-new .slls-sx-pill { margin-left: 0; }
.slls-sx-rm-new .name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-rm-card { border: 1px solid var(--ui-border); border-radius: 8px; background: var(--ui-bg); }
.slls-sx-rm-card.deleted .name { text-decoration: line-through; color: var(--ui-text-tertiary); }
.slls-sx-rm-card-head { display: flex; align-items: center; gap: 6px; padding: 4px 8px 4px 4px; }
.slls-sx-rm-toggle {
    appearance: none; flex: 1; min-width: 0; display: flex; align-items: center; gap: 7px; padding: 5px 6px;
    border: none; border-radius: 6px; background: transparent; color: inherit; font: inherit; font-size: 12.5px;
    text-align: left; cursor: pointer; transform: none !important;
}
.slls-sx-rm-toggle:hover { background: var(--ui-surface-2); }
.slls-sx-rm-toggle > span.slls-sx-ricon svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }
.slls-sx-rm-toggle .name { flex: 1; min-width: 0; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-rm-toggle .slls-sx-pill { margin-left: 0; }
.slls-sx-rm-grid {
    display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px 24px;
    padding: 10px 14px 12px; border-top: 1px solid var(--ui-border);
}
.slls-sx-rm-row { display: flex; align-items: center; gap: 10px; min-height: 30px; font-size: 12.5px; }
.slls-sx-rm-row .label { width: 180px; flex-shrink: 0; display: inline-flex; align-items: center; gap: 5px; color: var(--ui-text-secondary); }
.slls-sx-rm-row .val { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-rm-row .slls-sx-select { flex: 1; min-width: 0; padding: 5px 8px; font-size: 12.5px; }

/* Security manager */
.slls-sx-sm-role { align-items: flex-start; }
.slls-sx-sm-role > .slls-sx-ricon { padding-top: 2px; }
.slls-sx-sm-role-body { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 1px; }
.slls-sx-sm-role-top { display: flex; align-items: center; gap: 6px; min-width: 0; }
.slls-sx-sm-role-top .slls-sx-pill { margin-left: 0; }
.slls-sx-sm-role .meta { font-size: 11px; color: var(--ui-text-tertiary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-sm-role.deleted .name { text-decoration: line-through; color: var(--ui-text-tertiary); }
.slls-sx-sm-del {
    display: inline-flex; align-items: center; justify-content: center; width: 22px; height: 22px; flex-shrink: 0;
    border-radius: 5px; color: var(--ui-text-tertiary); opacity: 0;
}
.slls-sx-sm-role:hover .slls-sx-sm-del, .slls-sx-sm-role.deleted .slls-sx-sm-del, .slls-sx-sm-del:focus { opacity: 1; }
.slls-sx-sm-del:hover { background: var(--ui-danger-bg); color: var(--ui-danger-text); }
.slls-sx-sm-del svg { width: 13px; height: 13px; }
.slls-sx-sm-general { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
.slls-sx-sm-add { display: flex; gap: 8px; }
.slls-sx-sm-add .slls-sx-input { flex: 1; }
.slls-sx-sm-members { display: flex; flex-direction: column; border: 1px solid var(--ui-border); border-radius: 8px; overflow: hidden; }
.slls-sx-sm-member { display: flex; align-items: center; gap: 8px; padding: 5px 8px 5px 12px; font-size: 12.5px; border-bottom: 1px solid var(--ui-border); }
.slls-sx-sm-member:last-child { border-bottom: none; }
.slls-sx-sm-member svg { width: 14px; height: 14px; color: var(--ui-text-tertiary); }
.slls-sx-sm-member .name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-sm-sechead { display: flex; align-items: center; gap: 12px; margin-top: 8px; }
.slls-sx-sm-sechead .slls-sx-section-title { margin: 0; }
.slls-sx-sm-sechead .slls-sx-seg button .slls-sx-dot { margin-left: 2px; }
.slls-sx-sm-count { margin-left: auto; font-size: 12px; color: var(--ui-text-tertiary); }
.slls-sx-sm-rls, .slls-sx-sm-ols { display: flex; flex-direction: column; border: 1px solid var(--ui-border); border-radius: 8px; overflow: hidden; }
.slls-sx-sm-rls-row { display: flex; align-items: center; gap: 8px; padding: 5px 8px; border-bottom: 1px solid var(--ui-border); }
.slls-sx-sm-rls-row:last-child, .slls-sx-sm-ols-row:last-child { border-bottom: none; }
.slls-sx-sm-table { width: 220px; flex-shrink: 0; display: flex; align-items: center; gap: 6px; font-size: 12.5px; min-width: 0; }
.slls-sx-sm-table svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }
.slls-sx-sm-table .name { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-sm-rls-row .slls-sx-input { flex: 1; padding: 5px 8px; }
.slls-sx-sm-code {
    flex: 1; min-width: 0; font-family: var(--sx-mono); font-size: 12px; color: var(--ui-text-secondary);
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.slls-sx-sm-olsbar { display: flex; align-items: flex-end; gap: 10px; }
.slls-sx-sm-olsbar .slls-sx-tool-field { flex-direction: row; align-items: center; gap: 6px; }
.slls-sx-sm-olsbar .slls-sx-search { flex: 1; }
.slls-sx-sm-ols-row { display: flex; align-items: center; gap: 7px; padding: 4px 8px; font-size: 12.5px; border-bottom: 1px solid var(--ui-border); }
.slls-sx-sm-ols-row.col { padding-left: 34px; border-bottom-style: dashed; }
.slls-sx-sm-ols-row.empty { color: var(--ui-text-tertiary); font-style: italic; }
.slls-sx-sm-ols-row > .slls-sx-ricon svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }
.slls-sx-sm-ols-row .name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-sm-ols-row.table .name { font-weight: 500; }
.slls-sx-sm-ols-row .meta { font-size: 11px; color: var(--ui-text-tertiary); }
.slls-sx-sm-ols-row .slls-sx-select { width: 110px; padding: 3px 6px; font-size: 12px; }
.slls-sx-sm-perm { width: 110px; text-align: right; font-size: 12px; color: var(--ui-text-secondary); }

/* Description manager */
.slls-sx-dm-row {
    display: grid; grid-template-columns: minmax(240px, 38%) minmax(0, 1fr); align-items: start; gap: 10px;
    padding: 3px 16px 3px 8px; border-bottom: 1px solid var(--ui-border);
}
.slls-sx-dm-row.head {
    position: sticky; top: 0; z-index: 1; align-items: center; padding-top: 7px; padding-bottom: 7px;
    background: var(--ui-bg-secondary); font-size: 11.5px; font-weight: 600; color: var(--ui-text-secondary);
}
.slls-sx-dm-obj { display: flex; align-items: center; gap: 6px; min-width: 0; min-height: 28px; font-size: 12.5px; }
.slls-sx-dm-row.head .slls-sx-dm-obj { min-height: 0; padding-left: 32px; }
.slls-sx-dm-obj > .slls-sx-ricon svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }
.slls-sx-dm-obj .name { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-dm-text { min-height: 28px; padding: 4px 8px; font-size: 12.5px; }
.slls-sx-dm-text.changed { border-color: var(--ui-accent); }
.slls-sx-dm-desc { padding: 5px 0; font-size: 12.5px; color: var(--ui-text); line-height: 1.45; white-space: pre-wrap; }
.slls-sx-dm-desc.empty { color: var(--ui-text-tertiary); }

/* Translation manager */
.slls-sx-tm-table { table-layout: fixed; border-collapse: collapse; font-size: 12.5px; }
.slls-sx-tm-table th {
    position: sticky; top: 0; z-index: 1; padding: 7px 8px; text-align: left; font-size: 11.5px; font-weight: 600;
    color: var(--ui-text-secondary); background: var(--ui-bg-secondary); border-bottom: 1px solid var(--ui-border);
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.slls-sx-tm-th { display: inline-flex; align-items: center; gap: 8px; padding-left: 24px; }
.slls-sx-tm-table td { padding: 3px 6px; border-bottom: 1px solid var(--ui-border); vertical-align: middle; overflow: hidden; }
.slls-sx-tm-input { width: 100%; padding: 4px 7px; font-size: 12.5px; }
.slls-sx-tm-input:disabled { background: transparent; border-color: transparent; }
.slls-sx-tm-val, .slls-sx-tm-orig { display: block; padding: 4px 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-tm-orig { color: var(--ui-text-secondary); }
.slls-sx-tm-val.empty, .slls-sx-tm-orig.empty { color: var(--ui-text-tertiary); }
.slls-sx-modal.slls-sx-tm-dialog { width: min(560px, 100%); }
.slls-sx-tm-dialog .slls-sx-modal-body { display: flex; flex-direction: column; gap: 10px; }
.slls-sx-tm-dialog .slls-sx-section-title { margin: 4px 0 0; }
.slls-sx-tm-dhead { display: flex; align-items: center; gap: 6px; }
.slls-sx-tm-dhead .slls-sx-section-title { flex: 1; }
.slls-sx-tm-chips { display: flex; flex-wrap: wrap; gap: 6px; }
.slls-sx-tm-chip {
    appearance: none; padding: 3px 10px; border: 1px solid var(--ui-border-strong); border-radius: 999px;
    background: var(--ui-bg); color: var(--ui-text-secondary); font: inherit; font-size: 12px; cursor: pointer;
}
.slls-sx-tm-chip.on { border-color: var(--ui-accent); background: var(--ui-accent-soft); color: var(--ui-accent); font-weight: 600; }
.slls-sx-tm-props { display: flex; gap: 16px; font-size: 12.5px; }
.slls-sx-tm-props label { display: inline-flex; align-items: center; gap: 6px; }
.slls-sx-tm-props input { margin: 0; accent-color: var(--ui-accent); }

/* Relationship diagram */
.slls-sx-rd-toolbar { gap: 6px; }
.slls-sx-rd-chip {
    appearance: none; display: inline-flex; align-items: center; gap: 6px; height: 26px; padding: 0 9px;
    border: 1px solid var(--ui-border-strong); border-radius: 7px; background: var(--ui-bg); color: var(--ui-text-secondary);
    font: inherit; font-size: 12px; font-weight: 500; cursor: pointer;
}
.slls-sx-rd-chip:hover { color: var(--ui-text); background: var(--ui-surface-2); }
.slls-sx-rd-chip svg { width: 13px; height: 13px; }
.slls-sx-rd-chip .sw { width: 10px; height: 3px; border-radius: 2px; background: var(--chip); }
.slls-sx-rd-chip .count {
    min-width: 16px; padding: 0 5px; border-radius: 999px; background: var(--ui-surface-2);
    font-size: 10.5px; font-weight: 600; text-align: center; font-variant-numeric: tabular-nums;
}
.slls-sx-rd-chip.mm { --chip: var(--sx-hl-mm); } .slls-sx-rd-chip.bidi { --chip: var(--sx-hl-bidi); }
.slls-sx-rd-chip.lim { --chip: var(--sx-hl-lim); } .slls-sx-rd-chip.nonint { --chip: var(--sx-hl-nonint); }
.slls-sx-rd-chip.sf { --chip: var(--sx-hl-sf); } .slls-sx-rd-chip.miss { --chip: var(--ui-accent); }
.slls-sx-rd-chip.on { border-color: var(--chip); color: var(--ui-text); background: color-mix(in srgb, var(--chip) 12%, transparent); }
.slls-sx-rd-chip.on svg { color: var(--chip); }
.slls-sx-rd-main { flex: 1 1 auto; min-height: 0; display: flex; }
.slls-sx-rd-side {
    position: relative; flex-shrink: 0; display: flex; flex-direction: column; gap: 8px; min-height: 0;
    padding: 8px 10px; border-right: 1px solid var(--ui-border); background: var(--ui-bg);
}
.slls-sx-rd-side.collapsed { width: 44px; align-items: center; padding: 8px 4px; }
.slls-sx-rd-rail { writing-mode: vertical-rl; transform: rotate(180deg); font-size: 11px; font-weight: 600; letter-spacing: 0.06em; text-transform: uppercase; color: var(--ui-text-tertiary); }
.slls-sx-rd-side-head { display: flex; align-items: center; gap: 4px; }
.slls-sx-rd-side-head .slls-sx-pane-label { flex: 1; }
.slls-sx-rd-side .slls-sx-dlm-note { margin: 0; }
.slls-sx-rd-tables { flex: 1 1 0; min-height: 0; overflow: auto; display: flex; flex-direction: column; gap: 1px; }
.slls-sx-rd-trow { display: flex; align-items: center; gap: 6px; padding: 2px 4px; border-radius: 6px; font-size: 12.5px; }
.slls-sx-rd-trow:hover { background: var(--ui-surface-2); }
.slls-sx-rd-trow.on { background: var(--ui-accent-soft); }
.slls-sx-rd-trow > .slls-sx-ricon svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }
.slls-sx-rd-trow .name {
    appearance: none; flex: 1; min-width: 0; padding: 0; border: none; background: none; color: inherit; font: inherit;
    text-align: left; cursor: pointer; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; transform: none !important;
}
.slls-sx-rd-trow .meta { font-size: 11px; color: var(--ui-text-tertiary); font-variant-numeric: tabular-nums; }
.slls-sx-rd-trow .slls-sx-iconbtn { width: 24px; height: 24px; opacity: 0; }
.slls-sx-rd-trow:hover .slls-sx-iconbtn { opacity: 1; }
.slls-sx-rd-side-foot { padding-top: 6px; border-top: 1px solid var(--ui-border); }
.slls-sx-rd-grip { position: absolute; top: 0; right: -3px; bottom: 0; width: 6px; cursor: col-resize; z-index: 2; }
.slls-sx-rd-grip:hover { background: var(--ui-accent-soft); }
.slls-sx-rd-canvas { position: relative; flex: 1 1 auto; min-width: 0; min-height: 0; background: var(--ui-bg-secondary); }
.slls-sx-rd-scroll { position: absolute; inset: 0; overflow: auto; cursor: grab; }
.slls-sx-rd-scroll.panning { cursor: grabbing; user-select: none; }
.slls-sx-rd-sizer { position: relative; }
.slls-sx-rd-layer { position: absolute; left: 0; top: 0; transform-origin: 0 0; }
.slls-sx-rd-svg { position: absolute; left: 0; top: 0; overflow: visible; }
.slls-sx-rd-edge { --edge: var(--ui-accent); }
.slls-sx-rd-edge .ln { stroke: var(--edge); stroke-opacity: 0.7; }
.slls-sx-rd-edge .arrow, .slls-sx-rd-edge .end { fill: var(--edge); }
.slls-sx-rd-edge .card circle { fill: var(--ui-bg-solid); stroke: var(--edge); stroke-width: 1.25; }
.slls-sx-rd-edge .card text { fill: var(--ui-text); font-size: 13px; font-weight: 700; font-family: var(--sx-mono); }
.slls-sx-rd-edge .hit { stroke: transparent; stroke-width: 14; pointer-events: stroke; cursor: pointer; }
.slls-sx-rd-edge.sf { --edge: var(--sx-hl-sf); } .slls-sx-rd-edge.lim { --edge: var(--sx-hl-lim); }
.slls-sx-rd-edge.bidi { --edge: var(--sx-hl-bidi); } .slls-sx-rd-edge.mm { --edge: var(--sx-hl-mm); }
.slls-sx-rd-edge.mm .ln, .slls-sx-rd-edge.bidi .ln, .slls-sx-rd-edge.lim .ln, .slls-sx-rd-edge.sf .ln, .slls-sx-rd-edge.hov .ln { stroke-opacity: 1; }
.slls-sx-rd-edge.hov { --edge: var(--ui-accent); }
.slls-sx-rd-edge.new .ln { stroke: var(--sx-warn); }
.slls-sx-rd-panel {
    position: absolute; display: flex; flex-direction: column; overflow: hidden; cursor: default;
    border: 1.5px solid var(--ui-border-strong); border-radius: 10px; background: var(--ui-bg-solid); box-shadow: var(--ui-shadow-sm);
}
.slls-sx-rd-panel.src { border-color: var(--src); }
.slls-sx-rd-panel.src-0 { --src: var(--sx-src-0); } .slls-sx-rd-panel.src-1 { --src: var(--sx-src-1); }
.slls-sx-rd-panel.src-2 { --src: var(--sx-src-2); } .slls-sx-rd-panel.src-3 { --src: var(--sx-src-3); }
.slls-sx-rd-panel.src-4 { --src: var(--sx-src-4); } .slls-sx-rd-panel.src-5 { --src: var(--sx-src-5); }
.slls-sx-rd-panel.src-6 { --src: var(--sx-src-6); } .slls-sx-rd-panel.src-7 { --src: var(--sx-src-7); }
.slls-sx-rd-panel.src-8 { --src: var(--sx-src-8); } .slls-sx-rd-panel.src-9 { --src: var(--sx-src-9); }
.slls-sx-rd-panel.snow { border-color: var(--sx-hl-sf); box-shadow: 0 0 0 3px color-mix(in srgb, var(--sx-hl-sf) 22%, transparent); }
.slls-sx-rd-panel.drop { border-color: var(--ui-accent); box-shadow: 0 0 0 3px var(--ui-accent-soft); }
.slls-sx-rd-head {
    flex-shrink: 0; display: flex; align-items: center; gap: 5px; padding: 0 4px 0 2px; cursor: move;
    border-bottom: 1px solid var(--ui-border); background: var(--ui-bg-secondary); user-select: none;
}
.slls-sx-rd-head .kind svg { width: 14px; height: 14px; color: var(--ui-accent); }
.slls-sx-rd-head .name { flex: 1; min-width: 0; font-size: 12.5px; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-rd-head .mode { display: inline-flex; color: var(--ui-text-tertiary); }
.slls-sx-rd-head .mode svg { width: 12px; height: 12px; }
.slls-sx-rd-head .slls-sx-iconbtn { width: 24px; height: 24px; }
.slls-sx-rd-head .slls-sx-iconbtn svg { width: 13px; height: 13px; }
.slls-sx-rd-cols { flex: 1 1 auto; min-height: 0; overflow: auto; padding: 4px 0; }
.slls-sx-rd-panel.collapsed .slls-sx-rd-cols { overflow: hidden; }
.slls-sx-rd-col { flex-shrink: 0; overflow: hidden; white-space: nowrap; }
.slls-sx-rd-col { display: flex; align-items: center; gap: 6px; padding: 0 8px; font-size: 12px; }
.slls-sx-rd-col[draggable="true"] { cursor: grab; }
.slls-sx-rd-col > span:first-child svg { width: 12px; height: 12px; color: var(--ui-text-tertiary); }
.slls-sx-rd-col .key.pk svg { color: var(--sx-key-pk); }
.slls-sx-rd-col .key.fk svg { color: var(--sx-key-fk); }
.slls-sx-rd-col .name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-rd-col .dt { font-size: 10.5px; color: var(--ui-text-tertiary); }
.slls-sx-rd-col.hl { background: var(--ui-accent-soft); }
.slls-sx-rd-col.hl .name { color: var(--ui-accent); font-weight: 600; }
.slls-sx-rd-col.flagged { background: color-mix(in srgb, var(--sx-hl-nonint) 12%, transparent); }
.slls-sx-rd-col.flagged .dt { color: var(--sx-hl-nonint); font-weight: 600; }
.slls-sx-rd-none {
    display: flex; align-items: center; height: 22px; padding: 0 10px; overflow: hidden; white-space: nowrap;
    font-size: 11.5px; color: var(--ui-text-tertiary); font-style: italic;
}
.slls-sx-rd-resize { position: absolute; right: 1px; bottom: 1px; width: 14px; height: 14px; cursor: nwse-resize; color: var(--ui-text-tertiary); }
.slls-sx-rd-tip {
    position: absolute; z-index: 5; width: max-content; min-width: 240px; max-width: 380px; padding: 9px 11px; pointer-events: none;
    font-size: 12px; line-height: 1.4; text-align: left; white-space: normal; word-break: normal; letter-spacing: normal;
    border: 1px solid var(--ui-border-strong); border-radius: 10px; background: var(--ui-bg-solid); box-shadow: var(--ui-shadow-lg);
}
.slls-sx-rd-tip-head { display: flex; align-items: center; gap: 6px; font-weight: 600; }
.slls-sx-rd-tip-head svg { width: 14px; height: 14px; color: var(--ui-accent); }
.slls-sx-rd-tip-head .name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-rd-tip-head .kind { font-weight: 400; font-size: 11px; color: var(--ui-text-tertiary); }
.slls-sx-rd-tip-head .slls-sx-pill { margin-left: 0; }
.slls-sx-rd-tip-path { margin-top: 4px; font-family: var(--sx-mono); font-size: 11px; color: var(--ui-text-secondary); overflow-wrap: break-word; }
.slls-sx-rd-tip-grid {
    display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 4px 14px; margin: 8px 0 0; padding: 0;
}
.slls-sx-rd-tip-grid > .k { color: var(--ui-text-tertiary); white-space: nowrap; }
.slls-sx-rd-tip-grid > .v { min-width: 0; color: var(--ui-text); overflow-wrap: break-word; word-break: normal; }
.slls-sx-rd-legend { position: absolute; left: 12px; bottom: 12px; z-index: 3; font-size: 12px; }
.slls-sx-rd-legend .pill {
    display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; border: 1px solid var(--ui-border-strong);
    border-radius: 999px; background: var(--ui-bg-solid); box-shadow: var(--ui-shadow-sm); color: var(--ui-text-secondary);
}
.slls-sx-rd-legend .pill svg { width: 13px; height: 13px; }
.slls-sx-rd-legend .dots { display: inline-flex; gap: 3px; }
.slls-sx-rd-legend .sw { display: inline-block; width: 9px; height: 9px; border-radius: 50%; background: var(--src); flex-shrink: 0; }
.slls-sx-rd-legend .sw.src-0 { --src: var(--sx-src-0); } .slls-sx-rd-legend .sw.src-1 { --src: var(--sx-src-1); }
.slls-sx-rd-legend .sw.src-2 { --src: var(--sx-src-2); } .slls-sx-rd-legend .sw.src-3 { --src: var(--sx-src-3); }
.slls-sx-rd-legend .sw.src-4 { --src: var(--sx-src-4); } .slls-sx-rd-legend .sw.src-5 { --src: var(--sx-src-5); }
.slls-sx-rd-legend .sw.src-6 { --src: var(--sx-src-6); } .slls-sx-rd-legend .sw.src-7 { --src: var(--sx-src-7); }
.slls-sx-rd-legend .sw.src-8 { --src: var(--sx-src-8); } .slls-sx-rd-legend .sw.src-9 { --src: var(--sx-src-9); }
.slls-sx-rd-legend .full {
    display: none; position: absolute; left: 0; bottom: 0; width: 300px; max-height: 280px; overflow: auto; padding: 10px;
    border: 1px solid var(--ui-border-strong); border-radius: 10px; background: var(--ui-bg-solid); box-shadow: var(--ui-shadow-lg);
}
.slls-sx-rd-legend:hover .full { display: block; }
.slls-sx-rd-legend .item { display: flex; align-items: center; gap: 8px; padding: 3px 0; }
.slls-sx-rd-legend .item .label { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-rd-zoom {
    position: absolute; right: 12px; bottom: 12px; z-index: 3; display: flex; align-items: center; gap: 2px; padding: 3px;
    border: 1px solid var(--ui-border-strong); border-radius: 10px; background: var(--ui-bg-solid); box-shadow: var(--ui-shadow-sm);
    font-size: 12px; color: var(--ui-text-secondary);
}
.slls-sx-rd-zoom .slls-sx-input { width: 44px; padding: 3px 4px; text-align: right; font-size: 12px; font-variant-numeric: tabular-nums; }
.slls-sx-rd-sidepanel { width: 340px; flex-shrink: 0; display: flex; flex-direction: column; min-height: 0; border-left: 1px solid var(--ui-border); background: var(--ui-bg); }
.slls-sx-rd-sp-head { display: flex; align-items: center; gap: 7px; padding: 8px 10px 8px 14px; border-bottom: 1px solid var(--ui-border); }
.slls-sx-rd-sp-head > .slls-sx-ricon svg { width: 15px; height: 15px; color: var(--ui-accent); }
.slls-sx-rd-sp-head .title { flex: 1; font-size: 13px; font-weight: 600; }
.slls-sx-rd-sp-body { flex: 1 1 0; min-height: 0; overflow: auto; padding: 10px 14px; display: flex; flex-direction: column; gap: 10px; }
.slls-sx-rd-sp-body .slls-sx-tool-error { margin: 0; }
.slls-sx-rd-tbtn {
    appearance: none; display: inline-flex; align-items: center; gap: 6px; max-width: 100%; padding: 3px 6px;
    border: none; border-radius: 6px; background: transparent; color: var(--ui-text); font: inherit; font-size: 12.5px;
    font-weight: 600; cursor: pointer; transform: none !important;
}
.slls-sx-rd-tbtn:hover { background: var(--ui-surface-2); }
.slls-sx-rd-tbtn svg { width: 14px; height: 14px; color: var(--ui-accent); }
.slls-sx-rd-tbtn .meta { font-weight: 400; font-size: 11px; color: var(--ui-text-tertiary); }
.slls-sx-rd-chain, .slls-sx-rd-cand { padding: 8px; border: 1px solid var(--ui-border); border-radius: 8px; }
.slls-sx-rd-chain .roles { font-size: 11px; color: var(--ui-text-tertiary); margin-bottom: 4px; }
.slls-sx-rd-chain .path { display: flex; align-items: center; flex-wrap: wrap; gap: 2px; }
.slls-sx-rd-chain .path .arrow { color: var(--ui-text-tertiary); }
.slls-sx-rd-chain .slls-sx-rd-tbtn svg { color: var(--sx-hl-sf); }
.slls-sx-rd-sug { padding: 4px 6px 4px 24px; font-size: 12px; }
.slls-sx-rd-sug .line, .slls-sx-rd-sug .inferred { display: flex; align-items: center; flex-wrap: wrap; gap: 5px; }
.slls-sx-rd-sug .line svg { width: 12px; height: 12px; color: var(--ui-text-tertiary); }
.slls-sx-rd-sug .slls-sx-pill { margin-left: 0; }
.slls-sx-rd-sug .inferred { margin-top: 4px; padding: 4px 6px; border-radius: 6px; background: var(--ui-surface-2); }
.slls-sx-rd-sug .slls-sx-tool-error { margin: 4px 0 0; padding: 4px 8px; }
.slls-sx-rd-more { padding: 2px 6px 0 24px; font-size: 11.5px; color: var(--ui-text-tertiary); }
.slls-sx-rd-link-from { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
.slls-sx-rd-link-from .slls-sx-pill { margin-left: 0; }
.slls-sx-modal:has(.slls-sx-rd-link-from) .slls-sx-modal-body { display: flex; flex-direction: column; gap: 10px; }

/* Notices shared by the service tools */
.slls-sx-tool-notice {
    display: flex; align-items: center; gap: 8px; margin: 8px 16px 0; padding: 8px 10px 8px 12px; font-size: 12.5px;
    border: 1px solid var(--ui-border); border-radius: 8px;
}
.slls-sx-tool-wrap > .slls-sx-tool-notice { margin: 0; }
.slls-sx-tool-notice > span:not(.slls-sx-ricon) { flex: 1; min-width: 0; }
.slls-sx-tool-notice svg { width: 15px; height: 15px; flex-shrink: 0; }
.slls-sx-tool-notice.ok { border-color: transparent; background: var(--sx-success-soft); color: var(--sx-success); }
.slls-sx-tool-notice.error { border-color: var(--ui-danger-border); background: var(--ui-danger-bg); color: var(--ui-danger-text); }
.slls-sx-tool-notice .slls-sx-iconbtn { width: 24px; height: 24px; color: inherit; }

/* Perspective editor */
.slls-sx-fpm-main .slls-sx-tool-toolbar { padding: 8px 16px; }
.slls-sx-pe-tree { display: flex; flex-direction: column; padding: 6px 12px 14px; }
.slls-sx-pe-row { display: flex; align-items: center; gap: 7px; min-height: 30px; padding: 2px 6px; border-radius: 6px; font-size: 12.5px; }
.slls-sx-pe-row:hover { background: var(--ui-surface-2); }
.slls-sx-pe-row.table { margin-top: 2px; }
.slls-sx-pe-row.table .name { font-weight: 600; }
.slls-sx-pe-row.field { padding-left: 52px; cursor: pointer; min-height: 26px; }
.slls-sx-pe-row.muted .name { color: var(--ui-text-tertiary); }
.slls-sx-pe-row > .slls-sx-ricon svg { width: 14px; height: 14px; color: var(--ui-text-secondary); }
.slls-sx-pe-row .name { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-pe-row .meta { margin-left: auto; padding-left: 10px; font-size: 11.5px; color: var(--ui-text-tertiary); white-space: nowrap; font-variant-numeric: tabular-nums; }

/* Query scale-out manager */
.slls-sx-qso-head { display: flex; align-items: center; gap: 10px; }
.slls-sx-qso-head .slls-sx-section-title { margin: 0; }
.slls-sx-qso-head .slls-sx-pill { margin-left: 0; }
.slls-sx-qso-head .slls-sx-btn { margin-left: auto; }
.slls-sx-qso-row {
    display: flex; align-items: center; justify-content: space-between; gap: 16px;
    padding: 10px 0; border-top: 1px solid var(--ui-border);
}
.slls-sx-qso-label { display: flex; flex-direction: column; gap: 2px; min-width: 0; }
.slls-sx-qso-label .title { font-size: 13px; font-weight: 500; }
.slls-sx-qso-label .help { font-size: 12px; color: var(--ui-text-secondary); }
.slls-sx .slls-sx-qso-max { width: 110px; text-align: right; font-variant-numeric: tabular-nums; }
.slls-sx-qso-actions { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; padding-top: 10px; border-top: 1px solid var(--ui-border); }
.slls-sx-qso-hint { font-size: 12px; color: var(--sx-warn); }
.slls-sx-qso-stats { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px 16px; }
.slls-sx-qso-stats .stat { display: flex; flex-direction: column; gap: 2px; min-width: 0; }
.slls-sx-qso-stats .k { font-size: 11.5px; color: var(--ui-text-tertiary); }
.slls-sx-qso-stats .v { font-size: 12.5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-qso-table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
.slls-sx-qso-table th { text-align: left; padding: 6px 8px; font-size: 11.5px; font-weight: 600; color: var(--ui-text-secondary); border-bottom: 1px solid var(--ui-border); }
.slls-sx-qso-table td { padding: 6px 8px; border-bottom: 1px solid var(--ui-border); }

/* Warm Direct Lake cache */
.slls-sx-warm-right { margin-left: auto; display: flex; align-items: center; gap: 8px; }
.slls-sx-warm-right .slls-sx-tool-count { margin-left: 4px; }
.slls-sx-warm-persp { gap: 12px; }
.slls-sx-warm-persp .slls-sx-dlm-note { flex: 1 1 240px; margin: 0; }
.slls-sx-warm-deps { display: inline-flex; align-items: center; gap: 6px; font-size: 12.5px; white-space: nowrap; }
.slls-sx-warm-table tr.table td { background: var(--ui-bg-tertiary); font-weight: 500; }
.slls-sx-warm-table td.num, .slls-sx-warm-table th.num { text-align: right; font-variant-numeric: tabular-nums; }
.slls-sx-warm-table .slls-sx-dm-obj .meta { font-size: 11px; color: var(--ui-text-tertiary); }
.slls-sx-warm-table td .slls-sx-pill { margin-left: 0; }
.slls-sx-warm-sort {
    appearance: none; display: inline-flex; align-items: center; gap: 4px; padding: 0; border: none; background: none;
    color: inherit; font: inherit; cursor: pointer; transform: none !important;
}
.slls-sx-warm-sort.on { color: var(--ui-text); }
.slls-sx-warm-sort .dir { font-size: 9px; }
.slls-sx-warm-table th:first-child .slls-sx-warm-sort { padding-left: 46px; }

/* Create .vpax file */
.slls-sx-modal.slls-sx-vpax { width: min(560px, 100%); }
.slls-sx-vpax .slls-sx-modal-body { display: flex; flex-direction: column; gap: 12px; overflow: visible; }
.slls-sx-vpax .slls-sx-tool-notice, .slls-sx-vpax .slls-sx-tool-error { margin: 0; }
.slls-sx-vpax .slls-sx-dlm-text { margin: 0; }
.slls-sx-vpax-fields { display: flex; flex-direction: column; gap: 12px; }
.slls-sx-vpax-name { display: flex; align-items: stretch; }
.slls-sx-vpax-name .slls-sx-input { flex: 1; border-top-right-radius: 0; border-bottom-right-radius: 0; }
.slls-sx-vpax-name .ext {
    display: inline-flex; align-items: center; padding: 0 10px; font-family: var(--sx-mono); font-size: 12px; color: var(--ui-text-secondary);
    border: 1px solid var(--ui-border-strong); border-left: none; border-radius: 0 var(--sx-radius-sm) var(--sx-radius-sm) 0; background: var(--ui-surface-2);
}
.slls-sx-vpax-opts { display: flex; align-items: center; flex-wrap: wrap; gap: 10px 18px; }
.slls-sx-vpax-opts .slls-sx-tool-field { flex-direction: row; align-items: center; gap: 8px; }
.slls-sx-tmdl-name { font-weight: 600; }

/* Dependencies: toolbar, segmented toggles, tree and node graph */
.slls-sx-dep-toolbar {
    display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
    padding: 8px 16px; border-bottom: 1px solid var(--ui-border);
}
.slls-sx-dep-hint {
    flex: 1 1 160px; min-width: 0; font-size: 12px; color: var(--ui-text-secondary);
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.slls-sx-seg {
    display: inline-flex; gap: 2px; padding: 2px; flex-shrink: 0;
    border: 1px solid var(--ui-border); border-radius: 8px; background: var(--ui-surface-2);
}
.slls-sx-seg button {
    appearance: none; display: inline-flex; align-items: center; gap: 5px; padding: 4px 10px;
    border: none; border-radius: 6px; background: transparent; color: var(--ui-text-secondary);
    font-size: 12px; font-weight: 500; cursor: pointer;
}
.slls-sx-seg button:hover { color: var(--ui-text); }
.slls-sx-seg button.on { background: var(--ui-accent); color: var(--ui-on-accent); }
.slls-sx-seg button svg { width: 13px; height: 13px; }
.slls-sx-dep-root { font-weight: 600; }
.slls-sx-dep-row .slls-sx-pill { margin-left: 4px; }
.slls-sx-dep-row .slls-sx-caret { width: 16px; height: 16px; }
.slls-sx-modal.graph .slls-sx-graph-body { padding: 0; position: relative; overflow: hidden; }
.slls-sx-graph { position: absolute; inset: 0; overflow: auto; background: var(--ui-bg-tertiary); cursor: grab; }
.slls-sx-graph.panning { cursor: grabbing; user-select: none; }
.slls-sx-graph-sizer { position: relative; }
.slls-sx-graph-layer { position: absolute; left: 0; top: 0; transform-origin: 0 0; }
.slls-sx-graph-inner { position: absolute; }
.slls-sx-graph-edges {
    position: absolute; inset: 0; overflow: visible; pointer-events: none; color: var(--ui-text-tertiary);
}
.slls-sx-gnode {
    position: absolute; display: flex; flex-direction: column; justify-content: center; gap: 2px;
    padding: 5px 9px; text-align: left; font-family: inherit; color: var(--ui-text); cursor: pointer;
    border: 1px solid color-mix(in srgb, var(--cat) 50%, transparent); border-radius: 8px;
    background: color-mix(in srgb, var(--cat) 10%, var(--ui-bg)); box-shadow: var(--ui-shadow-sm);
    transition: box-shadow 120ms ease;
}
.slls-sx-gnode:hover { box-shadow: 0 0 0 2px var(--ui-accent-soft), var(--ui-shadow-sm); }
.slls-sx-gnode.root { border: 2px solid var(--cat); cursor: default; }
.slls-sx-gnode-top { display: flex; align-items: center; gap: 6px; min-width: 0; }
.slls-sx-gnode-icon { display: inline-flex; color: var(--cat); }
.slls-sx-gnode-icon svg { width: 14px; height: 14px; }
.slls-sx-gnode-label { font-size: 12.5px; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.slls-sx-gnode-type {
    font-size: 10px; letter-spacing: 0.05em; text-transform: uppercase; color: var(--ui-text-tertiary);
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.slls-sx-zoom {
    position: absolute; right: 12px; bottom: 12px; display: flex; flex-direction: column; align-items: center;
    gap: 2px; padding: 3px; border: 1px solid var(--ui-border); border-radius: 8px;
    background: var(--ui-bg-solid); box-shadow: var(--ui-shadow-md);
}
.slls-sx-zoom-pct { display: flex; align-items: center; font-size: 11.5px; font-weight: 600; color: var(--ui-text-secondary); }
.slls-sx-zoom-pct input {
    width: 30px; border: none; background: transparent; text-align: right; color: var(--ui-text);
    font: inherit; outline: none;
}
.slls-sx-graph-empty { position: absolute; left: 0; white-space: nowrap; font-size: 12.5px; color: var(--ui-text-tertiary); pointer-events: none; }
.slls-sx-prompt-row { display: flex; flex-direction: column; gap: 6px; }
"""

_WIDGET_JS = r"""
function render({ model, el }) {
    const ICON = __SLLS_EXPLORER_ICONS__;
    const KEY_SEP = "\u001f";

    const root = document.createElement("div");
    root.className = "slls-sx";
    el.appendChild(root);
    function applyTheme() {
        root.classList.remove("slls-sx-dark", "slls-sx-auto");
        const dm = model.get("dark_mode");
        if (dm === true) root.classList.add("slls-sx-dark");
        else if (dm == null) root.classList.add("slls-sx-auto");
    }
    applyTheme();
    model.on("change:dark_mode", applyTheme);

    // ------------------------------------------------------------------
    // Small DOM helpers
    // ------------------------------------------------------------------
    function h(tag, cls, text) {
        const node = document.createElement(tag);
        if (cls) node.className = cls;
        if (text !== undefined && text !== null) node.textContent = String(text);
        return node;
    }
    function esc(s) {
        return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => (
            { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
    }
    function iconEl(name, cls) {
        const span = h("span", cls || "slls-sx-ricon");
        span.innerHTML = ICON[name] || "";
        return span;
    }
    function button(cls, label, iconName, onClick, title) {
        const b = h("button", cls);
        b.type = "button";
        if (iconName) b.innerHTML = ICON[iconName] || "";
        if (label) b.appendChild(document.createTextNode(label));
        if (title) { b.title = title; b.setAttribute("aria-label", title); }
        if (onClick) b.addEventListener("click", onClick);
        return b;
    }
    function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); }
    function plural(n, word) { return `${n} ${word}${n === 1 ? "" : "s"}`; }

    // ------------------------------------------------------------------
    // DAX / M tokenizers (syntax highlighting + rename cascade)
    // ------------------------------------------------------------------
    function makeTokenizer(spec, flags) {
        const re = new RegExp(spec.map(([, r]) => `(${r})`).join("|"), "y" + (flags || ""));
        return function tokenize(text) {
            const tokens = [];
            let pos = 0;
            while (pos < text.length) {
                const ch = text[pos];
                if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") { pos += 1; continue; }
                re.lastIndex = pos;
                const m = re.exec(text);
                if (!m) { pos += 1; continue; }
                let type = null;
                for (let i = 1; i < m.length; i++) {
                    if (m[i] !== undefined) { type = spec[i - 1][0]; break; }
                }
                tokens.push({ type, text: m[0], position: pos });
                pos = re.lastIndex;
            }
            return tokens;
        };
    }
    const tokenizeDax = makeTokenizer([
        ["COMMENT", "//[^\\n]*|--[^\\n]*|/\\*[\\s\\S]*?\\*/"],
        ["TABLE_COLUMN", "(?:'(?:[^']|'')+'|[A-Za-z_][A-Za-z0-9_]*)\\[[^\\]]+\\]"],
        ["QUOTED_IDENTIFIER", "'(?:[^']|'')+'"],
        ["COLUMN", "\\[[^\\]]+\\]"],
        ["STRING", '"(?:[^"]|"")*"'],
        ["NUMBER", "\\d+(?:\\.\\d+)?"],
        ["IDENTIFIER", "[A-Za-z_][A-Za-z0-9_.]*"],
        ["OPERATOR", "<=|>=|<>|==|&&|\\|\\||[-+*/=<>&^!]"],
        ["LPAREN", "\\("], ["RPAREN", "\\)"], ["LBRACE", "\\{"], ["RBRACE", "\\}"], ["COMMA", ","],
    ], "i");
    const tokenizeM = makeTokenizer([
        ["COMMENT", "//[^\\n]*|/\\*[\\s\\S]*?\\*/"],
        ["STRING", '"(?:[^"]|"")*"'],
        ["QUOTED_IDENTIFIER", '#"(?:[^"]|"")*"'],
        ["HASH_KEYWORD", "#(?:binary|datetimezone|datetime|date|duration|infinity|nan|sections|shared|table|time)\\b"],
        ["NUMBER", "0[xX][0-9A-Fa-f]+|(?:\\d+(?:\\.\\d+)?|\\.\\d+)(?:[eE][+-]?\\d+)?"],
        ["IDENTIFIER", "[A-Za-z_][A-Za-z0-9_]*(?:\\.[A-Za-z0-9_]+)*"],
        ["OPERATOR", "=>|<=|>=|<>|\\.\\.\\.|\\.\\.|[-+*/&=<>@?]"],
        ["LPAREN", "\\("], ["RPAREN", "\\)"], ["LBRACE", "\\{"], ["RBRACE", "\\}"],
        ["LBRACKET", "\\["], ["RBRACKET", "\\]"], ["COMMA", ","], ["SEMICOLON", ";"],
    ]);
    const DAX_KEYWORDS = new Set([
        "DAY", "WEEK", "MONTH", "QUARTER", "YEAR", "ASC", "DESC", "SKIP", "DENSE", "LAST", "FIRST",
        "TRUE", "FALSE", "BOTH", "ONEWAY", "ONEWAY_LEFTFILTERSRIGHT", "ONEWAY_RIGHTFILTERSLEFT", "NONE",
        "ABSOLUTE", "RELATIVE", "INTEGER", "STRING", "DOUBLE", "BOOLEAN", "DATETIME", "VARIANT", "TEXT",
        "ALPHABETICAL", "DEFINE", "ORDER", "BY", "START", "AT", "EVALUATE", "VAR", "RETURN", "IN", "NOT",
        "AND", "OR", "FUNCTION", "MEASURE", "TABLE", "COLUMN", "EXPR", "SCALAR", "ANYVAL", "ANYREF", "VAL",
    ]);
    const M_KEYWORDS = new Set([
        "and", "as", "each", "else", "error", "false", "if", "in", "is", "let", "meta", "not", "null",
        "or", "otherwise", "section", "shared", "then", "true", "try", "type",
    ]);
    function nextSignificant(tokens, start) {
        for (let j = start; j < tokens.length; j++) if (tokens[j].type !== "COMMENT") return tokens[j];
        return null;
    }
    function daxKinds(tokens) {
        const vars = new Set();
        for (let i = 0; i < tokens.length; i++) {
            if (tokens[i].type === "IDENTIFIER" && tokens[i].text.toUpperCase() === "VAR") {
                const next = nextSignificant(tokens, i + 1);
                if (next && next.type === "IDENTIFIER") vars.add(next.text);
            }
        }
        return tokens.map((t, i) => {
            switch (t.type) {
                case "COMMENT": return "comment";
                case "STRING": return "string";
                case "NUMBER": return "number";
                case "OPERATOR": return "operator";
                case "LPAREN": case "RPAREN": case "LBRACE": case "RBRACE": case "COMMA": return "punctuation";
                case "IDENTIFIER": {
                    const next = nextSignificant(tokens, i + 1);
                    if (next && next.type === "LPAREN") return "function";
                    if (vars.has(t.text)) return "variable";
                    if (DAX_KEYWORDS.has(t.text.toUpperCase())) return "keyword";
                    return "";
                }
                default: return "";
            }
        });
    }
    function mKinds(tokens) {
        return tokens.map((t, i) => {
            switch (t.type) {
                case "COMMENT": return "comment";
                case "STRING": return "string";
                case "QUOTED_IDENTIFIER": return "variable";
                case "HASH_KEYWORD": return "keyword";
                case "NUMBER": return "number";
                case "OPERATOR": return "operator";
                case "IDENTIFIER": {
                    if (M_KEYWORDS.has(t.text)) return "keyword";
                    const next = nextSignificant(tokens, i + 1);
                    if ((next && next.type === "LPAREN") || t.text.includes(".")) return "function";
                    return "";
                }
                default: return t.type === "IDENTIFIER" ? "" : "punctuation";
            }
        });
    }
    function highlight(text, lang) {
        if (!text) return "";
        const tokens = lang === "m" ? tokenizeM(text) : tokenizeDax(text);
        const kinds = lang === "m" ? mKinds(tokens) : daxKinds(tokens);
        let out = "";
        let cursor = 0;
        tokens.forEach((t, i) => {
            if (t.position > cursor) out += esc(text.slice(cursor, t.position));
            out += kinds[i] ? `<span class="slls-sx-tk-${kinds[i]}">${esc(t.text)}</span>` : esc(t.text);
            cursor = t.position + t.text.length;
        });
        if (cursor < text.length) out += esc(text.slice(cursor));
        return out;
    }

    // ------------------------------------------------------------------
    // Rename cascade: rewrite DAX references to renamed tables / columns /
    // measures, using the tokenizer so strings and comments are never touched.
    // ------------------------------------------------------------------
    const ciEq = (a, b) => String(a).toLowerCase() === String(b).toLowerCase();
    function daxTableRef(name) {
        return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? name : `'${name.replace(/'/g, "''")}'`;
    }
    function unquoteTable(text) {
        return text.startsWith("'") && text.endsWith("'") ? text.slice(1, -1).replace(/''/g, "'") : text;
    }
    function rewriteDax(text, renames, contextTable, rowContext) {
        const tokens = tokenizeDax(text);
        let out = "";
        let cursor = 0;
        for (const tok of tokens) {
            out += text.slice(cursor, tok.position);
            cursor = tok.position + tok.text.length;
            let repl = tok.text;
            if (tok.type === "COLUMN") {
                const inner = tok.text.slice(1, -1);
                const mr = renames.find((r) => r.type === "measure" && ciEq(inner, r.oldName));
                if (mr) repl = `[${mr.newName}]`;
                else if (rowContext && contextTable) {
                    const cr = renames.find((r) => r.type === "column" && ciEq(r.table, contextTable) && ciEq(inner, r.oldName));
                    if (cr) repl = `[${cr.newName}]`;
                }
            } else if (tok.type === "TABLE_COLUMN") {
                const i = tok.text.indexOf("[");
                const table = unquoteTable(tok.text.slice(0, i));
                const column = tok.text.slice(i + 1, -1);
                const tr = renames.find((r) => r.type === "table" && ciEq(r.oldName, table));
                const cr = renames.find((r) => r.type === "column" && ciEq(r.table, table) && ciEq(column, r.oldName));
                const mr = renames.find((r) => r.type === "measure" && ciEq(r.table, table) && ciEq(column, r.oldName));
                if (tr || cr || mr) {
                    const newTable = tr ? tr.newName : table;
                    const newCol = cr ? cr.newName : (mr ? mr.newName : column);
                    repl = `${daxTableRef(newTable)}[${newCol}]`;
                }
            } else if (tok.type === "QUOTED_IDENTIFIER") {
                const tr = renames.find((r) => r.type === "table" && ciEq(r.oldName, unquoteTable(tok.text)));
                if (tr) repl = daxTableRef(tr.newName);
            } else if (tok.type === "IDENTIFIER") {
                const tr = renames.find((r) => r.type === "table" && ciEq(r.oldName, tok.text));
                if (tr) {
                    const nextCh = text.slice(cursor).replace(/^\s+/, "")[0];
                    if (nextCh !== "(") repl = daxTableRef(tr.newName);
                }
            }
            out += repl;
        }
        return out + text.slice(cursor);
    }

    // ------------------------------------------------------------------
    // Constants
    // ------------------------------------------------------------------
    const TYPE_LABEL = {
        model: "Model", table: "Table", column: "Column", measure: "Measure", hierarchy: "Hierarchy",
        level: "Level", calculationItem: "Calculation item", partition: "Partition",
        relationship: "Relationship", expression: "Expression", function: "Function",
        perspective: "Perspective", role: "Role", culture: "Culture",
    };
    const TABLE_KIND_LABEL = {
        table: "Table", calculated_table: "Calculated table", calculation_group: "Calculation group",
        field_parameter: "Field parameter", date_table: "Date table",
    };
    const OBJECT_TYPE_FILTERS = [
        ["calculatedColumns", "Calculated columns", "calculated_column"],
        ["calculatedTables", "Calculated tables", "calculated_table"],
        ["calculationGroups", "Calculation groups", "calculation_group"],
        ["calculationItems", "Calculation items", "calculation_item"],
        ["columns", "Columns", "column"],
        ["cultures", "Cultures", "globe"],
        ["expressions", "Expressions", "code"],
        ["functions", "Functions", "zap"],
        ["hierarchies", "Hierarchies", "hierarchy"],
        ["measures", "Measures", "measure"],
        ["partitions", "Partitions", "partition"],
        ["perspectives", "Perspectives", "perspective"],
        ["relationships", "Relationships", "relationship"],
        ["roles", "Roles", "shield_check"],
        ["tables", "Tables", "table"],
    ];
    const TABLE_FILTERS = ["tables", "calculatedTables", "calculationGroups"];
    const FIELD_FILTERS = ["columns", "calculatedColumns", "measures", "hierarchies", "calculationItems", "partitions"];
    const FOLDERS = [
        ["relationships", "Relationships", "relationship"],
        ["expressions", "Expressions", "code"],
        ["functions", "Functions", "zap"],
        ["perspectives", "Perspectives", "perspective"],
        ["roles", "Roles", "shield_check"],
        ["cultures", "Cultures", "globe"],
    ];
    const ENUM_FALLBACK = {
        DataType: ["Automatic", "String", "Int64", "Double", "DateTime", "Decimal", "Boolean", "Binary", "Unknown", "Variant"],
        AggregateFunction: ["Default", "None", "Sum", "Min", "Max", "Count", "Average", "DistinctCount"],
        ModelPermission: ["None", "Read", "ReadRefresh", "Refresh", "Administrator"],
        RelationshipEndCardinality: ["One", "Many", "None"],
        CrossFilteringBehavior: ["OneDirection", "BothDirections", "Automatic"],
    };
    const JSON_PROPS = new Set(["Annotations", "Translations", "Perspectives", "Members", "RlsFilters", "Ols"]);
    const PANE_MIN = 240, PANE_MAX = 640;
    // Starting definition of a new named expression (matches the kernel side).
    const NEW_MODEL_COMPATIBILITY_LEVEL = 1706;
    const NEW_EXPRESSION = "let\n    Source = \"\"\nin\n    Source";

    // ------------------------------------------------------------------
    // State
    // ------------------------------------------------------------------
    let data = {};
    let index = new Map();
    let tableByName = new Map();
    let selectedKey = "";
    let expanded = new Set(["model", "folder:tables"]);
    let search = "";
    let typeFilter = new Set();
    let viewPerspective = "";
    let viewCulture = "";
    let showFilters = true;
    let paneWidth = 340;
    let paneCollapsed = false;
    let mode = "read";
    let propFilter = "";
    let staging = { edits: {}, ops: [], creates: [] };
    let undoStack = [];
    let redoStack = [];
    let lastCoalesce = { key: "", at: 0 };
    let history = [];
    let createSeq = 0;
    // A re-rendered widget (e.g. the notebook reloaded) reopens what was open.
    let pickerOpen = !model.get("explorer_kind") && model.get("dataset_chosen") !== true;
    let pickerWs = model.get("active_workspace_id") || "";
    let pickerDs = model.get("active_dataset_id") || "";
    // Picker screens: "landing" (semantic model or semantic view?), "model"
    // and "view".
    let pickerStep = "landing";
    // "existing" connects to a model; "new" creates an empty one first.
    let pickerMode = "existing";
    let newModelName = "";
    let createError = null;
    let creating = false;
    let workspacesRequested = false;
    // Semantic view picker: "existing" opens a view, "new" creates one.
    let viewPickerMode = "existing";
    let viewWs = pickerWs;
    let viewLh = "";
    let viewPick = "";
    let newViewName = "";
    let viewError = null;
    let lakehouseList = { ws: null, items: [], loading: false, error: "" };
    let viewList = { key: null, items: [], loading: false, error: "" };
    let viewNameFocused = false;
    // Open semantic view (read-only): its payload, an index of its objects by
    // key, and the tree / detail state.
    let viewData = null;
    let vIndex = new Map();
    let vSelected = "view";
    let vExpanded = new Set(["view", "v:datasets", "v:relationships", "v:metrics"]);
    let vSearch = "";
    let vTreeEl = null;
    // Editing a view: the saved objects (vBase), the staged working copy
    // (vWork), undo / redo snapshots and lookups for the editors.
    let vBase = null;
    let vWork = null;
    let vUndo = [];
    let vRedo = [];
    let vEditKey = "";
    let vMode = "read";
    let vSaving = false;
    let vNewSeq = 0;
    let vColCache = {};
    let vColumns = new Map();
    let vTables = null;
    let vPendingSelect = null;
    let requestSeq = 0;
    const callbacks = new Map();
    const collapsedCards = new Set(["Lineage", "Timeline", "Extended properties"]);
    let statusTimer = null;
    let pendingSelect = null;
    // True from clicking Save until the kernel replies; staging is frozen meanwhile
    // so nothing edited during the save is lost when the batch is cleared.
    let saving = false;

    // ------------------------------------------------------------------
    // Kernel communication
    // ------------------------------------------------------------------
    function dispatch(payload) {
        model.set("pending_action", payload);
        model.set("run", (model.get("run") || 0) + 1);
        model.save_changes();
    }
    function request(action, payload, onDone) {
        const id = ++requestSeq;
        if (onDone) callbacks.set(id, onDone);
        dispatch(Object.assign({ action, request_id: id }, payload || {}));
        return id;
    }
    model.on("change:result", () => {
        const res = model.get("result") || {};
        const cb = callbacks.get(res.request_id);
        if (cb) { callbacks.delete(res.request_id); cb(res); }
    });

    // ------------------------------------------------------------------
    // Model index + effective (pending-aware) values
    // ------------------------------------------------------------------
    function keyOf(type, table, hierarchy, name) {
        return [type, table || "", hierarchy || "", name || ""].join(KEY_SEP);
    }
    function targetOf(node) {
        return { type: node.type, table: node.table || "", hierarchy: node.hierarchy || "", name: node.name };
    }
    // The synced payload is compact: property metadata is sent once per object
    // type ("specs"), each object carries only its non-empty values ("v"), the
    // editable properties it does not support ("x") and its non-empty
    // expressions ("e"). It is expanded into the explorer's own node objects so
    // the synced trait value is never mutated.
    const EXPRESSION_DEFS = {
        measure: [["Expression", "Expression"], ["FormatStringExpression", "Format string expression"],
            ["DetailRowsExpression", "Detail rows expression"]],
        calculationItem: [["Expression", "Expression"], ["FormatStringExpression", "Format string expression"]],
        function: [["Expression", "Expression"]],
    };
    function expressionDefs(raw) {
        if (EXPRESSION_DEFS[raw.type]) return EXPRESSION_DEFS[raw.type].map(([p, l]) => [p, l, "dax"]);
        if (raw.type === "column") return raw.column_type === "Calculated" ? [["Expression", "Expression", "dax"]] : [];
        if (raw.type === "partition") {
            if (raw.source_type === "Calculated") return [["Expression", "Expression", "dax"]];
            return raw.source_type === "M" ? [["Expression", "Expression", "m"]] : [];
        }
        if (raw.type === "expression") return [["Expression", "Expression", raw.lang || "m"]];
        return [];
    }
    function inflate(raw) {
        if (!raw || !raw.model) return {};
        const specs = raw.specs || {};
        const node = (r) => {
            const n = Object.assign({}, r);
            delete n.v; delete n.x; delete n.e;
            n.table = r.table || "";
            n.hierarchy = r.hierarchy || "";
            n.key = keyOf(r.type, n.table, n.hierarchy, r.name);
            const values = r.v || {};
            const absent = new Set(r.x || []);
            n.props = [];
            for (const [key, label, group, editable, kind, en] of specs[r.type] || []) {
                if (absent.has(key)) continue;
                const has = Object.prototype.hasOwnProperty.call(values, key);
                if (!editable && !has) continue;
                n.props.push({ key, label, group, editable, kind, enum: en,
                    value: has ? values[key] : (kind === "bool" ? false : null) });
            }
            n.expressions = expressionDefs(r).map(([prop, label, lang]) => ({ prop, label, lang, value: (r.e || {})[prop] || "" }));
            n.annotations = r.annotations || {};
            n.extended_properties = r.extended_properties || {};
            n.hidden = !!r.hidden;
            return n;
        };
        const tables = (raw.tables || []).map((rt) => {
            const t = node(rt);
            t.columns = (rt.columns || []).map(node);
            t.column_names = t.columns.map((c) => c.name);
            t.measures = (rt.measures || []).map(node);
            t.hierarchies = (rt.hierarchies || []).map((rh) => Object.assign(node(rh), { levels: (rh.levels || []).map(node) }));
            t.calculation_items = (rt.calculation_items || []).map(node);
            t.partitions = (rt.partitions || []).map(node);
            const calc = rt.calc_partition ? t.partitions.find((p) => p.name === rt.calc_partition) : null;
            if (calc) {
                t.expressions = [{ prop: "Expression", label: "Expression", lang: "dax",
                    value: (calc.expressions[0] || {}).value || "", target: calc.key }];
            }
            return t;
        });
        const out = Object.assign({}, raw, { model: node(raw.model), tables, errors: raw.errors || [], enums: raw.enums || {} });
        for (const [folder] of FOLDERS) out[folder] = (raw[folder] || []).map(node);
        return out;
    }
    function rebuildIndex() {
        index = new Map();
        tableByName = new Map();
        if (!data || !data.model) return;
        const add = (node, parent) => { node.parent = parent || null; index.set(node.key, node); };
        add(data.model);
        for (const t of data.tables || []) {
            add(t);
            tableByName.set(t.name, t);
            for (const c of t.columns || []) add(c, t);
            for (const m of t.measures || []) add(m, t);
            for (const hi of t.hierarchies || []) {
                add(hi, t);
                for (const lv of hi.levels || []) add(lv, hi);
            }
            for (const it of t.calculation_items || []) add(it, t);
            for (const p of t.partitions || []) add(p, t);
        }
        for (const [k] of FOLDERS) for (const n of data[k] || []) add(n);
    }

    function createFor(node) { return node && node.isNew ? staging.creates.find((c) => c.id === node.createId) : null; }
    function syntheticNode(c) {
        const parent = c.parentKey ? getNode(c.parentKey) : null;
        const tableName = parent ? parent.name : "";
        const enumOf = (name) => ((data.enums || {})[name] ? name : name);
        const P = (key, label, group, kind, extra) => Object.assign({ key, label, group, kind, editable: true, value: null }, extra || {});
        const node = {
            key: "new:" + c.id, isNew: true, createId: c.id, type: c.type, name: c.name, table: tableName,
            hierarchy: "", parent, props: [P("Name", "Name", "General", "text"), P("Description", "Description", "General", "longtext")],
            expressions: [], annotations: {}, extended_properties: {},
        };
        if (c.type === "measure") {
            node.props.push(P("IsHidden", "Hidden", "General", "bool", { value: false }),
                P("DisplayFolder", "Display folder", "Formatting", "text"), P("FormatString", "Format string", "Formatting", "text"));
            node.expressions = [{ prop: "Expression", label: "Expression", lang: "dax", value: "" },
                { prop: "FormatStringExpression", label: "Format string expression", lang: "dax", value: "" }];
        } else if (c.type === "column") {
            node.column_type = "Calculated";
            node.props.push(P("IsHidden", "Hidden", "General", "bool", { value: false }),
                P("DataType", "Data type", "General", "enum", { enum: enumOf("DataType"), value: "String" }),
                P("DisplayFolder", "Display folder", "Formatting", "text"), P("FormatString", "Format string", "Formatting", "text"),
                P("SummarizeBy", "Summarize by", "Formatting", "enum", { enum: enumOf("AggregateFunction"), value: "Default" }));
            node.expressions = [{ prop: "Expression", label: "Expression", lang: "dax", value: "" }];
        } else if (c.type === "calculationItem") {
            node.props.push(P("Ordinal", "Ordinal", "General", "number"));
            node.expressions = [{ prop: "Expression", label: "Expression", lang: "dax", value: "SELECTEDMEASURE()" },
                { prop: "FormatStringExpression", label: "Format string expression", lang: "dax", value: "" }];
        } else if (c.type === "table") {
            node.kind = "calculation_group";
            node.table = "";
            node.props.push(P("IsHidden", "Hidden", "General", "bool", { value: false }));
            node.columns = []; node.measures = []; node.hierarchies = []; node.calculation_items = []; node.partitions = [];
        } else if (c.type === "fieldParameter") {
            node.type = "table";
            node.kind = "field_parameter";
            node.table = "";
            node.props.push(P("IsHidden", "Hidden", "General", "bool", { value: false }));
            node.columns = []; node.measures = []; node.hierarchies = []; node.calculation_items = []; node.partitions = [];
            node.expressions = [{ prop: "Expression", label: "Expression", lang: "dax", value: "" }];
        } else if (c.type === "role") {
            node.props.push(P("ModelPermission", "Model permission", "General", "enum", { enum: enumOf("ModelPermission"), value: "Read" }));
        } else if (c.type === "expression") {
            node.expressions = [{ prop: "Expression", label: "Expression", lang: "m",
                value: NEW_EXPRESSION }];
        } else if (c.type === "relationship") {
            node.from_table = c.props.fromTable; node.from_column = c.props.fromColumn;
            node.to_table = c.props.toTable; node.to_column = c.props.toColumn;
            node.props = [
                P("IsActive", "Active", "Behavior", "bool", { value: true }),
                P("FromCardinality", "From cardinality", "General", "enum", { enum: enumOf("RelationshipEndCardinality") }),
                P("ToCardinality", "To cardinality", "General", "enum", { enum: enumOf("RelationshipEndCardinality") }),
                P("CrossFilteringBehavior", "Cross filtering", "Behavior", "enum", { enum: enumOf("CrossFilteringBehavior") }),
            ];
        }
        return node;
    }
    function newNodes(type, parentKey) {
        return staging.creates.filter((c) => c.type === type && (c.parentKey || "") === (parentKey || "")).map(syntheticNode);
    }
    function getNode(key) {
        if (!key) return null;
        if (key.startsWith("new:")) {
            const c = staging.creates.find((x) => "new:" + x.id === key);
            return c ? syntheticNode(c) : null;
        }
        return index.get(key) || null;
    }
    function editKey(key, prop) { return key + "|" + prop; }
    // A role's non-default object-level security, sorted so it compares stably.
    function olsEntries(node) {
        const out = [];
        for (const tp of node.table_permissions || []) {
            if (tp.permission && tp.permission !== "Default") out.push({ table: tp.table, column: "", permission: tp.permission });
            for (const c of tp.columns || []) {
                if (c.permission && c.permission !== "Default") out.push({ table: tp.table, column: c.column, permission: c.permission });
            }
        }
        return sortOls(out);
    }
    function sortOls(list) {
        return list.sort((a, b) => (a.table + "\u0000" + a.column).localeCompare(b.table + "\u0000" + b.column));
    }
    function originalValue(node, prop) {
        if (prop === "Annotations") return JSON.stringify(node.annotations || {});
        if (prop === "Translations") return JSON.stringify(node.translations || {});
        if (prop === "Perspectives") return JSON.stringify(node.perspectives || []);
        if (prop === "Members") return JSON.stringify(node.members || []);
        if (prop === "RlsFilters") {
            const map = {};
            for (const tp of node.table_permissions || []) if (tp.filter) map[tp.table] = tp.filter;
            return JSON.stringify(map);
        }
        if (prop === "Ols") return JSON.stringify(olsEntries(node));
        const row = (node.props || []).find((p) => p.key === prop);
        if (row) return row.value;
        const expr = (node.expressions || []).find((x) => x.prop === prop && !x.target);
        if (expr) return expr.value;
        if (prop === "Name") return node.name;
        if (prop === "IsHidden") return !!node.hidden;
        return null;
    }
    function value(node, prop) {
        const c = createFor(node);
        if (c) {
            if (prop === "Name") return c.name;
            if (Object.prototype.hasOwnProperty.call(c.props, prop)) return c.props[prop];
            return originalValue(node, prop);
        }
        const e = staging.edits[editKey(node.key, prop)];
        return e ? e.newValue : originalValue(node, prop);
    }
    function jsonValue(node, prop, fallback) {
        try { return JSON.parse(value(node, prop) || "null") || fallback; } catch (e) { return fallback; }
    }
    function effName(node) { return node ? String(value(node, "Name") ?? node.name) : ""; }
    function isHidden(node) { return node.type === "level" ? false : !!value(node, "IsHidden"); }
    function tableDisplayName(savedName) {
        const t = tableByName.get(savedName);
        return t ? effName(t) : savedName;
    }
    // Labels are memoized while the tree renders (sorting asks repeatedly).
    let labelCache = null;
    function displayLabel(node) {
        if (!labelCache) return computeLabel(node);
        let label = labelCache.get(node.key);
        if (label === undefined) { label = computeLabel(node); labelCache.set(node.key, label); }
        return label;
    }
    function computeLabel(node) {
        if (node.type === "relationship") {
            const v = (p) => value(node, p);
            const card = (c) => (c === "Many" ? "*" : c === "One" ? "1" : "");
            const marks = `${card(v("FromCardinality"))}:${card(v("ToCardinality"))}`;
            const end = (table, column) => `'${tableDisplayName(table)}'[${columnDisplayName(table, column)}]`;
            const fromColumn = node.isNew ? node.from_column : (v("FromColumn") || node.from_column);
            const toColumn = node.isNew ? node.to_column : (v("ToColumn") || node.to_column);
            return `${end(node.from_table, fromColumn)} ${marks} ${end(node.to_table, toColumn)}`;
        }
        if (viewCulture && ["table", "column", "measure", "hierarchy", "level"].includes(node.type)) {
            const tr = jsonValue(node, "Translations", {})[viewCulture];
            if (tr && tr.caption) return tr.caption;
        }
        return effName(node);
    }
    function isTranslatedLabel(node) {
        if (!viewCulture) return false;
        const tr = (node.translations || {})[viewCulture];
        return !!(tr && tr.caption);
    }
    function columnDisplayName(table, column) {
        const n = index.get(keyOf("column", table, "", column));
        return n ? effName(n) : column;
    }
    function isDeleted(node) {
        if (!node) return false;
        if (staging.ops.some((o) => o.op === "delete" && o.key === node.key)) return true;
        return node.parent ? isDeleted(node.parent) : false;
    }
    function isDuplicated(node) { return staging.ops.some((o) => o.op === "duplicate" && o.key === node.key); }
    function isDirty(node) {
        if (node.isNew) return true;
        const edited = new Set(Object.values(staging.edits).map((e) => e.key));
        staging.ops.forEach((o) => edited.add(o.key));
        if (edited.has(node.key)) return true;
        return node.type === "table" && (node.partitions || []).some((p) => edited.has(p.key));
    }
    function pendingCount() {
        return Object.keys(staging.edits).length + staging.ops.length + staging.creates.length;
    }
    function objectLabel(node) {
        if (node.type === "table" || !node.table) return displayLabel(node);
        return `'${tableDisplayName(node.table)}'[${effName(node)}]`;
    }
    function same(a, b) {
        const norm = (v) => (v === null || v === undefined ? "" : String(v));
        return norm(a) === norm(b);
    }

    // ------------------------------------------------------------------
    // Staging (edits / ops / creates), undo / redo and rename cascade
    // ------------------------------------------------------------------
    function snapshot() { return JSON.stringify(staging); }
    function pushUndo(coalesceKey) {
        const now = Date.now();
        if (coalesceKey && lastCoalesce.key === coalesceKey && now - lastCoalesce.at < 1500) {
            lastCoalesce.at = now;
            return;
        }
        lastCoalesce = { key: coalesceKey || "", at: now };
        undoStack.push(snapshot());
        if (undoStack.length > 200) undoStack.shift();
        redoStack = [];
    }
    function undo() {
        if (saving) return;
        if (!undoStack.length) return;
        redoStack.push(snapshot());
        staging = JSON.parse(undoStack.pop());
        lastCoalesce = { key: "", at: 0 };
        afterChange();
    }
    function redo() {
        if (saving) return;
        if (!redoStack.length) return;
        undoStack.push(snapshot());
        staging = JSON.parse(redoStack.pop());
        lastCoalesce = { key: "", at: 0 };
        afterChange();
    }
    function setValue(node, prop, newValue, opts) {
        if (saving) return;
        const o = opts || {};
        if (!node) return;
        pushUndo(o.coalesce ? editKey(node.key, prop) : "");
        stageValue(node, prop, newValue, o.label);
        if (!createFor(node)) recomputeCascade();
        afterChange(o);
    }
    function stageValue(node, prop, newValue, label) {
        const c = createFor(node);
        if (c) {
            if (prop === "Name") c.name = String(newValue || "");
            else c.props[prop] = newValue;
            return;
        }
        const ek = editKey(node.key, prop);
        const old = originalValue(node, prop);
        if (same(old, newValue)) delete staging.edits[ek];
        else {
            staging.edits[ek] = {
                key: node.key, target: targetOf(node), property: prop, label: label || prop,
                objectType: node.type, oldValue: old, newValue, userValue: newValue,
            };
        }
    }
    // Stages several values as one undo step (e.g. AI-generated descriptions).
    function stageMany(items) {
        if (saving || !items.length) return;
        pushUndo("");
        for (const it of items) stageValue(it.node, it.prop, it.value, it.label);
        recomputeCascade();
        afterChange();
    }
    function currentRenames() {
        const renames = [];
        for (const e of Object.values(staging.edits)) {
            if (e.property !== "Name" || !["table", "column", "measure"].includes(e.objectType)) continue;
            if (!e.newValue || same(e.newValue, e.oldValue)) continue;
            renames.push({ type: e.objectType, table: e.target.table, oldName: e.oldValue, newName: String(e.newValue) });
        }
        return renames;
    }
    const DAX_PROPS = new Set(["Expression", "FormatStringExpression", "DetailRowsExpression"]);
    function isDaxProp(node, prop) {
        if (!node || !DAX_PROPS.has(prop)) return false;
        if (["measure", "calculationItem", "function"].includes(node.type)) return true;
        if (node.type === "column") return node.column_type === "Calculated";
        if (node.type === "partition") return node.source_type === "Calculated";
        return false;
    }
    function recomputeCascade() {
        for (const k of Object.keys(staging.edits)) if (staging.edits[k].cascade) delete staging.edits[k];
        for (const e of Object.values(staging.edits)) if (e.userValue !== undefined) e.newValue = e.userValue;
        const renames = currentRenames();
        if (!renames.length) return;
        // Expressions the user already edited follow the renames too, re-derived
        // from what they typed so the cascade can be recomputed or undone.
        for (const e of Object.values(staging.edits)) {
            if (e.userValue === undefined) continue;
            const node = getNode(e.key);
            if (e.property === "RlsFilters") {
                const map = JSON.parse(e.userValue || "{}");
                for (const [table, filter] of Object.entries(map)) {
                    if (filter) map[table] = rewriteDax(filter, renames, table, true);
                }
                e.newValue = JSON.stringify(map);
            } else if (isDaxProp(node, e.property)) {
                e.newValue = rewriteDax(String(e.userValue || ""), renames, node.table, node.type === "column");
            }
        }
        const lowered = renames.map((r) => String(r.oldName).toLowerCase());
        const mentions = (expr) => { const l = String(expr).toLowerCase(); return lowered.some((n) => l.includes(n)); };
        const stage = (node, original, contextTable, rowContext) => {
            if (!original || !mentions(original)) return;
            const ek = editKey(node.key, "Expression");
            if (staging.edits[ek]) return;
            const rewritten = rewriteDax(original, renames, contextTable, rowContext);
            if (rewritten === original) return;
            staging.edits[ek] = {
                key: node.key, target: targetOf(node), property: "Expression", label: "Expression",
                objectType: node.type, oldValue: original, newValue: rewritten, cascade: true,
            };
        };
        for (const t of data.tables || []) {
            for (const m of t.measures || []) {
                stage(m, originalValue(m, "Expression"), t.name, false);
            }
            for (const it of t.calculation_items || []) stage(it, originalValue(it, "Expression"), t.name, false);
            for (const c of t.columns || []) if (c.column_type === "Calculated") stage(c, originalValue(c, "Expression"), t.name, true);
            for (const p of t.partitions || []) if (p.source_type === "Calculated") stage(p, originalValue(p, "Expression"), t.name, false);
        }
        for (const role of data.roles || []) {
            const ek = editKey(role.key, "RlsFilters");
            if (staging.edits[ek]) continue;
            const map = JSON.parse(originalValue(role, "RlsFilters"));
            let changed = false;
            for (const [table, filter] of Object.entries(map)) {
                if (!mentions(filter)) continue;
                const rewritten = rewriteDax(filter, renames, table, true);
                if (rewritten !== filter) { map[table] = rewritten; changed = true; }
            }
            if (changed) {
                staging.edits[ek] = {
                    key: role.key, target: targetOf(role), property: "RlsFilters", label: "Row-level security",
                    objectType: "role", oldValue: originalValue(role, "RlsFilters"), newValue: JSON.stringify(map), cascade: true,
                };
            }
        }
    }
    function toggleOp(node, op) {
        if (saving) return;
        pushUndo("");
        const c = createFor(node);
        if (c && op === "delete") {
            const doomed = new Set([c.id]);
            staging.creates.forEach((x) => { if (x.parentKey === node.key) doomed.add(x.id); });
            staging.creates = staging.creates.filter((x) => !doomed.has(x.id));
            if (selectedKey === node.key) selectedKey = node.parent ? node.parent.key : modelKey();
        } else {
            const i = staging.ops.findIndex((o) => o.op === op && o.key === node.key);
            if (i >= 0) staging.ops.splice(i, 1);
            else staging.ops.push({ op, key: node.key, target: targetOf(node), label: objectLabel(node), objectType: node.type });
        }
        afterChange();
    }
    function uniqueName(base, taken) {
        const lower = new Set([...taken].map((n) => String(n).toLowerCase()));
        if (!lower.has(base.toLowerCase())) return base;
        let i = 1;
        while (lower.has(`${base} ${i}`.toLowerCase())) i += 1;
        return `${base} ${i}`;
    }
    function addObject(type, parent) {
        if (saving) return;
        if (mode !== "write") setMode("write");
        let base = "New object";
        let taken = [];
        const tables = [...(data.tables || []), ...newNodes("table", ""), ...newNodes("fieldParameter", "")];
        if (type === "measure") {
            base = "New measure";
            for (const t of tables) for (const m of [...(t.measures || []), ...newNodes("measure", t.key)]) taken.push(effName(m));
        } else if (type === "column") {
            base = "New column";
            taken = [...(parent.columns || []), ...newNodes("column", parent.key)].map(effName);
        } else if (type === "calculationItem") {
            base = "New item";
            taken = [...(parent.calculation_items || []), ...newNodes("calculationItem", parent.key)].map(effName);
        } else if (type === "table") {
            base = "Calculation group";
            taken = tables.map(effName);
        } else {
            const folder = { role: "roles", perspective: "perspectives", expression: "expressions" }[type];
            base = { role: "New role", perspective: "New perspective", expression: "New expression" }[type];
            taken = [...(data[folder] || []), ...newNodes(type, "")].map(effName);
        }
        pushUndo("");
        const id = ++createSeq + "-" + Date.now().toString(36);
        staging.creates.push({ id, type, parentKey: parent ? parent.key : "", name: uniqueName(base, taken), props: {} });
        if (type === "table") {
            staging.creates.push({ id: id + "-item", type: "calculationItem", parentKey: "new:" + id, name: "Item", props: {} });
            expanded.add("new:" + id);
        }
        if (parent) expanded.add(parent.key);
        expanded.add("model");
        if (type === "table") expanded.add("folder:tables");
        else if (!parent) expanded.add("folder:" + { role: "roles", perspective: "perspectives", expression: "expressions" }[type]);
        selectedKey = "new:" + id;
        afterChange();
        scrollToSelected();
    }
    function discardAll() {
        if (saving) return;
        if (!pendingCount()) return;
        pushUndo("");
        staging = { edits: {}, ops: [], creates: [] };
        afterChange();
    }
    function revertChange(item) {
        if (saving) return;
        pushUndo("");
        if (item.kind === "edit") delete staging.edits[item.id];
        else if (item.kind === "op") staging.ops = staging.ops.filter((o) => !(o.op === item.op && o.key === item.key));
        else if (item.kind === "create") staging.creates = staging.creates.filter((c) => c.id !== item.id && c.parentKey !== "new:" + item.id);
        recomputeCascade();
        afterChange();
    }
    function pendingItems() {
        const items = [];
        for (const c of staging.creates) {
            if (c.type === "daxlibPackage") {
                items.push({ kind: "create", id: c.id, detail: "",
                    title: `${c.props.installed ? "Update" : "Add"} DAXLib package '${c.name}' (v${c.props.version}, ${plural(c.props.functions.length, "function")})`
                        + (c.props.upgradeCompatibility ? ` and upgrade to compatibility level ${DAXLIB_MIN_COMPAT}` : "") });
                continue;
            }
            const node = syntheticNode(c);
            const title = c.type === "table" ? `Add calculation group '${c.name}'`
                : c.type === "fieldParameter" ? `Add field parameter '${c.name}'`
                : `Add ${TYPE_LABEL[c.type].toLowerCase()} ${objectLabel(node)}`;
            items.push({ kind: "create", id: c.id, title, detail: "" });
        }
        for (const [id, e] of Object.entries(staging.edits)) {
            const node = getNode(e.key);
            const label = node ? objectLabel(node) : e.target.name;
            const show = (v) => {
                const s = v === null || v === undefined || v === "" ? "(blank)" : String(v);
                return s.length > 240 ? s.slice(0, 240) + "\u2026" : s;
            };
            items.push({
                kind: "edit", id, cascade: !!e.cascade,
                title: `${TYPE_LABEL[e.objectType] || e.objectType} ${label} \u2014 ${e.label}${e.cascade ? " (auto-updated reference)" : ""}`,
                detail: JSON_PROPS.has(e.property) || e.property.endsWith("Expression") ? "" : `${show(e.oldValue)} \u2192 ${show(e.newValue)}`,
            });
        }
        for (const o of staging.ops) {
            items.push({ kind: "op", op: o.op, key: o.key, title: `${o.op === "delete" ? "Delete" : "Duplicate"} ${(TYPE_LABEL[o.objectType] || "").toLowerCase()} ${o.label}`, detail: "" });
        }
        return items;
    }
    function savePayload() {
        const renames = currentRenames();
        const creates = staging.creates.map((c) => {
            const parent = c.parentKey ? getNode(c.parentKey) : null;
            const table = parent ? (parent.isNew ? effName(parent) : parent.name) : "";
            const props = Object.assign({}, c.props);
            if (renames.length && ["measure", "column", "calculationItem", "fieldParameter"].includes(c.type)) {
                for (const prop of DAX_PROPS) {
                    if (props[prop]) props[prop] = rewriteDax(String(props[prop]), renames, parent ? parent.name : "", c.type === "column");
                }
            }
            return { type: c.type, name: c.name, table, props };
        });
        const deleted = new Set(staging.ops.filter((o) => o.op === "delete").map((o) => o.key));
        const edits = Object.values(staging.edits)
            .filter((e) => !deleted.has(e.key))
            .map((e) => ({ target: e.target, property: e.property, value: e.newValue, label: e.label }));
        const ops = staging.ops.map((o) => ({ op: o.op, target: o.target }));
        return { creates, edits, ops };
    }
    function save() {
        if (saving || !pendingCount() || model.get("busy")) return;
        const sel = getNode(selectedKey);
        pendingSelect = sel ? { type: sel.type, name: effName(sel), table: sel.table ? tableDisplayName(sel.table) : "",
            hierarchy: sel.hierarchy || "" } : null;
        const changes = pendingItems();
        const payload = savePayload();
        saving = true;
        closeContextMenu();
        request("save", payload, (res) => {
            saving = false;
            if (res.ok) {
                history.unshift({ savedAt: new Date(), changes });
                staging = { edits: {}, ops: [], creates: [] };
                undoStack = []; redoStack = [];
            }
            renderAll();
        });
        if (saving) renderAll();
    }

    let treeTimer = null;
    function afterChange(opts) {
        const o = opts || {};
        renderHeader();
        if (o.keepDetail) {
            // Typing in a code editor: refresh the tree's unsaved dots lazily.
            if (treeTimer) clearTimeout(treeTimer);
            treeTimer = setTimeout(() => { treeTimer = null; renderTree(); }, 250);
            refreshToolActions();
            return;
        }
        renderTree();
        renderDetail();
        redrawTools();
    }

    // ------------------------------------------------------------------
    // Layout skeleton
    // ------------------------------------------------------------------
    const header = h("div", "slls-sx-header");
    const pickerHost = h("div", "slls-sx-picker");
    const body = h("div", "slls-sx-body");
    const statusBar = h("div", "slls-sx-status");
    const footer = h("div", "slls-sx-footer");
    footer.innerHTML = `__SLLS_ATTRIBUTION__`;
    root.append(header, pickerHost, body, statusBar, footer);

    const treePane = h("div", "slls-sx-tree-pane");
    const dragHandle = h("div", "slls-sx-drag");
    const detailPane = h("div", "slls-sx-detail");
    const tree = h("div", "slls-sx-tree");
    tree.setAttribute("role", "tree");

    // ------------------------------------------------------------------
    // Header
    // ------------------------------------------------------------------
    const fsBtn = h("button", "sl-theme-btn");
    fsBtn.type = "button";
    sllsSetupFullscreen(root, fsBtn, "slls-sx-fs", ICON.fullscreen, ICON.fullscreen_exit);
    const themeBtn = h("button", "sl-theme-btn");
    themeBtn.type = "button";
    themeBtn.addEventListener("click", () => {
        model.set("dark_mode", !(model.get("dark_mode") === true));
        model.save_changes();
    });
    function renderThemeBtn() {
        const dark = model.get("dark_mode") === true;
        themeBtn.innerHTML = dark ? ICON.sun : ICON.moon;
        themeBtn.title = dark ? "Switch to light mode" : "Switch to dark mode";
        themeBtn.setAttribute("aria-label", themeBtn.title);
    }
    model.on("change:dark_mode", renderThemeBtn);
    renderThemeBtn();

    function kind() { return model.get("explorer_kind") || ""; }
    function viewOpen() { return kind() === "view" && !!(viewData && viewData.view); }
    function loaded() { return kind() !== "view" && model.get("dataset_chosen") === true && data && data.model; }
    function setMode(next) {
        if (saving) return;
        if (next === "read" && pendingCount()) {
            confirmDialog(`Discard ${plural(pendingCount(), "pending change")} and switch to read-only mode?`, "Discard", () => {
                staging = { edits: {}, ops: [], creates: [] };
                undoStack = []; redoStack = [];
                if (getNode(selectedKey) === null) selectedKey = modelKey();
                mode = "read";
                renderAll();
            });
            return;
        }
        mode = next;
        renderAll();
    }
    function renderHeader() {
        clear(header);
        header.appendChild(iconEl("telescope", "slls-sx-headicon"));
        const titleWrap = h("div", "slls-sx-titlewrap");
        titleWrap.appendChild(h("div", "slls-sx-title", "Semantic Explorer"));
        const sub = h("div", "slls-sx-subtitle");
        const ds = model.get("dataset_name") || "", ws = model.get("workspace_name") || "";
        const sep = '<span class="slls-sx-sep">\u00b7</span>';
        if (viewOpen()) {
            const v = viewData.view;
            sub.innerHTML = `<b>${esc(v.name)}</b>${sep}${esc(v.lakehouse_name)}${sep}${esc(v.workspace_name)}`;
        } else if (model.get("dataset_chosen") !== true) sub.textContent = "No semantic model or semantic view selected";
        else sub.innerHTML = `<b>${esc(ds)}</b>${ds && ws ? sep : ""}${esc(ws)}`;
        sub.title = sub.textContent;
        titleWrap.appendChild(sub);
        header.appendChild(titleWrap);
        if (viewOpen()) {
            if (!pickerOpen) renderViewHeaderControls();
            header.appendChild(h("div", "slls-sx-head-spacer"));
            header.append(fsBtn, themeBtn);
            return;
        }
        const busy = model.get("busy") || "";
        if (!pickerOpen && model.get("dataset_chosen") === true) {
            const change = button("sl-change-btn", "", "swap", () => openPicker(), "Change workspace or semantic model");
            header.appendChild(change);
        }
        if (!pickerOpen && loaded()) {
            const tools = button("slls-sx-btn", "Tools", "more", () => {
                const r = tools.getBoundingClientRect();
                showMenu(r.left, r.bottom + 4, TOOLS.map((t) => [t.label, t.icon, t.open]));
            }, "Model tools");
            tools.setAttribute("aria-haspopup", "menu");
            header.appendChild(tools);
        }
        if (!pickerOpen && loaded()) {
            const group = h("div", "slls-sx-head-group");
            const modeBtn = h("button", "slls-sx-mode" + (mode === "write" ? " write" : ""));
            modeBtn.type = "button";
            modeBtn.setAttribute("role", "switch");
            modeBtn.setAttribute("aria-checked", String(mode === "write"));
            modeBtn.title = mode === "write" ? "Read-write mode (editing enabled)" : "Read-only mode \u2014 click to enable editing";
            modeBtn.innerHTML = (mode === "write" ? ICON.pencil : ICON.eye) + '<span class="slls-sx-track"></span>';
            modeBtn.appendChild(document.createTextNode(mode === "write" ? "Edit" : "Read-only"));
            modeBtn.addEventListener("click", () => setMode(mode === "write" ? "read" : "write"));
            group.appendChild(modeBtn);
            if (mode === "write") {
                const u = button("slls-sx-btn icon", "", "undo", undo, "Undo");
                u.disabled = !undoStack.length || !!busy || saving;
                const r = button("slls-sx-btn icon", "", "redo", redo, "Redo");
                r.disabled = !redoStack.length || !!busy || saving;
                const count = pendingCount();
                const saveTitle = saving ? "Saving changes to the semantic model\u2026"
                    : count ? `Save ${plural(count, "change")} to the semantic model` : "No changes to save";
                const s = button("slls-sx-btn icon", "", saving ? "refresh" : "save", save, saveTitle);
                if (saving) s.querySelector("svg").classList.add("slls-sx-spin");
                s.disabled = !count || !!busy || saving;
                group.append(u, r, s);
            }
            const hist = button("slls-sx-btn icon", "", "history", openHistory, "Change history");
            group.appendChild(hist);
            header.appendChild(group);
            const errs = (data.errors || []).length;
            const errBtn = button("slls-sx-btn errors" + (errs ? " has" : ""), String(errs), "alert", openErrors,
                `Model errors (${errs})`);
            header.appendChild(errBtn);
        }
        header.appendChild(h("div", "slls-sx-head-spacer"));
        header.append(fsBtn, themeBtn);
    }

    // ------------------------------------------------------------------
    // Picker
    // ------------------------------------------------------------------
    function openPicker() {
        if (kind() === "view" && viewPendingCount()) {
            confirmDialog(`Changing the semantic view discards ${plural(viewPendingCount(), "pending change")}.`, "Discard", () => {
                discardViewChanges();
                openPicker();
            });
            return;
        }
        if (pendingCount()) {
            confirmDialog(`Changing the semantic model discards ${plural(pendingCount(), "pending change")}.`, "Discard", () => {
                staging = { edits: {}, ops: [], creates: [] };
                undoStack = []; redoStack = [];
                openPicker();
            });
            return;
        }
        pickerOpen = true;
        pickerWs = model.get("active_workspace_id") || pickerWs;
        pickerDs = model.get("active_dataset_id") || "";
        pickerStep = kind() || "landing";
        if (viewOpen()) {
            const v = viewData.view;
            viewWs = v.workspace_id; viewLh = v.lakehouse_id; viewPick = v.name;
            viewPickerMode = "existing";
        }
        renderAll();
    }
    function closePicker() {
        pickerOpen = false;
        pickerMode = "existing";
        viewPickerMode = "existing";
        renderAll();
    }
    // Back from a model / view picker to the landing screen.
    function pickerBack(card, disabled) {
        const back = button("slls-sx-iconbtn slls-sx-picker-back", "", "arrow_left", () => {
            pickerStep = "landing";
            createError = null; viewError = null;
            renderPicker();
        }, "Back");
        back.disabled = !!disabled;
        return back;
    }
    function ensureWorkspaces() {
        if (workspacesRequested) return;
        workspacesRequested = true;
        model.set("picker_loading", true);
        model.save_changes();
        dispatch({ action: "list_workspaces", workspace_id: pickerWs });
    }
    function renderPicker() {
        // Requesting the workspaces fires change events which re-enter this
        // function, so it must happen before the DOM is rebuilt.
        if (pickerOpen) ensureWorkspaces();
        clear(pickerHost);
        pickerHost.style.display = pickerOpen ? "" : "none";
        if (!pickerOpen) return;
        if (pickerStep === "landing") { renderLanding(); return; }
        if (pickerStep === "view") { renderViewPicker(); return; }
        const loading = model.get("picker_loading") === true;
        const connecting = model.get("busy") === "connect";
        const card = h("div", "slls-sx-picker-card");
        const top = h("div", "");
        top.style.cssText = "display:flex;align-items:flex-start;justify-content:space-between;gap:12px";
        const isNew = pickerMode === "new";
        const tt = h("div", "slls-sx-picker-head");
        const titles = h("div", "");
        titles.append(h("div", "slls-sx-picker-title", isNew ? "Create a semantic model" : "Select a semantic model"),
            h("div", "slls-sx-picker-sub", isNew ? "Create a new, empty semantic model and open it in the explorer."
                : "Choose the workspace and semantic model to explore."));
        tt.append(pickerBack(card, connecting || creating), titles);
        const reload = button("sl-reload-btn" + (loading ? " sl-spinning" : ""), "", "refresh", () => {
            workspacesRequested = false;
            ensureWorkspaces();
        }, "Reload workspaces and semantic models");
        reload.disabled = loading || connecting || creating;
        const right = h("div", "slls-sx-picker-top");
        const seg = h("div", "slls-sx-seg");
        seg.setAttribute("role", "radiogroup");
        seg.setAttribute("aria-label", "Semantic model source");
        for (const [id, label] of [["existing", "Existing model"], ["new", "New model"]]) {
            const b = h("button", pickerMode === id ? "on" : "", label);
            b.type = "button";
            b.setAttribute("role", "radio");
            b.setAttribute("aria-checked", String(pickerMode === id));
            b.disabled = connecting || creating;
            b.addEventListener("click", () => { pickerMode = id; createError = null; renderPicker(); });
            seg.appendChild(b);
        }
        right.append(seg, reload);
        top.append(tt, right);
        card.appendChild(top);
        if (isNew) {
            renderNewModelForm(card, loading);
            pickerHost.appendChild(card);
            return;
        }
        const row = h("div", "slls-sx-picker-row");
        const wsField = h("div", "slls-sx-field");
        wsField.appendChild(h("label", "", "Workspace"));
        const dsField = h("div", "slls-sx-field");
        dsField.appendChild(h("label", "", "Semantic model"));
        const wsSel = createSearchSelect({
            placeholder: "Select a workspace\u2026", searchPlaceholder: "Filter workspaces\u2026",
            ariaLabel: "Workspace", emptyLabel: loading ? "Loading workspaces\u2026" : "No workspaces",
            onChange: (o) => {
                pickerWs = o.value; pickerDs = "";
                model.set("available_datasets", []);
                model.set("picker_loading", true);
                model.save_changes();
                dispatch({ action: "list_datasets", workspace_id: pickerWs });
                renderPicker();
            },
        });
        wsSel.setOptions((model.get("available_workspaces") || []).map((w) => ({ value: w.id, label: w.name })), pickerWs);
        wsSel.setDisabled(connecting);
        const datasets = model.get("available_datasets") || [];
        const dsSel = createSearchSelect({
            placeholder: "Select a semantic model\u2026", searchPlaceholder: "Filter semantic models\u2026",
            ariaLabel: "Semantic model",
            emptyLabel: !pickerWs ? "Select a workspace first\u2026" : (loading ? "Loading semantic models\u2026" : "No semantic models"),
            onChange: (o) => { pickerDs = o.value; renderPicker(); },
        });
        dsSel.setOptions(datasets.map((d) => ({ value: d.id, label: d.name })), pickerDs);
        dsSel.setDisabled(!pickerWs || connecting);
        wsField.appendChild(wsSel.el);
        dsField.appendChild(dsSel.el);
        row.append(wsField, dsField);
        card.appendChild(row);
        const actions = h("div", "slls-sx-picker-actions");
        if (kind()) actions.appendChild(button("slls-sx-btn", "Cancel", "", closePicker));
        const connect = button("slls-sx-btn primary", connecting ? "Connecting\u2026" : "Connect", "", () => {
            if (!pickerWs || !pickerDs) return;
            request("connect", { workspace_id: pickerWs, dataset_id: pickerDs }, (res) => {
                if (res.ok) {
                    pickerOpen = false;
                    staging = { edits: {}, ops: [], creates: [] };
                    undoStack = []; redoStack = []; history = [];
                    selectedKey = modelKey();
                    expanded = new Set(["model", "folder:tables"]);
                }
                renderAll();
            });
            renderAll();
        });
        connect.disabled = !pickerWs || !pickerDs || connecting;
        actions.appendChild(connect);
        card.appendChild(actions);
        pickerHost.appendChild(card);
    }

    function renderNewModelForm(card, loading) {
        const row = h("div", "slls-sx-picker-row");
        const wsField = h("div", "slls-sx-field");
        wsField.appendChild(h("label", "", "Workspace"));
        const wsSel = createSearchSelect({
            placeholder: "Select a workspace\u2026", searchPlaceholder: "Filter workspaces\u2026",
            ariaLabel: "Workspace", emptyLabel: loading ? "Loading workspaces\u2026" : "No workspaces",
            onChange: (o) => {
                pickerWs = o.value; pickerDs = "";
                model.set("available_datasets", []);
                model.set("picker_loading", true);
                model.save_changes();
                dispatch({ action: "list_datasets", workspace_id: pickerWs });
                renderPicker();
            },
        });
        wsSel.setOptions((model.get("available_workspaces") || []).map((w) => ({ value: w.id, label: w.name })), pickerWs);
        wsSel.setDisabled(creating);
        wsField.appendChild(wsSel.el);
        const nameField = h("div", "slls-sx-field");
        nameField.appendChild(h("label", "", "New model name"));
        const input = h("input", "slls-sx-input");
        input.value = newModelName;
        input.placeholder = "e.g. Sales Model";
        input.setAttribute("aria-label", "New model name");
        input.disabled = creating;
        nameField.appendChild(input);
        row.append(wsField, nameField);
        card.appendChild(row);
        // The workspace's models are listed so a taken name is caught early.
        const taken = () => (model.get("available_datasets") || []).some((d) => d.name.toLowerCase() === newModelName.trim().toLowerCase());
        const hint = h("div", "slls-sx-picker-note");
        const err = h("div", "slls-sx-picker-error");
        card.append(hint, err);
        const actions = h("div", "slls-sx-picker-actions");
        if (kind()) actions.appendChild(button("slls-sx-btn", "Cancel", "", closePicker));
        const create = button("slls-sx-btn primary", creating ? "Creating\u2026" : "Create model", "", () => {
            const name = newModelName.trim();
            if (!pickerWs || !name || taken() || creating) return;
            creating = true;
            createError = null;
            request("create_model", { workspace_id: pickerWs, name }, (res) => {
                creating = false;
                if (res.ok) {
                    pickerOpen = false;
                    pickerMode = "existing";
                    pickerDs = res.data.dataset_id || "";
                    newModelName = "";
                    staging = { edits: {}, ops: [], creates: [] };
                    undoStack = []; redoStack = []; history = [];
                    selectedKey = modelKey();
                    expanded = new Set(["model", "folder:tables"]);
                    mode = "write";
                } else createError = res.error || "The semantic model could not be created.";
                renderAll();
            });
            renderPicker();
        });
        actions.appendChild(create);
        card.appendChild(actions);
        const refresh = () => {
            const clash = !!newModelName.trim() && taken();
            hint.textContent = clash
                ? `A semantic model named \u201c${newModelName.trim()}\u201d already exists in this workspace.`
                : `Creates a new, empty semantic model (compatibility level ${NEW_MODEL_COMPATIBILITY_LEVEL}) in the selected workspace. `
                    + "Choose a name that isn\u2019t already used there.";
            hint.classList.toggle("error", clash);
            input.classList.toggle("invalid", clash);
            err.textContent = createError || "";
            err.style.display = createError ? "" : "none";
            create.disabled = creating || !pickerWs || !newModelName.trim() || clash;
        };
        input.addEventListener("input", () => { newModelName = input.value; createError = null; refresh(); });
        input.addEventListener("keydown", (ev) => { if (ev.key === "Enter") create.click(); });
        refresh();
        if (!creating) setTimeout(() => { if (input.isConnected && document.activeElement !== input && !newModelName) input.focus(); }, 0);
    }

    // ------------------------------------------------------------------
    // Landing: semantic model or semantic view
    // ------------------------------------------------------------------
    function renderLanding() {
        const card = h("div", "slls-sx-picker-card slls-sx-landing");
        card.append(h("div", "slls-sx-picker-title", "What would you like to explore?"),
            h("div", "slls-sx-picker-sub", "Open or create a semantic model, or a semantic view defined in a lakehouse."));
        const choices = h("div", "slls-sx-choices");
        const choice = (id, icon, title, desc, cta, badge) => {
            const b = h("button", "slls-sx-choice");
            b.type = "button";
            b.dataset.choice = id;
            const head = h("div", "slls-sx-choice-head");
            head.append(iconEl(icon, "slls-sx-objicon"), h("span", "", title));
            if (badge) head.appendChild(h("span", "slls-sx-pill", badge));
            const meta = h("div", "slls-sx-choice-meta", cta);
            meta.insertAdjacentHTML("beforeend", ICON.chevron_right);
            b.append(head, h("div", "slls-sx-choice-desc", desc), meta);
            b.addEventListener("click", () => {
                pickerStep = id;
                createError = null; viewError = null;
                renderPicker();
            });
            return b;
        };
        choices.append(
            choice("model", "database", "Semantic model",
                "Browse and edit a semantic model: tables, columns, measures, relationships, security, perspectives, translations and more.",
                "Open or create a semantic model"),
            choice("view", "layers", "Semantic view",
                "Explore the datasets, relationships and metrics that a semantic view defines in YAML in a lakehouse\u2019s Context folder.",
                "Open or create a semantic view", "Preview"));
        card.appendChild(choices);
        if (kind()) {
            const actions = h("div", "slls-sx-picker-actions");
            actions.appendChild(button("slls-sx-btn", "Cancel", "", closePicker));
            card.appendChild(actions);
        }
        pickerHost.appendChild(card);
    }

    // ------------------------------------------------------------------
    // Semantic view picker (workspace -> lakehouse -> view, or a new view)
    // ------------------------------------------------------------------
    function viewBusy() { const b = model.get("busy"); return b === "open_view" || b === "create_view"; }
    function loadLakehouses(force) {
        if (!viewWs || (!force && lakehouseList.ws === viewWs)) return;
        const ws = viewWs;
        lakehouseList = { ws, items: [], loading: true, error: "" };
        request("list_lakehouses", { workspace_id: ws }, (res) => {
            if (lakehouseList.ws !== ws) return;
            lakehouseList = { ws, items: res.ok ? (res.data.items || []) : [], loading: false,
                error: res.ok ? "" : (res.error || "The lakehouses could not be listed.") };
            if (viewLh && !lakehouseList.items.some((l) => l.id === viewLh)) { viewLh = ""; viewPick = ""; }
            if (pickerOpen && pickerStep === "view") renderPicker();
        });
    }
    function viewListKey() { return viewWs + "/" + viewLh; }
    function loadViews(force) {
        if (!viewWs || !viewLh || (!force && viewList.key === viewListKey())) return;
        const key = viewListKey();
        viewList = { key, items: [], loading: true, error: "" };
        request("list_views", { workspace_id: viewWs, lakehouse_id: viewLh }, (res) => {
            if (viewList.key !== key) return;
            viewList = { key, items: res.ok ? (res.data.items || []) : [], loading: false,
                error: res.ok ? "" : (res.error || "The semantic views could not be listed.") };
            if (viewPick && !viewList.items.some((v) => v.name === viewPick)) viewPick = "";
            if (pickerOpen && pickerStep === "view") renderPicker();
        });
    }
    function segmented(label, options, value, disabled, onPick) {
        const seg = h("div", "slls-sx-seg");
        seg.setAttribute("role", "radiogroup");
        seg.setAttribute("aria-label", label);
        for (const [id, text] of options) {
            const b = h("button", value === id ? "on" : "", text);
            b.type = "button";
            b.setAttribute("role", "radio");
            b.setAttribute("aria-checked", String(value === id));
            b.disabled = !!disabled;
            b.addEventListener("click", () => onPick(id));
            seg.appendChild(b);
        }
        return seg;
    }
    function pickerField(label, control) {
        const f = h("div", "slls-sx-field");
        f.append(h("label", "", label), control);
        return f;
    }
    function viewOpened() {
        pickerOpen = false;
        viewPickerMode = "existing";
        vSelected = "view";
        vExpanded = new Set(["view", "v:datasets", "v:relationships", "v:metrics"]);
        vSearch = "";
        propFilter = "";
        vMode = "read";
        vTables = null;
        vColCache = {};
    }
    function renderViewPicker() {
        loadLakehouses(false);
        loadViews(false);
        const wsLoading = model.get("picker_loading") === true;
        const busy = viewBusy();
        const isNew = viewPickerMode === "new";
        const card = h("div", "slls-sx-picker-card slls-sx-view-picker");
        const top = h("div", "slls-sx-picker-toprow");
        const tt = h("div", "slls-sx-picker-head");
        const titles = h("div", "");
        titles.append(h("div", "slls-sx-picker-title", isNew ? "Create a semantic view" : "Open a semantic view"),
            h("div", "slls-sx-picker-sub", isNew
                ? "Create a semantic view in a lakehouse, starting from a YAML definition file."
                : "Choose the workspace, lakehouse and semantic view to explore."));
        tt.append(pickerBack(card, busy), titles);
        const right = h("div", "slls-sx-picker-top");
        const seg = segmented("Semantic view source", [["existing", "Existing view"], ["new", "New view"]], viewPickerMode, busy,
            (id) => { viewPickerMode = id; viewError = null; renderPicker(); });
        const spinning = wsLoading || lakehouseList.loading || viewList.loading;
        const reload = button("sl-reload-btn" + (spinning ? " sl-spinning" : ""), "", "refresh", () => {
            workspacesRequested = false;
            ensureWorkspaces();
            loadLakehouses(true);
            loadViews(true);
            renderPicker();
        }, "Reload workspaces, lakehouses and semantic views");
        reload.disabled = busy || spinning;
        right.append(seg, reload);
        top.append(tt, right);
        card.appendChild(top);

        const row = h("div", "slls-sx-picker-row three");
        const wsSel = createSearchSelect({
            placeholder: "Select a workspace\u2026", searchPlaceholder: "Filter workspaces\u2026",
            ariaLabel: "Workspace", emptyLabel: wsLoading ? "Loading workspaces\u2026" : "No workspaces",
            onChange: (o) => {
                viewWs = o.value; viewLh = ""; viewPick = ""; viewError = null;
                loadLakehouses(true);
                renderPicker();
            },
        });
        wsSel.setOptions((model.get("available_workspaces") || []).map((w) => ({ value: w.id, label: w.name })), viewWs);
        wsSel.setDisabled(busy);
        const lhSel = createSearchSelect({
            placeholder: "Select a lakehouse\u2026", searchPlaceholder: "Filter lakehouses\u2026", ariaLabel: "Lakehouse",
            emptyLabel: !viewWs ? "Select a workspace first\u2026"
                : (lakehouseList.loading ? "Loading lakehouses\u2026" : (lakehouseList.error || "No lakehouses in this workspace")),
            onChange: (o) => {
                viewLh = o.value; viewPick = ""; viewError = null;
                loadViews(true);
                renderPicker();
            },
        });
        lhSel.setOptions(lakehouseList.items.map((l) => ({ value: l.id, label: l.name })), viewLh);
        lhSel.setDisabled(!viewWs || busy);
        row.append(pickerField("Workspace", wsSel.el), pickerField("Lakehouse", lhSel.el));
        const listed = viewList.key === viewListKey() && !viewList.loading;

        const note = h("div", "slls-sx-picker-note");
        const err = h("div", "slls-sx-picker-error");
        err.textContent = viewError || "";
        err.style.display = viewError ? "" : "none";
        const actions = h("div", "slls-sx-picker-actions");
        if (kind()) actions.appendChild(button("slls-sx-btn", "Cancel", "", closePicker));

        if (!isNew) {
            const vSel = createSearchSelect({
                placeholder: "Select a semantic view\u2026", searchPlaceholder: "Filter semantic views\u2026", ariaLabel: "Semantic view",
                emptyLabel: !viewLh ? "Select a lakehouse first\u2026"
                    : (viewList.loading ? "Loading semantic views\u2026" : (viewList.error || "No semantic views")),
                onChange: (o) => { viewPick = o.value; viewError = null; renderPicker(); },
            });
            vSel.setOptions(viewList.items.map((v) => ({ value: v.name, label: v.name })), viewPick);
            vSel.setDisabled(!viewLh || busy);
            row.appendChild(pickerField("Semantic view", vSel.el));
            card.append(row, note, err);
            if (viewList.error && viewLh) {
                note.textContent = viewList.error;
                note.classList.add("error");
            } else if (viewLh && listed && !viewList.items.length) {
                note.appendChild(document.createTextNode("This lakehouse has no semantic views yet. "));
                const link = h("a", "slls-sx-link", "Create a semantic view");
                link.addEventListener("click", () => { viewPickerMode = "new"; viewError = null; renderPicker(); });
                note.appendChild(link);
            } else {
                note.textContent = "Semantic views are the folders in the lakehouse\u2019s Context/Semantic Views folder.";
            }
            const open = button("slls-sx-btn primary", model.get("busy") === "open_view" ? "Opening\u2026" : "Open", "", () => {
                if (!viewWs || !viewLh || !viewPick || viewBusy()) return;
                viewError = null;
                request("open_view", { workspace_id: viewWs, lakehouse_id: viewLh, view: viewPick }, (res) => {
                    if (res.ok) viewOpened();
                    else viewError = res.error || "The semantic view could not be opened.";
                    renderAll();
                });
                renderPicker();
            });
            open.disabled = !viewWs || !viewLh || !viewPick || busy;
            actions.appendChild(open);
            card.appendChild(actions);
            pickerHost.appendChild(card);
            return;
        }

        const input = h("input", "slls-sx-input");
        input.value = newViewName;
        input.placeholder = "e.g. sales";
        input.setAttribute("aria-label", "New semantic view name");
        input.disabled = busy;
        row.appendChild(pickerField("View name", input));
        card.append(row, note, err);
        const create = button("slls-sx-btn primary", model.get("busy") === "create_view" ? "Creating\u2026" : "Create view", "", () => {
            const name = newViewName.trim();
            if (create.disabled || viewBusy()) return;
            viewError = null;
            request("create_view", { workspace_id: viewWs, lakehouse_id: viewLh, name }, (res) => {
                if (res.ok) {
                    newViewName = "";
                    viewPick = res.data.view || name;
                    viewList.key = null;
                    viewOpened();
                } else viewError = res.error || "The semantic view could not be created.";
                renderAll();
            });
            renderPicker();
        });
        actions.appendChild(create);
        card.appendChild(actions);
        const refresh = () => {
            const name = newViewName.trim();
            const clash = !!name && listed && viewList.items.some((v) => v.name.toLowerCase() === name.toLowerCase());
            const invalid = /[\\/:*?"<>|#%\u0000-\u001f]/.test(name) || name.endsWith(".");
            note.classList.toggle("error", clash || invalid);
            input.classList.toggle("invalid", clash || invalid);
            if (clash) note.textContent = `A semantic view named \u201c${name}\u201d already exists in this lakehouse.`;
            else if (invalid) note.textContent = "A semantic view name can\u2019t contain \\ / : * ? \" < > | # % or end with a period.";
            else if (name) {
                note.textContent = `Creates Context/Semantic Views/${name}/${name}_semantic_view.yaml in the lakehouse. `
                    + "Publishing the view creates a semantic model with the same name.";
            } else note.textContent = "The view name is the name of its folder in Context/Semantic Views and of the semantic model it publishes.";
            create.disabled = busy || !viewWs || !viewLh || !name || clash || invalid;
        };
        input.addEventListener("input", () => { newViewName = input.value; viewError = null; err.style.display = "none"; refresh(); });
        input.addEventListener("keydown", (ev) => { if (ev.key === "Enter") create.click(); });
        input.addEventListener("focus", () => { viewNameFocused = true; });
        input.addEventListener("blur", () => { if (input.isConnected) viewNameFocused = false; });
        refresh();
        pickerHost.appendChild(card);
        // The picker re-renders as lists load; keep the caret in the name box.
        if (!busy && (viewNameFocused || (!newViewName && viewLh))) {
            setTimeout(() => {
                if (!input.isConnected || document.activeElement === input) return;
                if (root.querySelector(".slls-ss-open")) return;
                input.focus();
                input.setSelectionRange(input.value.length, input.value.length);
            }, 0);
        }
    }

    // ------------------------------------------------------------------
    // Semantic view explorer (read-only / edit)
    // ------------------------------------------------------------------
    // A view is edited as a working copy of its datasets, relationships and
    // metrics. Each object keeps a stable id plus the YAML file and list
    // position it came from; edits are staged (undo / redo are snapshots of
    // the working copy) and only written to the YAML files on Save.
    const VIEW_TYPE_LABEL = { view: "Semantic view", dataset: "Dataset", column: "Column", relationship: "Relationship", metric: "Metric" };
    const VIEW_ICON = { view: "layers", dataset: "table", column: "column", relationship: "relationship", metric: "measure" };
    const VIEW_LISTS = { dataset: "datasets", relationship: "relationships", metric: "metrics" };
    const VIEW_FIELDS = {
        datasets: ["name", "source", "description"],
        relationships: ["name", "from", "to", "from_column", "to_column"],
        metrics: ["name", "description", "dax"],
    };
    function vKey() { return Array.from(arguments).join(KEY_SEP); }
    function viewObjects(p) {
        const take = (type, prefix) => (o, i) => Object.assign({}, o, { id: prefix + i, key: prefix + i, type, deleted: false, isNew: false });
        return {
            description: (p.view && p.view.description) || "",
            datasets: (p.datasets || []).map(take("dataset", "ds")),
            relationships: (p.relationships || []).map(take("relationship", "rel")),
            metrics: (p.metrics || []).map(take("metric", "met")),
        };
    }
    function onViewData() {
        viewData = model.get("view_data") || null;
        vBase = viewData && viewData.view ? viewObjects(viewData) : null;
        vWork = vBase ? JSON.parse(JSON.stringify(vBase)) : null;
        vUndo = []; vRedo = []; vEditKey = "";
        rebuildViewIndex();
        if (vPendingSelect) {
            const p = vPendingSelect;
            vPendingSelect = null;
            const hit = [...vIndex.values()].find((n) => n.type === p.type && n.name === p.name);
            if (hit) vSelected = hit.key;
        }
        if (!vIndex.has(vSelected)) vSelected = "view";
    }
    function vAll(kind) { return vWork ? vWork[kind] : []; }
    function vLive(kind) { return vAll(kind).filter((o) => !o.deleted); }
    function vBaseOf(o) { return o.isNew || !vBase ? null : (vBase[VIEW_LISTS[o.type]] || []).find((b) => b.id === o.id) || null; }
    function vEditable(o) { return !!o && (o.type === "view" || ((o.isNew || !!o.file) && o.type !== "column")); }
    function columnsFor(d) {
        const base = vBaseOf(d);
        if (base && base.source === d.source) return base.columns || [];
        const cached = vColCache[String(d.source || "").toLowerCase()];
        return cached && cached.columns ? cached.columns : [];
    }
    function rebuildViewIndex() {
        vIndex = new Map();
        vColumns = new Map();
        if (!vWork) return;
        vIndex.set("view", Object.assign({}, viewData.view, { key: "view", type: "view", description: vWork.description }));
        for (const d of vWork.datasets) {
            const cols = columnsFor(d).map((c) => Object.assign({}, c, { key: vKey("col", d.id, c.name), type: "column", dataset: d.name, datasetKey: d.key }));
            vColumns.set(d.key, cols);
            vIndex.set(d.key, d);
            for (const c of cols) vIndex.set(c.key, c);
        }
        for (const r of vWork.relationships) vIndex.set(r.key, r);
        for (const m of vWork.metrics) vIndex.set(m.key, m);
    }
    function vCols(d) { return vColumns.get(d.key) || []; }
    const byName = (a, b) => String(a.name).localeCompare(String(b.name), undefined, { sensitivity: "base" });
    function vNodes(type, withDeleted) {
        return vAll(VIEW_LISTS[type]).filter((o) => withDeleted || !o.deleted).slice().sort(byName);
    }
    function vDataset(name) { return vLive("datasets").find((d) => ciEq(d.name, name)) || null; }
    function vFind(key) {
        for (const kind of Object.values(VIEW_LISTS)) {
            const o = vAll(kind).find((x) => x.key === key);
            if (o) return o;
        }
        return null;
    }
    function fmtBytes(n) {
        const units = ["B", "KB", "MB", "GB"];
        let v = Number(n) || 0, i = 0;
        while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
        return `${v.toFixed(i ? 1 : 0)} ${units[i]}`;
    }
    function viewIssues() { return (viewData && viewData.issues) || []; }

    // Pending changes
    function vIsDirty(o) {
        if (!o || !vWork) return false;
        if (o.type === "view") return (vWork.description || "") !== (vBase.description || "");
        if (o.type === "column") return false;
        if (o.isNew || o.deleted) return true;
        const b = vBaseOf(o);
        return !!b && VIEW_FIELDS[VIEW_LISTS[o.type]].some((f) => (o[f] || "") !== (b[f] || ""));
    }
    function viewChanges() {
        const out = { description: null, updates: [], deletes: [], creates: [] };
        if (!vWork) return out;
        if ((vWork.description || "") !== (vBase.description || "")) out.description = vWork.description || "";
        for (const kind of Object.values(VIEW_LISTS)) {
            for (const o of vWork[kind]) {
                if (o.isNew) {
                    const values = {};
                    for (const f of VIEW_FIELDS[kind]) values[f] = o[f] || "";
                    out.creates.push({ kind, file: o.file, values });
                    continue;
                }
                const b = vBaseOf(o);
                if (!o.file || !b) continue;
                if (o.deleted) { out.deletes.push({ kind, file: o.file, index: o.index, name: b.name }); continue; }
                const values = {};
                for (const f of VIEW_FIELDS[kind]) if ((o[f] || "") !== (b[f] || "")) values[f] = o[f] || "";
                if (Object.keys(values).length) out.updates.push({ kind, file: o.file, index: o.index, name: b.name, values });
            }
        }
        return out;
    }
    function viewPendingCount() {
        const c = viewChanges();
        return (c.description !== null ? 1 : 0) + c.updates.length + c.deletes.length + c.creates.length;
    }
    function viewValidation() {
        const errors = new Map();
        if (!vWork) return errors;
        const add = (o, msg) => { if (!errors.has(o.key)) errors.set(o.key, []); errors.get(o.key).push(msg); };
        const text = (v) => String(v || "").trim();
        const datasets = vLive("datasets"), rels = vLive("relationships"), metrics = vLive("metrics");
        const dup = (items, label, fold) => {
            const seen = new Set();
            for (const o of items) {
                const k = fold ? text(o.name).toLowerCase() : text(o.name);
                if (!k) continue;
                if (seen.has(k)) add(o, `Another ${label} is named \u201c${text(o.name)}\u201d.`);
                seen.add(k);
            }
        };
        for (const d of datasets) {
            if (!vEditable(d)) continue;
            if (!text(d.name)) add(d, "A name is required.");
            if (!text(d.source)) add(d, "A source table is required (e.g. dbo.sales).");
        }
        dup(datasets, "dataset", true);
        for (const r of rels) {
            if (!vEditable(r)) continue;
            if (!text(r.name)) add(r, "A name is required.");
            if (!text(r.from) || !text(r.to)) add(r, "Choose the datasets on both sides.");
            if (!text(r.from_column) || !text(r.to_column)) add(r, "Choose the columns on both sides.");
        }
        dup(rels, "relationship", false);
        for (const m of metrics) {
            if (!vEditable(m)) continue;
            if (!text(m.name)) add(m, "A name is required.");
            if (!text(m.dax) && vIsDirty(m)) add(m, "A DAX expression is required.");
            if (datasets.some((d) => ciEq(text(d.name), text(m.name)))) add(m, "A metric can\u2019t have the same name as a dataset.");
        }
        dup(metrics, "metric", true);
        return errors;
    }
    function viewBlockingErrors() {
        const out = [];
        for (const [key, msgs] of viewValidation()) {
            const n = vIndex.get(key);
            if (n && vIsDirty(n)) out.push(`${n.name || "(unnamed)"}: ${msgs[0]}`);
        }
        return out;
    }

    // Editing
    function vBeginStep(editKey) {
        // Typing in one field is a single undo step: every keystroke re-applies
        // the edit to the state from before the field was first changed, so
        // renames cascade from the original name.
        if (editKey && editKey === vEditKey && vUndo.length) vWork = JSON.parse(vUndo[vUndo.length - 1]);
        else {
            vUndo.push(JSON.stringify(vWork));
            if (vUndo.length > 200) vUndo.shift();
        }
        vEditKey = editKey || "";
        vRedo = [];
    }
    function applyViewField(obj, field, value) {
        const old = obj[field];
        obj[field] = value;
        if (field !== "name" || !old || !value || old === value) return;
        // A rename updates the references to the object, unless the old name
        // is ambiguous.
        if (obj.type === "dataset" && !vLive("datasets").some((d) => d !== obj && ciEq(d.name, old))) {
            for (const r of vLive("relationships")) {
                if (ciEq(r.from, old)) r.from = value;
                if (ciEq(r.to, old)) r.to = value;
            }
            for (const m of vLive("metrics")) if (m.dax) m.dax = rewriteDax(m.dax, [{ type: "table", oldName: old, newName: value }]);
        } else if (obj.type === "metric" && !vLive("metrics").some((m) => m !== obj && ciEq(m.name, old))) {
            for (const m of vLive("metrics")) {
                if (m !== obj && m.dax) m.dax = rewriteDax(m.dax, [{ type: "measure", oldName: old, newName: value, table: "Metrics" }]);
            }
        }
    }
    function vEdit(key, field, value, opts) {
        const o = opts || {};
        vBeginStep(o.typing ? key + "\u0000" + field : "");
        if (key === "view") vWork.description = value;
        else {
            const obj = vFind(key);
            if (obj) applyViewField(obj, field, value);
        }
        vAfterChange(!!o.typing);
    }
    function vAfterChange(typing) {
        rebuildViewIndex();
        if (!vIndex.has(vSelected)) vSelected = "view";
        renderHeader();
        renderViewTree();
        if (typing) refreshViewDetailHead();
        else renderViewDetail();
    }
    function vUndoStep() {
        if (!vUndo.length || vSaving) return;
        vRedo.push(JSON.stringify(vWork));
        vWork = JSON.parse(vUndo.pop());
        vEditKey = "";
        vAfterChange(false);
    }
    function vRedoStep() {
        if (!vRedo.length || vSaving) return;
        vUndo.push(JSON.stringify(vWork));
        vWork = JSON.parse(vRedo.pop());
        vEditKey = "";
        vAfterChange(false);
    }
    function uniqueViewName(kind, base) {
        const names = new Set(vAll(kind).map((o) => String(o.name || "").toLowerCase()));
        let n = base, i = 2;
        while (names.has(n.toLowerCase())) n = `${base} ${i++}`;
        return n;
    }
    // New objects go to the file that already holds that kind of object (or
    // the datasets, or the first YAML file); Save makes the same choice.
    function defaultViewFile(kind) {
        const first = (k) => ((vBase && vBase[k]) || []).find((o) => o.file);
        const f = first(kind) || first("datasets");
        if (f) return f.file;
        const y = ((viewData && viewData.files) || []).find((x) => x.area === "definition" && !x.ignored && !x.error);
        return y ? y.path : "";
    }
    function vAdd(type) {
        const kind = VIEW_LISTS[type];
        vBeginStep("");
        const id = `new-${type}-${++vNewSeq}`;
        const names = { dataset: "new_dataset", relationship: "new_relationship", metric: "New metric" };
        const o = { id, key: id, type, isNew: true, deleted: false, file: defaultViewFile(kind), name: uniqueViewName(kind, names[type]) };
        if (type === "dataset") Object.assign(o, { source: "", description: "" });
        if (type === "relationship") {
            const ds = vLive("datasets");
            Object.assign(o, { from: ds[0] ? ds[0].name : "", to: ds[1] ? ds[1].name : "", from_column: "", to_column: "" });
        }
        if (type === "metric") Object.assign(o, { description: "", dax: "" });
        vWork[kind].push(o);
        vExpanded.add("view");
        vExpanded.add("v:" + kind);
        vSelected = id;
        vAfterChange(false);
    }
    function vToggleDelete(node) {
        vBeginStep("");
        const kind = VIEW_LISTS[node.type];
        if (node.isNew) vWork[kind] = vWork[kind].filter((x) => x.key !== node.key);
        else { const w = vFind(node.key); if (w) w.deleted = !w.deleted; }
        vAfterChange(false);
    }
    function discardViewChanges() {
        if (vBase) vWork = JSON.parse(JSON.stringify(vBase));
        vUndo = []; vRedo = []; vEditKey = "";
        rebuildViewIndex();
        if (!vIndex.has(vSelected)) vSelected = "view";
    }
    function setViewMode(next) {
        if (vSaving) return;
        if (next === "read" && viewPendingCount()) {
            confirmDialog(`Discard ${plural(viewPendingCount(), "pending change")} and switch to read-only mode?`, "Discard", () => {
                discardViewChanges();
                vMode = "read";
                renderAll();
            });
            return;
        }
        vMode = next;
        if (next === "write") ensureViewTables();
        renderAll();
    }
    function viewEtags() {
        const out = {};
        for (const f of (viewData && viewData.files) || []) if (f.area === "definition" && f.etag) out[f.path] = f.etag;
        return out;
    }
    function vSave(publish) {
        if (vSaving || (!viewPendingCount() && !publish)) return;
        if (publish) {
            const name = viewData.view.name;
            const what = viewPendingCount() ? `Save ${plural(viewPendingCount(), "change")} and publish` : "Publish";
            confirmDialog(`${what} the \u201c${name}\u201d semantic view? Publishing creates or updates the \u201c${name}\u201d semantic model `
                + "and then refreshes it (reports and agents using it see the changes).", viewPendingCount() ? "Save and publish" : "Publish", () => runViewSave(true));
            return;
        }
        runViewSave(false);
    }
    function runViewSave(publish) {
        if (vSaving) return;
        const blocking = viewBlockingErrors();
        if (blocking.length) {
            model.set("status", { message: `Fix these before saving: ${blocking.join("; ")}`, kind: "error" });
            model.save_changes();
            return;
        }
        const sel = vIndex.get(vSelected);
        vPendingSelect = sel && sel.type !== "view" ? { type: sel.type, name: sel.name } : null;
        vSaving = true;
        renderAll();
        request("save_view", { changes: viewChanges(), etags: viewEtags(), publish: !!publish }, (res) => {
            vSaving = false;
            if (!res.ok) vPendingSelect = null;
            renderAll();
        });
    }
    // Lakehouse tables offered for dataset sources, and the columns of tables
    // a dataset is pointed at.
    function ensureViewTables() {
        if (vTables !== null) return;
        vTables = [];
        request("view_tables", {}, (res) => {
            vTables = res.ok ? (res.data.items || []) : [];
            const list = root.querySelector("#slls-sx-vtables");
            if (list) fillTableList(list);
        });
    }
    function fillTableList(list) {
        clear(list);
        for (const t of vTables || []) { const o = h("option"); o.value = t; list.appendChild(o); }
    }
    function ensureViewColumns(source) {
        const key = String(source || "").trim().toLowerCase();
        if (!key || vColCache[key]) return;
        if (vAll("datasets").some((d) => { const b = vBaseOf(d); return b && String(b.source).toLowerCase() === key; })) return;
        vColCache[key] = { loading: true, columns: [] };
        request("view_columns", { source: source.trim() }, (res) => {
            vColCache[key] = { loading: false, columns: res.ok ? (res.data.columns || []) : [] };
            if (!viewOpen() || pickerOpen) return;
            rebuildViewIndex();
            renderViewTree();
            renderViewDetail();
        });
    }

    // Header
    function renderViewHeaderControls() {
        header.appendChild(button("sl-change-btn", "", "swap", () => openPicker(), "Change the semantic view, or open a semantic model"));
        const group = h("div", "slls-sx-head-group");
        const write = vMode === "write";
        const modeBtn = h("button", "slls-sx-mode" + (write ? " write" : ""));
        modeBtn.type = "button";
        modeBtn.setAttribute("role", "switch");
        modeBtn.setAttribute("aria-checked", String(write));
        modeBtn.title = write ? "Edit mode (changes are saved to the YAML files)" : "Read-only mode \u2014 click to enable editing";
        modeBtn.innerHTML = (write ? ICON.pencil : ICON.eye) + '<span class="slls-sx-track"></span>';
        modeBtn.appendChild(document.createTextNode(write ? "Edit" : "Read-only"));
        modeBtn.disabled = vSaving;
        modeBtn.addEventListener("click", () => setViewMode(write ? "read" : "write"));
        group.appendChild(modeBtn);
        if (write) {
            const u = button("slls-sx-btn icon", "", "undo", vUndoStep, "Undo");
            u.disabled = !vUndo.length || vSaving;
            const r = button("slls-sx-btn icon", "", "redo", vRedoStep, "Redo");
            r.disabled = !vRedo.length || vSaving;
            const count = viewPendingCount();
            const blocking = count ? viewBlockingErrors() : [];
            const title = vSaving ? "Saving changes to the YAML files\u2026"
                : !count ? "No changes to save"
                : blocking.length ? `Fix before saving: ${blocking.join("; ")}`
                : `Save ${plural(count, "change")} to the YAML files`;
            const s = button("slls-sx-btn icon slls-sx-split-main", "", vSaving ? "refresh" : "save", () => vSave(false), title);
            if (vSaving) s.querySelector("svg").classList.add("slls-sx-spin");
            s.disabled = !count || vSaving || !!model.get("busy");
            const more = button("slls-sx-btn icon slls-sx-split-more", "", "chevron_down", () => {
                const rect = more.getBoundingClientRect();
                const items = [];
                if (count) items.push(["Save", "save", () => vSave(false)]);
                items.push([count ? "Save and publish" : "Publish", "upload", () => vSave(true)]);
                showMenu(rect.left, rect.bottom + 4, items);
            }, "Save options");
            more.setAttribute("aria-haspopup", "menu");
            more.disabled = vSaving || !!model.get("busy");
            const split = h("div", "slls-sx-split");
            split.append(s, more);
            group.append(u, r, split);
        }
        header.appendChild(group);
        header.appendChild(button("slls-sx-btn", "YAML", "file", () => openViewFiles(), "View the YAML files behind the semantic view"));
        const issues = viewIssues();
        const errors = issues.filter((i) => i.level === "error").length;
        header.appendChild(button("slls-sx-btn errors" + (errors ? " has" : ""), String(issues.length), "alert", openViewIssues,
            `Problems (${plural(errors, "error")}, ${plural(issues.length - errors, "warning")})`));
    }
    function renderViewBody() {
        if (model.get("busy") === "reload_view" && !treePane.isConnected) { loadingInto(body, "Reading the semantic view\u2026"); return; }
        body.append(treePane, dragHandle, detailPane);
        dragHandle.style.display = paneCollapsed ? "none" : "";
        renderViewTreePane();
        renderViewDetail();
    }

    // Tree
    function buildViewRows() {
        const rows = [];
        const q = vSearch.trim().toLowerCase();
        const open = (id) => !!q || vExpanded.has(id);
        const hit = (s) => !q || String(s || "").toLowerCase().includes(q);
        rows.push({ id: "view", depth: 0, node: vIndex.get("view"), hasChildren: true, open: open("view") });
        if (!open("view")) return rows;
        const datasets = [];
        for (const d of vNodes("dataset", true)) {
            const columns = vCols(d).slice().sort(byName).filter((c) => hit(d.name) || hit(c.name));
            if (hit(d.name) || columns.length) datasets.push({ d, columns });
        }
        const folder = (id, label, icon, count, noun) => {
            if (q && !count) return false;
            const trailTitle = `${plural(count, noun)} ${q ? "matching the search" : "in the semantic view"}`;
            rows.push({ id, depth: 1, folder: true, label, icon, hasChildren: count > 0, open: open(id), trailing: String(count), trailTitle });
            return open(id);
        };
        if (folder("v:datasets", "Datasets", "table", datasets.length, "dataset")) {
            for (const { d, columns } of datasets) {
                const n = vCols(d).length;
                rows.push({ id: d.key, depth: 2, node: d, hasChildren: columns.length > 0, open: open(d.key), trailing: n ? `${n}c` : "",
                    trailTitle: `${plural(n, "column")} in this dataset` });
                if (open(d.key)) for (const c of columns) rows.push({ id: c.key, depth: 3, node: c, trailing: c.data_type || "", trailTitle: "Data type" });
            }
        }
        const rels = vNodes("relationship", true).filter((r) => hit(r.name) || hit(r.from) || hit(r.to));
        if (folder("v:relationships", "Relationships", "relationship", rels.length, "relationship")) {
            for (const r of rels) rows.push({ id: r.key, depth: 2, node: r });
        }
        const metrics = vNodes("metric", true).filter((m) => hit(m.name));
        if (folder("v:metrics", "Metrics", "measure", metrics.length, "metric")) {
            for (const m of metrics) rows.push({ id: m.key, depth: 2, node: m });
        }
        return rows;
    }
    function viewRowElement(r, errors) {
        const node = r.node;
        const row = h("div", "slls-sx-row");
        row.setAttribute("role", "treeitem");
        row.style.paddingLeft = (6 + r.depth * 16) + "px";
        row.dataset.id = r.id;
        if (node) row.dataset.key = node.key;
        if (node && node.key === vSelected) row.classList.add("selected");
        if (r.folder) row.classList.add("folder");
        const caret = h("span", "slls-sx-caret" + (r.open ? " open" : ""));
        if (r.hasChildren) {
            caret.innerHTML = ICON.caret_right;
            caret.addEventListener("click", (ev) => { ev.stopPropagation(); vToggle(r.id); });
        }
        row.appendChild(caret);
        row.appendChild(iconEl(node ? VIEW_ICON[node.type] : r.icon));
        const text = node ? (node.name || "(unnamed)") : r.label;
        const label = h("span", "slls-sx-rlabel", text);
        label.title = node && node.description ? `${text}\n${node.description}` : text;
        if (node && node.type === "relationship") label.title = `${text}\n${node.from}[${node.from_column}] \u2192 ${node.to}[${node.to_column}]`;
        row.appendChild(label);
        if (node && ((node.type === "column" && node.is_hidden) || (node.type === "metric" && !String(node.dax || "").trim() && !node.isNew))) row.classList.add("muted");
        if (node && node.isNew) row.classList.add("new");
        if (node && node.deleted) row.classList.add("deleted");
        if (node && errors.has(node.key)) {
            const err = iconEl("alert", "slls-sx-err");
            err.title = errors.get(node.key).join("\n");
            row.appendChild(err);
        }
        if (node && node.type !== "column" && vIsDirty(node)) { const d = h("span", "slls-sx-dot"); d.title = "Unsaved change"; row.appendChild(d); }
        if (node && !node.isNew && viewData.view.published && ["dataset", "relationship", "metric"].includes(node.type)
            && (node.publish_state === "changed" || node.publish_state === "missing")) {
            const u = iconEl("upload", "slls-sx-unpub");
            u.title = node.publish_state === "missing" ? "Not in the published model" : unpublishedText(node);
            row.appendChild(u);
        }
        if (r.trailing) {
            const trail = h("span", "slls-sx-trail", r.trailing);
            if (r.trailTitle) trail.title = r.trailTitle;
            row.appendChild(trail);
        }
        row.addEventListener("click", () => { if (node) vSelect(node.key); else vToggle(r.id); });
        row.addEventListener("dblclick", () => { if (r.hasChildren && node) vToggle(r.id); });
        return row;
    }
    function renderViewTree() {
        if (!vTreeEl) return;
        const top = vTreeEl.scrollTop;
        clear(vTreeEl);
        const rows = buildViewRows();
        const errors = vMode === "write" ? viewValidation() : new Map();
        root.dataset.rows = String(rows.length);
        for (const r of rows) vTreeEl.appendChild(viewRowElement(r, errors));
        if (rows.length <= 1 && vSearch.trim()) vTreeEl.appendChild(h("div", "slls-sx-empty", "No matches."));
        vTreeEl.scrollTop = top;
    }
    function vToggle(id) {
        if (vExpanded.has(id)) vExpanded.delete(id); else vExpanded.add(id);
        renderViewTree();
    }
    function vSelect(key) {
        vSelected = key;
        vEditKey = "";
        for (const row of vTreeEl ? vTreeEl.querySelectorAll(".slls-sx-row") : []) row.classList.toggle("selected", row.dataset.key === key);
        renderViewDetail();
    }
    function vReveal(key) {
        const node = vIndex.get(key);
        if (!node) return;
        vExpanded.add("view");
        if (node.type === "dataset" || node.type === "column") vExpanded.add("v:datasets");
        if (node.type === "column") vExpanded.add(node.datasetKey);
        if (node.type === "relationship") vExpanded.add("v:relationships");
        if (node.type === "metric") vExpanded.add("v:metrics");
        vSearch = "";
        vSelected = key;
        vEditKey = "";
        renderViewTreePane();
        renderViewDetail();
        const row = vTreeEl && [...vTreeEl.children].find((r) => r.dataset && r.dataset.key === key);
        if (row) row.scrollIntoView({ block: "nearest" });
    }
    function renderViewTreePane() {
        clear(treePane);
        treePane.classList.toggle("collapsed", paneCollapsed);
        treePane.style.width = paneWidth + "px";
        if (paneCollapsed) {
            treePane.appendChild(iconEl("list_tree"));
            treePane.appendChild(button("slls-sx-iconbtn", "", "panel_expand", () => { paneCollapsed = false; renderBody(); }, "Expand view tree"));
            return;
        }
        const head = h("div", "slls-sx-pane-head");
        head.appendChild(h("span", "slls-sx-pane-label", "Semantic view"));
        const tools = h("div", "slls-sx-pane-tools");
        const forced = !!vSearch.trim();
        const ex = button("slls-sx-iconbtn", "", "expand_rows", () => {
            vExpanded = new Set(["view", "v:datasets", "v:relationships", "v:metrics", ...vNodes("dataset", true).map((d) => d.key)]);
            renderViewTree();
        }, "Expand all");
        const co = button("slls-sx-iconbtn", "", "collapse_rows", () => { vExpanded = new Set(["view"]); renderViewTree(); }, "Collapse all");
        ex.disabled = co.disabled = forced;
        const reloading = model.get("busy") === "reload_view";
        const doReload = () => {
            request("reload_view", {}, () => renderAll());
            renderViewTreePane();
        };
        const reload = button("slls-sx-iconbtn", "", "refresh", () => {
            if (viewPendingCount()) confirmDialog(`Reloading discards ${plural(viewPendingCount(), "pending change")}.`, "Reload", doReload);
            else doReload();
        }, "Reload the semantic view");
        if (reloading) reload.querySelector("svg").classList.add("slls-sx-spin");
        reload.disabled = !!model.get("busy") || vSaving;
        const collapse = button("slls-sx-iconbtn", "", "panel_collapse", () => { paneCollapsed = true; renderBody(); }, "Collapse view tree");
        tools.append(ex, co, reload, collapse);
        head.appendChild(tools);
        treePane.appendChild(head);
        const filters = h("div", "slls-sx-filters");
        const sw = h("div", "slls-sx-search");
        sw.appendChild(iconEl("search", ""));
        const input = h("input", "slls-sx-input");
        input.type = "search";
        input.placeholder = "Search datasets, columns and metrics\u2026";
        input.value = vSearch;
        let timer = null;
        input.addEventListener("input", () => {
            vSearch = input.value;
            ex.disabled = co.disabled = !!vSearch.trim();
            clearTimeout(timer);
            timer = setTimeout(renderViewTree, 150);
        });
        sw.appendChild(input);
        filters.appendChild(sw);
        treePane.appendChild(filters);
        vTreeEl = h("div", "slls-sx-tree slls-sx-vtree");
        vTreeEl.setAttribute("role", "tree");
        treePane.appendChild(vTreeEl);
        renderViewTree();
    }

    // Detail pane
    function vProps(rows) {
        const grid = h("div", "slls-sx-props");
        for (const [label, value, long] of rows) {
            const l = h("div", "slls-sx-plabel");
            l.appendChild(h("span", "", label));
            const v = h("div", "slls-sx-pvalue" + (long ? " long" : ""));
            if (value instanceof Node) v.appendChild(value);
            else if (value === "" || value === null || value === undefined) v.appendChild(h("span", "none", "(blank)"));
            else if (typeof value === "boolean") v.appendChild(h("span", value ? "yes" : "no", value ? "Yes" : "No"));
            else v.textContent = String(value);
            grid.append(l, v);
        }
        return grid;
    }
    function vCard(parent, title, fill, opts) {
        const c = card(title, Object.assign({ noFilter: true }, opts || {}));
        if (c.open) fill(c.body, c);
        parent.appendChild(c.el);
        return c;
    }
    function vLink(text, onClick) {
        const a = h("a", "slls-sx-link", text);
        a.addEventListener("click", onClick);
        return a;
    }
    function fileLink(path, node) {
        if (node && node.isNew) return h("span", "", path ? `${path} (when saved)` : "(the first YAML file, when saved)");
        return path ? vLink(path, () => openViewFiles(path)) : h("span", "none", "(inferred when published)");
    }
    function datasetLink(name) {
        const d = vDataset(name);
        return d ? vLink(name, () => vReveal(d.key)) : document.createTextNode(name || "");
    }
    // Editors for a staged field: typing is one undo step per field.
    function vInput(node, field, opts) {
        const o = opts || {};
        const input = h(o.multiline ? "textarea" : "input", o.multiline ? "slls-sx-textarea" : "slls-sx-input");
        if (o.multiline) input.rows = 3;
        input.value = (node.type === "view" ? vWork.description : node[field]) || "";
        if (o.placeholder) input.placeholder = o.placeholder;
        if (o.list) input.setAttribute("list", o.list);
        input.setAttribute("aria-label", o.label || field);
        input.dataset.field = field;
        input.addEventListener("input", () => vEdit(node.key, field, input.value, { typing: true }));
        input.addEventListener("change", () => { vEditKey = ""; if (o.onCommit) o.onCommit(input.value); });
        return input;
    }
    function vChoice(node, field, options, placeholder, onChange) {
        const sel = h("select", "slls-sx-select");
        sel.setAttribute("aria-label", field);
        const current = node[field] || "";
        const values = options.slice();
        if (current && !values.includes(current)) values.unshift(current);
        if (!current) {
            const ph = h("option", "", placeholder || "Select\u2026");
            ph.value = ""; ph.disabled = true; ph.selected = true;
            sel.appendChild(ph);
        }
        for (const v of values) { const op = h("option", "", v); op.value = v; op.selected = v === current; sel.appendChild(op); }
        sel.addEventListener("change", () => { vEdit(node.key, field, sel.value); if (onChange) onChange(sel.value); });
        return sel;
    }
    function issueList(container, issues) {
        if (!issues.length) { container.appendChild(h("div", "slls-sx-kv-empty", "No problems found.")); return; }
        for (const i of issues) {
            const row = h("div", "slls-sx-issue " + i.level);
            row.append(iconEl(i.level === "error" ? "alert" : "info", ""), h("span", "", i.message));
            if (i.file) { const f = fileLink(i.file); f.classList.add("file"); row.appendChild(f); }
            container.appendChild(row);
        }
    }
    function openViewIssues() {
        const m = modal(`Problems \u2014 ${viewData.view.name}`, "alert");
        const note = h("div", "slls-sx-picker-note");
        note.style.margin = "0 0 8px";
        note.textContent = "Checked when the view was read (saved files only). Fabric validates the definition again when the view is published.";
        m.body.appendChild(note);
        issueList(m.body, viewIssues());
    }
    function viewPills(node, typeEl) {
        const view = viewData.view;
        if (node.type === "view") {
            typeEl.appendChild(h("span", "slls-sx-pill" + (view.published ? " new" : " warn"), view.published ? "Published" : "Not published"));
            if (view.published && view.unpublished_count) {
                const p = h("span", "slls-sx-pill warn", `${plural(view.unpublished_count, "unpublished change")}`);
                p.title = "Saved changes in the YAML files which haven\u2019t been published yet";
                typeEl.appendChild(p);
            }
        }
        if (node.isNew) typeEl.appendChild(h("span", "slls-sx-pill new", "New"));
        else if (node.deleted) typeEl.appendChild(h("span", "slls-sx-pill danger", "Pending delete"));
        else if (vIsDirty(node)) typeEl.appendChild(h("span", "slls-sx-pill warn", "Modified"));
        if (node.type === "dataset" && node.inferred) typeEl.appendChild(h("span", "slls-sx-pill", "Inferred"));
        if (node.type === "metric" && !node.isNew && !String(node.dax || "").trim()) typeEl.appendChild(h("span", "slls-sx-pill warn", "No DAX"));
        // Published = in the generated model's TMDL (the last publish); saved
        // definitions which differ from it are unpublished changes.
        if (["dataset", "relationship", "metric"].includes(node.type) && view.published && !node.isNew) {
            if (!node.published) typeEl.appendChild(h("span", "slls-sx-pill warn", "Not in the published model"));
            else {
                typeEl.appendChild(h("span", "slls-sx-pill new", "Published"));
                if (node.publish_state === "changed") {
                    const p = h("span", "slls-sx-pill warn", "Unpublished changes");
                    p.title = unpublishedText(node);
                    typeEl.appendChild(p);
                }
            }
        }
        if (node.type === "column" && node.is_hidden) typeEl.appendChild(h("span", "slls-sx-pill", "Hidden"));
    }
    function unpublishedText(node) {
        return `Saved changes which haven\u2019t been published yet: ${(node.publish_diff || []).join(", ") || "definition"}`;
    }
    // "In the published model" + what has changed since it was published.
    function publishRows(node) {
        if (!viewData.view.published || node.isNew) return [];
        const rows = [["In the published model", !!node.published]];
        if (node.publish_state === "changed") {
            rows.push(["Unpublished changes", h("span", "slls-sx-unpub-text", (node.publish_diff || []).join(", ") || "definition")]);
        }
        return rows;
    }
    function paintViewErrors(node) {
        const box = detailPane.querySelector(".slls-sx-verrors");
        if (!box) return;
        clear(box);
        const msgs = vMode === "write" ? (viewValidation().get(node.key) || []) : [];
        box.style.display = msgs.length ? "" : "none";
        for (const msg of msgs) {
            const row = h("div", "slls-sx-issue error");
            row.append(iconEl("alert", ""), h("span", "", msg));
            box.appendChild(row);
        }
    }
    // While typing only the title, badges and validation refresh, so the
    // field being edited keeps focus.
    function refreshViewDetailHead() {
        const node = vIndex.get(vSelected);
        if (!node) return;
        const nameEl = detailPane.querySelector(".slls-sx-objname");
        if (nameEl) { nameEl.textContent = node.type === "view" ? node.name : (node.name || "(unnamed)"); nameEl.title = nameEl.textContent; }
        const typeEl = detailPane.querySelector(".slls-sx-objtype");
        if (typeEl) {
            typeEl.textContent = VIEW_TYPE_LABEL[node.type] + (node.type === "column" ? ` \u00b7 ${node.dataset}` : "");
            viewPills(node, typeEl);
        }
        paintViewErrors(node);
    }
    function renderViewDetail() {
        clear(detailPane);
        const node = vIndex.get(vSelected) || vIndex.get("view");
        if (!node) return;
        const write = vMode === "write" && !vSaving && vEditable(node) && !node.deleted;
        const head = h("div", "slls-sx-detail-head");
        head.appendChild(iconEl(VIEW_ICON[node.type], "slls-sx-objicon"));
        const titles = h("div", "slls-sx-objtitle");
        const nameEl = h("div", "slls-sx-objname", node.name || "(unnamed)");
        nameEl.title = nameEl.textContent;
        const typeEl = h("div", "slls-sx-objtype", VIEW_TYPE_LABEL[node.type] + (node.type === "column" ? ` \u00b7 ${node.dataset}` : ""));
        viewPills(node, typeEl);
        titles.append(nameEl, typeEl);
        head.appendChild(titles);
        const stats = h("div", "slls-sx-stats");
        const stat = (n, label) => { const s = h("div", "slls-sx-stat"); s.append(h("b", "", n.toLocaleString()), h("span", "", label)); stats.appendChild(s); };
        if (node.type === "view") {
            const datasets = vNodes("dataset");
            stat(datasets.length, "datasets");
            stat(datasets.reduce((a, d) => a + vCols(d).length, 0), "columns");
            stat(vNodes("relationship").length, "relationships");
            stat(vNodes("metric").length, "metrics");
        } else if (node.type === "dataset") stat(vCols(node).length, "columns");
        head.appendChild(stats);
        detailPane.appendChild(head);

        const actions = h("div", "slls-sx-detail-actions");
        if (node.type === "view") actions.appendChild(button("slls-sx-btn small", "YAML", "file", () => openViewFiles(), "View the YAML files behind the semantic view"));
        else if (node.file && !node.isNew) actions.appendChild(button("slls-sx-btn small", "YAML", "file", () => openViewFiles(node.file), `View ${node.file}`));
        if (node.type === "column") actions.appendChild(button("slls-sx-btn small", "Dataset", "table", () => vReveal(node.datasetKey), "Go to the dataset"));
        if (vMode === "write" && !vSaving) {
            if (node.type === "view") {
                actions.appendChild(button("slls-sx-btn small", "Dataset", "plus", () => vAdd("dataset"), "Add a dataset"));
                actions.appendChild(button("slls-sx-btn small", "Relationship", "plus", () => vAdd("relationship"), "Add a relationship"));
                actions.appendChild(button("slls-sx-btn small", "Metric", "plus", () => vAdd("metric"), "Add a metric"));
            } else if (vEditable(node)) {
                actions.appendChild(node.deleted
                    ? button("slls-sx-btn small", "Restore", "undo", () => vToggleDelete(node))
                    : button("slls-sx-btn small danger", "Delete", "trash", () => vToggleDelete(node)));
            }
        }
        if (actions.children.length) detailPane.appendChild(actions);

        const b = h("div", "slls-sx-detail-body");
        detailPane.appendChild(b);
        const errBox = h("div", "slls-sx-verrors");
        b.appendChild(errBox);
        paintViewErrors(node);
        if (node.type === "view") viewDetail(node, b, write);
        else if (node.type === "dataset") datasetDetail(node, b, write);
        else if (node.type === "column") columnDetail(node, b);
        else if (node.type === "relationship") relationshipDetail(node, b, write);
        else if (node.type === "metric") metricDetail(node, b, write);
        if (vMode === "write" && !vEditable(node) && node.type === "dataset") {
            const note = "Fabric added this table when the view was published; it isn\u2019t defined in the YAML files, so it can\u2019t be edited here.";
            b.insertBefore(h("div", "slls-sx-picker-note slls-sx-vnote", note), errBox.nextSibling);
        }
    }
    function notApplied(parent, node) {
        if (!(node.ignored || []).length) return;
        vCard(parent, "Not applied", (body) => {
            const keys = node.ignored.join(", ");
            const verb = node.ignored.length === 1 ? "is" : "are";
            body.appendChild(h("div", "slls-sx-picker-note", `${keys} ${verb} accepted in the definition but omitted from the generated semantic model.`));
        });
    }
    function viewDetail(node, b, write) {
        const issues = viewIssues();
        if (issues.length) vCard(b, "Problems", (body) => issueList(body, issues), { count: issues.length });
        const files = (viewData.files || []).filter((f) => f.area === "definition");
        const yamlFiles = files.filter((f) => !f.ignored);
        vCard(b, "General", (body) => body.appendChild(vProps([
            ["Name", node.name],
            ["Description", write ? vInput(node, "description", { multiline: true, label: "Description", placeholder: "What this view is about" }) : node.description, true],
            ["Lakehouse", node.lakehouse_name],
            ["Workspace", node.workspace_name],
            ["Definition folder", node.folder],
            ["YAML files", `${yamlFiles.length} of 20 \u00b7 ${fmtBytes(yamlFiles.reduce((a, f) => a + f.size, 0))}`],
            ["Published", !!node.published],
            ["Semantic model", node.published ? node.name : h("span", "none", "Created when the view is published")],
            ["Published output", node.published ? node.output_folder : ""],
            ...(node.published ? [["Unpublished changes", node.unpublished_count
                ? h("span", "slls-sx-unpub-text", `${node.unpublished_count}${(node.publish_diff || []).length ? " (including the description)" : ""}`)
                : h("span", "yes", "None \u2014 the published model is up to date")]] : []),
        ])), { dirty: vIsDirty(node) });
        vCard(b, "Files", (body) => {
            const list = h("div", "slls-sx-sublist");
            for (const f of files) {
                const it = h("div", "slls-sx-subitem" + (f.ignored ? " muted" : ""));
                it.append(iconEl(f.error ? "alert" : "file", ""), h("span", "slls-sx-files-name", f.path), h("span", "meta", f.ignored ? "ignored" : fmtBytes(f.size)));
                it.title = f.ignored ? `${f.path}\nIgnored \u2014 only .yaml files are part of the view` : f.path;
                it.addEventListener("click", () => openViewFiles(f.path));
                list.appendChild(it);
            }
            if (!files.length) list.appendChild(h("div", "slls-sx-kv-empty", "The view folder is empty."));
            body.appendChild(list);
        }, { count: files.length });
    }
    function datasetDetail(node, b, write) {
        const cached = vColCache[String(node.source || "").trim().toLowerCase()];
        const from = cached && cached.loading ? "Reading the lakehouse table\u2026"
            : !vBaseOf(node) || vBaseOf(node).source !== node.source ? (vCols(node).length ? "Lakehouse table" : "")
            : { published: "Published semantic model", lakehouse: "Lakehouse table (the view isn\u2019t published yet)" }[node.columns_from];
        let source = node.source;
        if (write) {
            const wrap = h("div", "slls-sx-vsource");
            const input = vInput(node, "source", {
                label: "Source", placeholder: "schema.table, e.g. dbo.sales", list: "slls-sx-vtables",
                onCommit: (value) => {
                    ensureViewColumns(value);
                    // A new dataset is named after its table until it is renamed.
                    const w = vFind(node.key);
                    const table = String(value || "").trim().split(".").pop();
                    if (w && w.isNew && /^new_dataset( \d+)?$/.test(w.name) && table) {
                        vEdit(node.key, "name", uniqueViewName("datasets", table));
                    } else renderViewDetail();
                },
            });
            const list = h("datalist");
            list.id = "slls-sx-vtables";
            fillTableList(list);
            wrap.append(input, list);
            source = wrap;
        }
        vCard(b, "General", (body) => body.appendChild(vProps([
            ["Name", write ? vInput(node, "name", { label: "Name" }) : node.name],
            ["Source", source],
            ["Description", write ? vInput(node, "description", { multiline: true, label: "Description" }) : node.description, true],
            ["Defined in", fileLink(node.file, node)],
            ["Columns from", from || h("span", "none", node.source ? "Unavailable \u2014 the source table could not be read" : "Choose a source table")],
            ...publishRows(node),
        ])), { dirty: vIsDirty(node) });
        notApplied(b, node);
        const rels = vNodes("relationship").filter((r) => ciEq(r.from, node.name) || ciEq(r.to, node.name));
        vCard(b, "Relationships", (body) => {
            const list = h("div", "slls-sx-sublist");
            for (const r of rels) {
                const it = h("div", "slls-sx-subitem");
                it.append(iconEl("relationship", ""), h("span", "", r.name), h("span", "meta", `${r.from}[${r.from_column}] \u2192 ${r.to}[${r.to_column}]`));
                it.addEventListener("click", () => vReveal(r.key));
                list.appendChild(it);
            }
            if (!rels.length) list.appendChild(h("div", "slls-sx-kv-empty", "No relationships use this dataset."));
            body.appendChild(list);
        }, { count: rels.length });
    }
    function columnDetail(node, b) {
        vCard(b, "General", (body) => body.appendChild(vProps([
            ["Name", node.name],
            ["Dataset", datasetLink(node.dataset)],
            ["Data type", node.data_type],
            ["Description", node.description, true],
            ["Source column", node.source_column],
            ["Format string", node.format_string],
            ["Hidden", !!node.is_hidden],
        ])));
    }
    function relationshipDetail(node, b, write) {
        const rel = h("div", "slls-sx-rel");
        const end = (t, c, many) => {
            const e = h("div", "slls-sx-rel-end");
            const tl = h("b", "", t || "(choose a dataset)");
            const d = vDataset(t);
            if (d) { tl.className = "slls-sx-link"; tl.addEventListener("click", () => vReveal(d.key)); }
            e.append(tl, h("span", "", `${c || "\u2014"} \u00b7 ${many ? "Many (*)" : "One (1)"}`));
            return e;
        };
        const mid = h("div", "slls-sx-rel-mid");
        mid.append(iconEl("relationship", ""), h("span", "", node.cross_filter === "bothDirections" ? "\u21c4 Both" : "\u2190 Single"));
        rel.append(end(node.from, node.from_column, true), mid, end(node.to, node.to_column, false));
        b.appendChild(rel);
        const names = vNodes("dataset").map((d) => d.name);
        const columnEditor = (side) => {
            const d = vDataset(node[side]);
            const cols = d ? vCols(d).map((c) => c.name) : [];
            if (cols.length) return vChoice(node, side + "_column", cols, "Select a column\u2026");
            return vInput(node, side + "_column", { label: side === "from" ? "From column" : "To column", placeholder: "column name" });
        };
        const rows = write ? [
            ["Name", vInput(node, "name", { label: "Name" })],
            ["From (many side)", vChoice(node, "from", names, "Select a dataset\u2026")],
            ["From column", columnEditor("from")],
            ["To (one side)", vChoice(node, "to", names, "Select a dataset\u2026")],
            ["To column", columnEditor("to")],
        ] : [
            ["Name", node.name],
            ["From (many side)", datasetLink(node.from)],
            ["From column", node.from_column],
            ["To (one side)", datasetLink(node.to)],
            ["To column", node.to_column],
        ];
        rows.push(["Cardinality", "Many to one"], ["Active", true], ["Defined in", fileLink(node.file, node)]);
        rows.push(...publishRows(node));
        vCard(b, "General", (body) => body.appendChild(vProps(rows)), { dirty: vIsDirty(node) });
        notApplied(b, node);
    }
    function metricDetail(node, b, write) {
        const c = vCard(b, "DAX expression", (body) => {
            if (write) {
                const ed = codeEditor(node.dax || "", "dax", false, (v) => vEdit(node.key, "dax", v, { typing: true }));
                ed.el.querySelector("textarea").addEventListener("blur", () => { vEditKey = ""; });
                body.appendChild(ed.el);
            } else if (String(node.dax || "").trim()) body.appendChild(codeEditor(node.dax, "dax", true).el);
            else body.appendChild(h("div", "slls-sx-code-error", "This metric has no DAX dialect, so it is omitted from the generated semantic model."));
        }, { dirty: vIsDirty(node) && (vBaseOf(node) || {}).dax !== node.dax });
        if (write) {
            const fmt = button("slls-sx-btn small", "Format DAX", "wand", () => {
                const text = (vFind(node.key) || {}).dax || "";
                if (!text.trim()) return;
                fmt.disabled = true;
                request("format_dax", { expression: text }, (res) => {
                    if (res.ok && res.data.formatted && res.data.formatted !== text) vEdit(node.key, "dax", res.data.formatted);
                    else if (!res.ok) { model.set("status", { message: `Error: ${res.error}`, kind: "error" }); model.save_changes(); renderViewDetail(); }
                    else renderViewDetail();
                });
            }, "Format the expression with DAX Formatter");
            c.actions.appendChild(fmt);
        }
        const rows = write ? [
            ["Name", vInput(node, "name", { label: "Name" })],
            ["Description", vInput(node, "description", { multiline: true, label: "Description" }), true],
        ] : [
            ["Name", node.name],
            ["Description", node.description, true],
        ];
        if (node.format_string) rows.push(["Format string", node.format_string]);
        if (node.datatype) rows.push(["Data type (ignored)", node.datatype]);
        rows.push(["Defined in", fileLink(node.file, node)]);
        rows.push(...publishRows(node));
        vCard(b, "General", (body) => body.appendChild(vProps(rows)));
        const others = (node.dialects || []).filter((d) => d.dialect !== "DAX");
        if (others.length) {
            vCard(b, "Other dialects", (body) => {
                const note = h("div", "slls-sx-picker-note", "Recognized but not published \u2014 only DAX is used in the generated semantic model.");
                note.style.margin = "0 0 8px";
                body.appendChild(note);
                for (const d of others) {
                    const lbl = h("div", "slls-sx-rls-head", d.dialect);
                    const pre = h("pre", "slls-sx-tmdl slls-sx-dialect", d.expression);
                    body.append(lbl, pre);
                }
            }, { count: others.length });
        }
        notApplied(b, node);
    }

    // YAML (definition) and TMDL (published output) files behind the view.
    function yamlScalar(v) {
        const t = v.trim();
        if (!t) return esc(v);
        const lead = v.slice(0, v.indexOf(t));
        const trail = v.slice(lead.length + t.length);
        let k = "";
        if (/^(["']).*\1$/.test(t)) k = "string";
        else if (/^-?(\d+(\.\d+)?|\.\d+)([eE][+-]?\d+)?$/.test(t)) k = "number";
        else if (/^(true|false|null|~)$/i.test(t)) k = "keyword";
        else if (/^[|>][+-]?$/.test(t)) k = "operator";
        return esc(lead) + (k ? `<span class="slls-sx-tk-${k}">${esc(t)}</span>` : esc(t)) + esc(trail);
    }
    function splitYamlComment(s) {
        let quote = null;
        for (let i = 0; i < s.length; i++) {
            const ch = s[i];
            if (quote) { if (ch === quote) quote = null; continue; }
            if ((ch === '"' || ch === "'") && (i === 0 || /[\s:\[,{-]/.test(s[i - 1]))) { quote = ch; continue; }
            if (ch === "#" && (i === 0 || /\s/.test(s[i - 1]))) return [s.slice(0, i), s.slice(i)];
        }
        return [s, ""];
    }
    function highlightYaml(text) {
        return String(text || "").split(/\r\n?|\n/).map((line) => {
            const [code, comment] = splitYamlComment(line);
            const m = /^(\s*)((?:-\s+)*)(.*)$/.exec(code);
            let out = esc(m[1]) + m[2].replace(/-/g, '<span class="slls-sx-tk-operator">-</span>');
            let rest = m[3];
            const km = /^("[^"]*"|'[^']*'|[^\s"'#][^:#]*?)(\s*:)(?=\s|$)/.exec(rest);
            if (km) {
                out += `<span class="slls-sx-tk-variable">${esc(km[1])}</span><span class="slls-sx-tk-punctuation">${esc(km[2])}</span>`;
                rest = rest.slice(km[0].length);
            }
            out += yamlScalar(rest);
            if (comment) out += `<span class="slls-sx-tk-comment">${esc(comment)}</span>`;
            return out;
        }).join("\n");
    }
    function openViewFiles(focusPath) {
        const files = viewData.files || [];
        const m = modal(`Files \u2014 ${viewData.view.name}`, "file", { wide: true });
        m.body.classList.add("slls-sx-files");
        const list = h("div", "slls-sx-files-list");
        const pane = h("div", "slls-sx-files-pane");
        m.body.append(list, pane);
        let current = files.find((f) => f.area === "definition" && f.path === focusPath)
            || files.find((f) => f.area === "definition" && !f.ignored) || files[0] || null;
        const copy = button("slls-sx-btn small", "Copy", "", () => {
            if (!current) return;
            try { navigator.clipboard.writeText(current.content || ""); copy.textContent = "Copied"; } catch (e) { copy.textContent = "Copy failed"; }
        });
        m.actions.appendChild(copy);
        const groups = [
            ["definition", "Definition (YAML)", viewData.view.folder],
            ["generated", "Published output (TMDL)", viewData.view.output_folder],
        ];
        function paint() {
            clear(list);
            for (const [area, label, folder] of groups) {
                const items = files.filter((f) => f.area === area);
                if (!items.length && area === "generated") continue;
                const g = h("div", "slls-sx-files-group");
                g.append(h("div", "slls-sx-files-label", label), h("div", "slls-sx-files-folder", folder));
                g.title = folder;
                list.appendChild(g);
                if (!items.length) list.appendChild(h("div", "slls-sx-kv-empty slls-sx-files-none", "No files"));
                for (const f of items) {
                    const it = h("div", "slls-sx-subitem" + (f === current ? " on" : "") + (f.ignored ? " muted" : ""));
                    it.append(iconEl(f.error ? "alert" : "file", ""), h("span", "slls-sx-files-name", f.path),
                        h("span", "meta", f.ignored ? "ignored" : fmtBytes(f.size)));
                    it.title = f.ignored ? `${f.path}\nIgnored \u2014 only .yaml files are part of the view` : f.path;
                    it.addEventListener("click", () => { current = f; paint(); });
                    list.appendChild(it);
                }
            }
            clear(pane);
            copy.textContent = "Copy";
            copy.disabled = !current || !current.content;
            if (!current) { pane.appendChild(h("div", "slls-sx-empty", "The view folder is empty.")); return; }
            const fh = h("div", "slls-sx-files-head");
            const folder = current.area === "generated" ? viewData.view.output_folder : viewData.view.folder;
            fh.append(h("b", "", current.path), h("span", "", `${folder}/${current.path}`));
            pane.appendChild(fh);
            if (current.error) pane.appendChild(h("div", "slls-sx-code-error", `Invalid YAML: ${current.error}`));
            if (current.ignored) {
                pane.appendChild(h("div", "slls-sx-picker-note", "Only .yaml files are part of a semantic view; this file is ignored when the view is published."));
                return;
            }
            if (current.area === "generated") {
                pane.appendChild(h("div", "slls-sx-picker-note", "Generated when the view was published. Edit the YAML definition, not this output."));
            }
            if (current.area === "definition" && viewPendingCount()) {
                pane.appendChild(h("div", "slls-sx-picker-note", "Showing the saved file \u2014 pending changes are written to it when you save."));
            }
            const scroller = h("div", "slls-sx-files-code");
            const pre = h("pre", "slls-sx-tmdl");
            const content = current.content || "";
            try {
                pre.innerHTML = current.language === "tmdl" ? highlightTmdl(content)
                    : current.language === "yaml" ? highlightYaml(content) : esc(content);
            } catch (e) {
                pre.textContent = content;
            }
            if (!content) pre.innerHTML = '<span class="slls-sx-code-empty">(empty file)</span>';
            scroller.appendChild(pre);
            pane.appendChild(scroller);
        }
        paint();
    }

    // ------------------------------------------------------------------
    // Tree
    // ------------------------------------------------------------------
    function modelKey() { return data && data.model ? data.model.key : ""; }
    function tableIcon(node) {
        return { calculated_table: "calculated_table", calculation_group: "calculation_group",
            field_parameter: "field_parameter", date_table: "date_table" }[node.kind] || "table";
    }
    function nodeIcon(node) {
        switch (node.type) {
            case "model": return "database";
            case "table": return tableIcon(node);
            case "column": return node.column_type === "Calculated" ? "calculated_column" : "column";
            case "measure": return "measure";
            case "hierarchy": return "hierarchy";
            case "level": return "level";
            case "calculationItem": return "calculation_item";
            case "partition": return "partition";
            case "relationship": return "relationship";
            case "expression": return "code";
            case "function": return "zap";
            case "perspective": return "perspective";
            case "role": return "shield_check";
            case "culture": return "globe";
            default: return "folder";
        }
    }
    function typeLabel(node) {
        if (node.type === "table") return TABLE_KIND_LABEL[node.kind] || "Table";
        if (node.type === "column" && node.column_type === "Calculated") return "Calculated column";
        if (node.type === "column" && node.column_type === "CalculatedTableColumn") return "Calculated table column";
        return TYPE_LABEL[node.type] || node.type;
    }
    function tableFilterId(t) {
        if (t.kind === "calculation_group") return "calculationGroups";
        if (t.kind === "calculated_table" || t.kind === "field_parameter") return "calculatedTables";
        return "tables";
    }
    function fieldFilterId(node) {
        if (node.type === "column") return node.column_type === "Calculated" ? "calculatedColumns" : "columns";
        return { measure: "measures", hierarchy: "hierarchies", calculationItem: "calculationItems", partition: "partitions" }[node.type];
    }
    function inPerspective(node) {
        if (!viewPerspective) return true;
        if (node.isNew) return true;
        const list = jsonValue(node, "Perspectives", []);
        return list.includes(viewPerspective);
    }
    const byLabel = (a, b) => displayLabel(a).localeCompare(displayLabel(b), undefined, { sensitivity: "base" });
    function tableFields(t) {
        const sortAll = (arr) => arr.slice().sort(byLabel);
        return [
            ...sortAll([...(t.measures || []), ...newNodes("measure", t.key)]),
            ...sortAll([...(t.calculation_items || []), ...newNodes("calculationItem", t.key)]),
            ...sortAll([...(t.columns || []), ...newNodes("column", t.key)]),
            ...sortAll(t.hierarchies || []),
        ];
    }

    function buildRows() {
        const rows = [];
        if (!loaded()) return rows;
        const q = search.trim().toLowerCase();
        const forced = !!q;
        const open = (id) => forced || expanded.has(id);
        const hit = (node) => !q || displayLabel(node).toLowerCase().includes(q);
        const typeOn = typeFilter.size > 0;
        const tableTypes = TABLE_FILTERS.some((f) => typeFilter.has(f));
        const fieldTypes = FIELD_FILTERS.some((f) => typeFilter.has(f));
        const allowField = (node) => !typeOn || !fieldTypes || typeFilter.has(fieldFilterId(node));

        const m = data.model;
        rows.push({ id: "model", depth: 0, node: m, label: "Model", icon: "database", hasChildren: true, open: open("model") });
        if (!open("model")) return rows;

        // Tables
        const tables = [...(data.tables || []), ...newNodes("table", ""), ...newNodes("fieldParameter", "")].sort(byLabel);
        const tableRows = [];
        for (const t of tables) {
            if (!inPerspective(t)) continue;
            const tableTypeOk = !typeOn || (tableTypes ? typeFilter.has(tableFilterId(t)) : true);
            if (!tableTypeOk && !fieldTypes) continue;
            const tableHit = hit(t);
            const partitions = (t.partitions || []).filter((p) => !viewPerspective && allowField(p) && (!typeOn || typeFilter.has("partitions")) && (tableHit || hit(p)));
            const fields = [];
            for (const f of tableFields(t)) {
                if (!inPerspective(f) || !allowField(f)) continue;
                if (typeOn && fieldTypes && !typeFilter.has(fieldFilterId(f))) continue;
                const levels = f.type === "hierarchy" ? (f.levels || []).filter((lv) => tableHit || hit(f) || hit(lv)) : [];
                if (!tableHit && !hit(f) && !levels.length) continue;
                fields.push({ node: f, levels: f.type === "hierarchy" ? (tableHit || hit(f) ? f.levels || [] : levels) : [] });
            }
            const showTable = (tableTypeOk && (!q || tableHit) && (!fieldTypes || !tableTypes || typeFilter.has(tableFilterId(t))))
                || fields.length || partitions.length;
            if (!showTable) continue;
            if (typeOn && fieldTypes && !tableTypes && !fields.length && !partitions.length) continue;
            tableRows.push({ t, partitions, fields });
        }
        const showTablesFolder = !typeOn || tableTypes || fieldTypes;
        if (showTablesFolder) {
            rows.push({ id: "folder:tables", depth: 1, folder: "tables", label: "Tables", icon: "table", hasChildren: true,
                open: open("folder:tables"), trailing: String(tableRows.length) });
            if (open("folder:tables")) {
                for (const { t, partitions, fields } of tableRows) {
                    const hasKids = partitions.length + fields.length > 0;
                    rows.push({ id: t.key, depth: 2, node: t, hasChildren: hasKids, open: open(t.key) });
                    if (!open(t.key)) continue;
                    if (partitions.length) {
                        const pid = t.key + "#partitions";
                        rows.push({ id: pid, depth: 3, group: true, label: "Partitions", icon: "partition", hasChildren: true,
                            open: open(pid), trailing: String(partitions.length) });
                        if (open(pid)) for (const p of partitions) rows.push({ id: p.key, depth: 4, node: p });
                    }
                    for (const { node: f, levels } of fields) {
                        rows.push({ id: f.key, depth: 3, node: f, hasChildren: levels.length > 0, open: open(f.key) });
                        if (levels.length && open(f.key)) for (const lv of levels) rows.push({ id: lv.key, depth: 4, node: lv });
                    }
                }
            }
        }

        for (const [folder, label, icon] of FOLDERS) {
            if (typeOn && !typeFilter.has(folder)) continue;
            const createType = { roles: "role", perspectives: "perspective", expressions: "expression", relationships: "relationship" }[folder];
            const items = [...(data[folder] || []), ...(createType ? newNodes(createType, "") : [])]
                .filter((n) => hit(n)).sort(byLabel);
            if (!items.length && (q || !(data[folder] || []).length) && !(mode === "write" && createType && !q && !typeOn)) continue;
            const id = "folder:" + folder;
            rows.push({ id, depth: 1, folder, label, icon, hasChildren: items.length > 0, open: open(id), trailing: String(items.length) });
            if (open(id)) for (const n of items) rows.push({ id: n.key, depth: 2, node: n });
        }
        return rows;
    }

    function renderTree() {
        if (kind() === "view") { if (viewOpen()) renderViewTree(); return; }
        labelCache = new Map();
        try {
            renderTreeRows();
        } finally {
            labelCache = null;
        }
    }
    // The tree is virtualized: rows are computed for the whole (expanded) model
    // but only those in or near the viewport are in the DOM, so large models
    // stay responsive however much of the tree is expanded.
    const ROW_HEIGHT = 26;
    const ROW_OVERSCAN = 12;
    let treeRows = [];
    let treeErrorKeys = new Set();
    let treeDirtyKeys = new Set();
    function renderTreeRows() {
        treeRows = buildRows();
        for (const r of treeRows) if (r.node && r.node.type !== "model") r.label = displayLabel(r.node);
        treeErrorKeys = new Set((data.errors || []).map((e) => e.key));
        root.dataset.rows = String(treeRows.length);
        clear(tree);
        if (!treeRows.length) { tree.appendChild(h("div", "slls-sx-empty", "Nothing to show.")); return; }
        const spacer = h("div", "slls-sx-tree-spacer");
        spacer.style.height = (treeRows.length * ROW_HEIGHT + 8) + "px";
        tree.appendChild(spacer);
        paintTreeRows();
    }
    function paintTreeRows() {
        if (!treeRows.length) return;
        treeDirtyKeys = new Set(Object.values(staging.edits).map((e) => e.key));
        staging.ops.forEach((o) => treeDirtyKeys.add(o.key));
        const viewport = tree.clientHeight || 640;
        const first = Math.max(0, Math.floor(tree.scrollTop / ROW_HEIGHT) - ROW_OVERSCAN);
        const last = Math.min(treeRows.length, Math.ceil((tree.scrollTop + viewport) / ROW_HEIGHT) + ROW_OVERSCAN);
        for (const old of [...tree.querySelectorAll(".slls-sx-row")]) old.remove();
        const frag = document.createDocumentFragment();
        for (let i = first; i < last; i++) frag.appendChild(treeRowElement(treeRows[i], i));
        tree.appendChild(frag);
    }
    let paintQueued = false;
    tree.addEventListener("scroll", () => {
        if (paintQueued) return;
        paintQueued = true;
        requestAnimationFrame(() => { paintQueued = false; paintTreeRows(); });
    });
    function rowIsDirty(node) {
        if (node.isNew || treeDirtyKeys.has(node.key)) return true;
        return node.type === "table" && (node.partitions || []).some((p) => treeDirtyKeys.has(p.key));
    }
    function treeRowElement(r, i) {
        const node = r.node;
        const row = h("div", "slls-sx-row");
        row.setAttribute("role", "treeitem");
        row.style.top = (4 + i * ROW_HEIGHT) + "px";
        row.style.paddingLeft = (6 + r.depth * 16) + "px";
        row.dataset.id = r.id;
        if (node) row.dataset.key = node.key;
        if (node && node.key === selectedKey) row.classList.add("selected");
        if (r.folder || r.group) row.classList.add("folder");
        const caret = h("span", "slls-sx-caret" + (r.open ? " open" : ""));
        if (r.hasChildren) {
            caret.innerHTML = ICON.caret_right;
            caret.addEventListener("click", (ev) => { ev.stopPropagation(); toggleExpand(r.id); });
        }
        row.appendChild(caret);
        row.appendChild(iconEl(node ? nodeIcon(node) : r.icon));
        const label = h("span", "slls-sx-rlabel", r.label);
        row.appendChild(label);
        if (node && node.type !== "model") {
            if (node.type !== "level" && node.type !== "relationship" && isHidden(node)) row.classList.add("muted");
            if (node.type === "relationship" && !value(node, "IsActive")) row.classList.add("muted");
            if (isTranslatedLabel(node)) row.classList.add("translated");
            if (isDeleted(node)) row.classList.add("deleted");
            if (node.isNew) row.classList.add("new");
            if (treeErrorKeys.has(node.key)) {
                const err = iconEl("alert", "slls-sx-err");
                err.title = "This object is in an error state";
                row.appendChild(err);
            }
            if (rowIsDirty(node)) { const d = h("span", "slls-sx-dot"); d.title = "Unsaved change"; row.appendChild(d); }
            const description = value(node, "Description");
            label.title = objectLabel(node) + (description ? `\n${description}` : "");
        }
        if (r.trailing) row.appendChild(h("span", "slls-sx-trail", r.trailing));
        else if (node && node.type === "table") {
            // Column / measure / hierarchy counts, e.g. "12c \u00b7 3m \u00b7 1h".
            const trail = h("span", "slls-sx-trail slls-sx-counts");
            const counts = [
                [(node.columns || []).length + newNodes("column", node.key).length, "c", "column", "columns"],
                [(node.measures || []).length + newNodes("measure", node.key).length, "m", "measure", "measures"],
                [(node.hierarchies || []).length, "h", "hierarchy", "hierarchies"],
            ];
            counts.forEach(([n, suffix, one, many], i) => {
                if (i) trail.appendChild(h("span", "sep", "\u00b7"));
                const c = h("span", "", `${n}${suffix}`);
                c.title = `${n} ${n === 1 ? one : many}`;
                trail.appendChild(c);
            });
            row.appendChild(trail);
        }
        row.addEventListener("click", () => {
            if (node) select(node.key);
            else toggleExpand(r.id);
        });
        row.addEventListener("dblclick", () => { if (r.hasChildren && node) toggleExpand(r.id); });
        row.addEventListener("contextmenu", (ev) => {
            ev.preventDefault();
            if (node) select(node.key);
            openContextMenu(ev.clientX, ev.clientY, node, r.folder);
        });
        return row;
    }
    function toggleExpand(id) {
        if (expanded.has(id)) expanded.delete(id); else expanded.add(id);
        renderTree();
    }
    function select(key) {
        selectedKey = key;
        propFilter = "";
        // Only the highlight moves; rebuilding a large expanded tree on every
        // click would make the explorer sluggish.
        for (const row of tree.querySelectorAll(".slls-sx-row.selected")) row.classList.remove("selected");
        for (const row of tree.children) if (row.dataset && row.dataset.key === key) row.classList.add("selected");
        renderDetail();
    }
    function reveal(key) {
        const node = getNode(key);
        if (!node) return;
        expanded.add("model");
        let p = node;
        while (p) {
            if (p.type === "table") expanded.add("folder:tables");
            if (p.parent) expanded.add(p.parent.key);
            if (p.type === "partition" && p.parent) expanded.add(p.parent.key + "#partitions");
            if (!p.parent && p.type !== "table" && p.type !== "model") {
                const folder = FOLDERS.find(([f]) => (data[f] || []).some((n) => n.key === p.key));
                if (folder) expanded.add("folder:" + folder[0]);
            }
            p = p.parent;
        }
        search = "";
        typeFilter = new Set();
        viewPerspective = "";
        selectedKey = key;
        renderTreePane();
        renderDetail();
        scrollToSelected();
    }
    function scrollToSelected() {
        const i = treeRows.findIndex((r) => r.node && r.node.key === selectedKey);
        if (i < 0) return;
        const top = 4 + i * ROW_HEIGHT;
        const viewport = tree.clientHeight || 640;
        if (top < tree.scrollTop) tree.scrollTop = top;
        else if (top + ROW_HEIGHT > tree.scrollTop + viewport) tree.scrollTop = top + ROW_HEIGHT - viewport;
        paintTreeRows();
    }

    function renderTreePane() {
        if (kind() === "view") { if (viewOpen()) renderViewTreePane(); return; }
        clear(treePane);
        treePane.classList.toggle("collapsed", paneCollapsed);
        treePane.style.width = paneWidth + "px";
        if (paneCollapsed) {
            treePane.appendChild(iconEl("list_tree"));
            treePane.appendChild(button("slls-sx-iconbtn", "", "panel_expand", () => { paneCollapsed = false; renderBody(); }, "Expand model view"));
            return;
        }
        const head = h("div", "slls-sx-pane-head");
        head.appendChild(h("span", "slls-sx-pane-label", "Model view"));
        const tools = h("div", "slls-sx-pane-tools");
        const filterBtn = button("slls-sx-iconbtn" + (showFilters ? " on" : ""), "", "sliders", () => { showFilters = !showFilters; renderTreePane(); },
            showFilters ? "Hide search & filters" : "Show search & filters");
        const forced = !!search.trim();
        const ex = button("slls-sx-iconbtn", "", "expand_rows", expandAll, "Expand all");
        ex.disabled = forced;
        const co = button("slls-sx-iconbtn", "", "collapse_rows", () => { expanded = new Set(["model"]); renderTree(); }, "Collapse all");
        co.disabled = forced;
        const busy = model.get("busy") === "reload";
        const doReload = () => {
            request("reload", {}, (res) => {
                if (res.ok) { staging = { edits: {}, ops: [], creates: [] }; undoStack = []; redoStack = []; }
                renderAll();
            });
            renderTreePane();
        };
        const reload = button("slls-sx-iconbtn", "", "refresh", () => {
            if (pendingCount()) confirmDialog(`Reloading discards ${plural(pendingCount(), "pending change")}.`, "Reload", doReload);
            else doReload();
        }, "Reload model metadata");
        if (busy) reload.querySelector("svg").classList.add("slls-sx-spin");
        reload.disabled = !!model.get("busy");
        const collapse = button("slls-sx-iconbtn", "", "panel_collapse", () => { paneCollapsed = true; renderBody(); }, "Collapse model view");
        tools.append(filterBtn, ex, co, reload, collapse);
        head.appendChild(tools);
        treePane.appendChild(head);

        if (showFilters) {
            const filters = h("div", "slls-sx-filters");
            const sw = h("div", "slls-sx-search");
            sw.appendChild(iconEl("search", ""));
            const input = h("input", "slls-sx-input");
            input.type = "search";
            input.placeholder = "Search tables and fields\u2026";
            input.value = search;
            let searchTimer = null;
            input.addEventListener("input", () => {
                search = input.value;
                ex.disabled = co.disabled = !!search.trim();
                clearTimeout(searchTimer);
                searchTimer = setTimeout(renderTree, 150);
            });
            sw.appendChild(input);
            filters.appendChild(sw);

            const tf = h("div", "slls-sx-typefilter");
            const tbtn = h("button", "slls-sx-typebtn");
            tbtn.type = "button";
            tbtn.title = "Filter by object type";
            tbtn.innerHTML = `<span>${ICON.sliders}${typeFilter.size === 0 ? "All object types" : esc(plural(typeFilter.size, "type") + " selected")}</span>${ICON.chevron_down}`;
            tf.appendChild(tbtn);
            let menu = null;
            const closeMenu = () => { if (menu) { menu.remove(); menu = null; document.removeEventListener("mousedown", outside, true); } };
            const outside = (ev) => { if (!tf.contains(ev.target)) closeMenu(); };
            tbtn.addEventListener("click", () => {
                if (menu) { closeMenu(); return; }
                menu = h("div", "slls-sx-menu");
                const mh = h("div", "slls-sx-menu-head");
                mh.appendChild(h("span", "", "Object types"));
                if (typeFilter.size) {
                    const clr = h("button", "", "Clear");
                    clr.type = "button";
                    clr.addEventListener("click", () => { typeFilter = new Set(); closeMenu(); renderTreePane(); });
                    mh.appendChild(clr);
                }
                menu.appendChild(mh);
                for (const [id, label, icon] of OBJECT_TYPE_FILTERS) {
                    const item = h("label", "slls-sx-menu-item");
                    const cb = h("input");
                    cb.type = "checkbox";
                    cb.checked = typeFilter.has(id);
                    cb.addEventListener("change", () => {
                        if (cb.checked) typeFilter.add(id); else typeFilter.delete(id);
                        tbtn.innerHTML = `<span>${ICON.sliders}${typeFilter.size === 0 ? "All object types" : esc(plural(typeFilter.size, "type") + " selected")}</span>${ICON.chevron_down}`;
                        renderTree();
                    });
                    item.append(cb, iconEl(icon, ""), document.createTextNode(label));
                    menu.appendChild(item);
                }
                tf.appendChild(menu);
                document.addEventListener("mousedown", outside, true);
            });
            filters.appendChild(tf);

            const perspectives = data.perspectives || [];
            if (perspectives.length) {
                const r = h("div", "slls-sx-filter-row");
                r.appendChild(iconEl("perspective", ""));
                const sel = h("select", "slls-sx-select");
                sel.title = "View as perspective";
                sel.setAttribute("aria-label", "View as perspective");
                sel.appendChild(new Option("Full model", ""));
                for (const p of perspectives) sel.appendChild(new Option(effName(p), p.name));
                sel.value = viewPerspective;
                sel.addEventListener("change", () => { viewPerspective = sel.value; renderTree(); });
                r.appendChild(sel);
                filters.appendChild(r);
            }
            const cultures = data.cultures || [];
            if (cultures.length) {
                const r = h("div", "slls-sx-filter-row");
                r.appendChild(iconEl("globe", ""));
                const sel = h("select", "slls-sx-select");
                sel.title = "View as translation";
                sel.setAttribute("aria-label", "View as translation");
                sel.appendChild(new Option("Default (no translation)", ""));
                for (const c of cultures) sel.appendChild(new Option(c.name, c.name));
                sel.value = viewCulture;
                sel.addEventListener("change", () => { viewCulture = sel.value; renderTree(); });
                r.appendChild(sel);
                filters.appendChild(r);
            }
            treePane.appendChild(filters);
        }
        treePane.appendChild(tree);
        renderTree();
    }
    function expandAll() {
        expanded.add("model");
        expanded.add("folder:tables");
        for (const [f] of FOLDERS) expanded.add("folder:" + f);
        for (const t of data.tables || []) {
            expanded.add(t.key);
            for (const hi of t.hierarchies || []) expanded.add(hi.key);
        }
        renderTree();
    }

    // Resizable split between the tree and the detail pane.
    dragHandle.addEventListener("mousedown", (ev) => {
        ev.preventDefault();
        const startX = ev.clientX, startW = paneWidth;
        dragHandle.classList.add("active");
        const move = (e) => {
            paneWidth = Math.max(PANE_MIN, Math.min(PANE_MAX, startW + e.clientX - startX));
            treePane.style.width = paneWidth + "px";
        };
        const up = () => {
            dragHandle.classList.remove("active");
            document.removeEventListener("mousemove", move);
            document.removeEventListener("mouseup", up);
        };
        document.addEventListener("mousemove", move);
        document.addEventListener("mouseup", up);
    });
    dragHandle.addEventListener("dblclick", () => { paneWidth = 340; treePane.style.width = "340px"; });

    // ------------------------------------------------------------------
    // Context menu
    // ------------------------------------------------------------------
    let ctxMenu = null;
    function closeContextMenu() {
        if (ctxMenu) { ctxMenu.remove(); ctxMenu = null; }
        document.removeEventListener("mousedown", ctxOutside, true);
    }
    function ctxOutside(ev) { if (ctxMenu && !ctxMenu.contains(ev.target)) closeContextMenu(); }
    function menuItems(node, folder) {
        const items = [];
        const write = mode === "write" && !saving;
        if (folder) {
            if (folder === "tables") items.push(["Add calculation group", "calculation_group", () => addObject("table", null)]);
            if (folder === "roles") items.push(["Add role", "shield_check", () => addObject("role", null)]);
            if (folder === "perspectives") items.push(["Add perspective", "perspective", () => addObject("perspective", null)]);
            if (folder === "expressions") items.push(["Add expression", "code", () => addObject("expression", null)]);
            if (folder === "relationships") items.push(["Add relationship\u2026", "relationship", () => openRelationshipManager()]);
            return items;
        }
        if (!node) return items;
        if (!node.isNew) {
            items.push(["View TMDL", "file", () => openTmdl(node)]);
            if (["table", "column", "measure", "calculationItem", "hierarchy", "partition", "expression", "function", "relationship"].includes(node.type)) {
                items.push(["Show dependencies", "workflow", () => openDependencies(node)]);
            }
        }
        if (node.type === "table") {
            if (items.length) items.push(null);
            items.push(["Add measure", "measure", () => addObject("measure", node)]);
            if (node.kind !== "calculation_group") items.push(["Add calculated column", "calculated_column", () => addObject("column", node)]);
            if (node.kind === "calculation_group") items.push(["Add calculation item", "calculation_item", () => addObject("calculationItem", node)]);
        }
        if (node.type === "model") {
            items.push(null);
            items.push(["Add calculation group", "calculation_group", () => addObject("table", null)]);
            items.push(["Add role", "shield_check", () => addObject("role", null)]);
            items.push(["Add perspective", "perspective", () => addObject("perspective", null)]);
            items.push(["Add expression", "code", () => addObject("expression", null)]);
        }
        const hideable = ["table", "column", "measure", "hierarchy", "function"].includes(node.type);
        const dupable = !node.isNew && ["table", "column", "measure", "hierarchy", "calculationItem", "role", "perspective", "expression"].includes(node.type);
        const deletable = !["model", "culture"].includes(node.type);
        if (hideable || dupable || deletable) items.push(null);
        if (hideable) {
            const hidden = isHidden(node);
            items.push([hidden ? "Unhide" : "Hide", hidden ? "eye" : "eye_off", () => setValue(node, "IsHidden", !hidden, { label: "Hidden" }), !write]);
        }
        if (dupable) items.push([isDuplicated(node) ? "Undo duplicate" : "Duplicate", "plus", () => toggleOp(node, "duplicate"), !write]);
        if (deletable) {
            const del = staging.ops.some((o) => o.op === "delete" && o.key === node.key);
            items.push([del ? "Restore" : "Delete", del ? "undo" : "trash", () => toggleOp(node, "delete"), !write, !del]);
        }
        while (items.length && items[items.length - 1] === null) items.pop();
        while (items.length && items[0] === null) items.shift();
        return items;
    }
    function openContextMenu(x, y, node, folder) {
        showMenu(x, y, menuItems(node, folder));
    }
    // Items are [label, icon, onClick, disabled, danger] or null (separator).
    function showMenu(x, y, items) {
        closeContextMenu();
        if (!items.length) return;
        ctxMenu = h("div", "slls-sx-menu slls-sx-ctx");
        for (const item of items) {
            if (item === null) { ctxMenu.appendChild(h("div", "slls-sx-menu-sep")); continue; }
            const [label, icon, fn, disabled, danger] = item;
            const b = h("button", "slls-sx-menu-item" + (danger ? " danger" : ""));
            b.type = "button";
            b.append(iconEl(icon, ""), document.createTextNode(label));
            if (disabled) { b.disabled = true; b.title = "Switch to edit mode to make changes"; }
            b.addEventListener("click", () => { closeContextMenu(); fn(); });
            ctxMenu.appendChild(b);
        }
        root.appendChild(ctxMenu);
        const rect = ctxMenu.getBoundingClientRect();
        ctxMenu.style.left = Math.max(4, Math.min(x, window.innerWidth - rect.width - 8)) + "px";
        ctxMenu.style.top = Math.max(4, Math.min(y, window.innerHeight - rect.height - 8)) + "px";
        document.addEventListener("mousedown", ctxOutside, true);
    }
    document.addEventListener("keydown", (ev) => {
        if (ev.key === "Escape") closeContextMenu();
        if (!root.isConnected) return;
        const viewEdit = kind() === "view" && vMode === "write";
        if (!viewEdit && (mode !== "write" || kind() === "view")) return;
        const inField = ev.target && /^(INPUT|TEXTAREA|SELECT)$/.test(ev.target.tagName);
        if (inField || !root.contains(document.activeElement) && document.activeElement !== document.body) return;
        const doUndo = viewEdit ? vUndoStep : undo, doRedo = viewEdit ? vRedoStep : redo;
        if ((ev.ctrlKey || ev.metaKey) && ev.key.toLowerCase() === "z") { ev.preventDefault(); if (ev.shiftKey) doRedo(); else doUndo(); }
        else if ((ev.ctrlKey || ev.metaKey) && ev.key.toLowerCase() === "y") { ev.preventDefault(); doRedo(); }
    });

    // ------------------------------------------------------------------
    // Code editor (textarea over a syntax-highlighted <pre>)
    // ------------------------------------------------------------------
    function codeEditor(text, lang, readOnly, onInput) {
        const wrap = h("div", "slls-sx-code" + (readOnly ? " readonly" : ""));
        const inner = h("div", "slls-sx-code-inner");
        const pre = h("pre", "");
        const paint = (v) => {
            pre.innerHTML = v ? highlight(v, lang) + (v.endsWith("\n") ? " " : "") : (readOnly ? '<span class="slls-sx-code-empty">(empty)</span>' : " ");
        };
        paint(text || "");
        inner.appendChild(pre);
        let ta = null;
        if (!readOnly) {
            ta = h("textarea", "");
            ta.spellcheck = false;
            ta.value = text || "";
            ta.setAttribute("aria-label", lang === "m" ? "M expression" : "DAX expression");
            ta.addEventListener("input", () => { paint(ta.value); onInput(ta.value); });
            ta.addEventListener("focus", () => wrap.classList.add("focus"));
            ta.addEventListener("blur", () => wrap.classList.remove("focus"));
            ta.addEventListener("keydown", (ev) => {
                if (ev.key === "Tab" && !ev.shiftKey) {
                    ev.preventDefault();
                    const s = ta.selectionStart, e = ta.selectionEnd;
                    ta.value = ta.value.slice(0, s) + "    " + ta.value.slice(e);
                    ta.selectionStart = ta.selectionEnd = s + 4;
                    paint(ta.value);
                    onInput(ta.value);
                }
            });
            inner.appendChild(ta);
        }
        wrap.appendChild(inner);
        return { el: wrap, set(v) { if (ta) ta.value = v; paint(v); } };
    }

    // ------------------------------------------------------------------
    // Detail pane
    // ------------------------------------------------------------------
    function card(title, opts) {
        const o = opts || {};
        const c = h("div", "slls-sx-card");
        // The header holds a toggle button and, beside it, the card's own
        // controls. Those must not be nested inside the toggle: browsers deliver
        // clicks on anything inside a <button> to that button.
        const head = h("div", "slls-sx-card-head");
        const toggle = h("button", "slls-sx-card-toggle");
        toggle.type = "button";
        const filtering = !!propFilter && !o.noFilter;
        const isOpen = filtering || !collapsedCards.has(title);
        toggle.setAttribute("aria-expanded", String(isOpen));
        const caret = h("span", "slls-sx-caret" + (isOpen ? " open" : ""));
        caret.innerHTML = ICON.caret_right;
        toggle.appendChild(caret);
        if (o.dirty) { const d = h("span", "slls-sx-dot"); d.title = "Unsaved change"; toggle.appendChild(d); }
        toggle.appendChild(document.createTextNode(title));
        if (o.count !== undefined) toggle.appendChild(h("span", "slls-sx-count", `(${o.count})`));
        toggle.addEventListener("click", () => {
            if (collapsedCards.has(title)) collapsedCards.delete(title); else collapsedCards.add(title);
            renderDetail();
        });
        const actions = h("span", "slls-sx-card-actions");
        head.append(toggle, actions);
        c.appendChild(head);
        const bodyEl = h("div", "slls-sx-card-body");
        if (isOpen) c.appendChild(bodyEl);
        return { el: c, body: bodyEl, actions, open: isOpen };
    }
    function formatValue(row, v) {
        const span = h("span", "");
        if (v === null || v === undefined || v === "") { span.className = "none"; span.textContent = "(blank)"; return span; }
        if (row.kind === "bool") { span.className = v ? "yes" : "no"; span.textContent = v ? "Yes" : "No"; return span; }
        if (row.kind === "bytes" && typeof v === "number") {
            const units = ["B", "KB", "MB", "GB", "TB"];
            let n = v, i = 0;
            while (n >= 1024 && i < units.length - 1) { n /= 1024; i += 1; }
            span.textContent = `${n.toFixed(i ? 1 : 0)} ${units[i]}`;
            return span;
        }
        // Version-like numbers (e.g. compatibility level 1706) read wrong with
        // thousands separators; quantities keep them.
        const plain = /(^|\.)CompatibilityLevel$/.test(row.key);
        span.textContent = typeof v === "number" && !plain ? v.toLocaleString() : String(v);
        return span;
    }
    function propEditor(node, row) {
        const v = value(node, row.key);
        const commit = (nv) => setValue(node, row.key, nv, { label: row.label });
        if (row.kind === "bool") {
            const on = !!v;
            const sw = h("button", "slls-sx-switch" + (on ? " on" : ""));
            sw.type = "button";
            sw.setAttribute("role", "switch");
            sw.setAttribute("aria-checked", String(on));
            sw.setAttribute("aria-label", row.label);
            sw.append(h("span", "slls-sx-track"), document.createTextNode(on ? "Yes" : "No"));
            sw.addEventListener("click", () => commit(!on));
            return sw;
        }
        if (row.kind === "enum" || row.kind === "column_ref") {
            const sel = h("select", "slls-sx-select");
            sel.setAttribute("aria-label", row.label);
            let options = [];
            if (row.kind === "enum") options = ((data.enums || {})[row.enum] || ENUM_FALLBACK[row.enum] || []).map((x) => [x, x]);
            else {
                const t = node.parent || tableByName.get(node.table);
                options = [["", "(none)"], ...((t && t.column_names) || []).filter((c) => c !== node.name)
                    .map((c) => [c, columnDisplayName(node.table, c)])];
            }
            if (v && !options.some(([val]) => val === v)) options.unshift([v, v]);
            for (const [val, label] of options) sel.appendChild(new Option(label, val));
            sel.value = v == null ? "" : String(v);
            sel.addEventListener("change", () => commit(sel.value));
            return sel;
        }
        if (row.kind === "longtext") {
            const ta = h("textarea", "slls-sx-textarea");
            ta.rows = 2;
            ta.value = v == null ? "" : String(v);
            ta.setAttribute("aria-label", row.label);
            ta.addEventListener("change", () => commit(ta.value));
            return ta;
        }
        const input = h("input", "slls-sx-input");
        input.type = row.kind === "number" ? "number" : "text";
        input.value = v == null ? "" : String(v);
        input.setAttribute("aria-label", row.label);
        input.addEventListener("change", () => {
            if (row.kind === "number") {
                if (input.value.trim() === "") { commit(null); return; }
                const n = Number(input.value);
                if (!Number.isFinite(n)) { input.value = v == null ? "" : String(v); return; }
                commit(n);
            } else if (row.key === "Name") {
                const name = input.value.trim();
                if (!name) { input.value = String(v); return; }
                commit(name);
            } else commit(input.value);
        });
        input.addEventListener("keydown", (ev) => { if (ev.key === "Enter") input.blur(); });
        return input;
    }
    function propMatches(text) {
        return !propFilter || String(text).toLowerCase().includes(propFilter.toLowerCase());
    }
    // `afterGroup(group)` lets the caller place extra cards after a group.
    function renderProps(node, container, afterGroup) {
        const groups = new Map();
        for (const row of node.props || []) {
            if (!propMatches(row.label) && !propMatches(row.key)) continue;
            if (!groups.has(row.group)) groups.set(row.group, []);
            groups.get(row.group).push(row);
        }
        const write = mode === "write" && !saving && !isDeleted(node);
        for (const [group, rows] of groups) {
            const dirty = rows.some((r) => staging.edits[editKey(node.key, r.key)] || (node.isNew && createFor(node).props[r.key] !== undefined));
            const c = card(group, { dirty });
            if (c.open) {
                const grid = h("div", "slls-sx-props");
                for (const row of rows) {
                    const label = h("div", "slls-sx-plabel");
                    if (staging.edits[editKey(node.key, row.key)]) { const d = h("span", "slls-sx-dot"); d.title = "Unsaved change"; label.appendChild(d); }
                    label.appendChild(h("span", "", row.label));
                    label.title = row.key;
                    const val = h("div", "slls-sx-pvalue" + (row.kind === "longtext" ? " long" : "") + (row.key === "ErrorMessage" ? " error" : ""));
                    if (write && row.editable) val.appendChild(propEditor(node, row));
                    else val.appendChild(formatValue(row, value(node, row.key)));
                    grid.append(label, val);
                }
                c.body.appendChild(grid);
            }
            container.appendChild(c.el);
            if (afterGroup) afterGroup(group);
        }
    }
    // The object's main expression ("Expression") is shown at the top; the
    // secondary ones (format string / detail rows) sit with the properties.
    function renderExpressions(node, container, secondary) {
        const write = mode === "write" && !saving && !isDeleted(node);
        for (const expr of node.expressions || []) {
            if ((expr.prop !== "Expression") !== !!secondary) continue;
            const target = expr.target ? getNode(expr.target) : node;
            if (!target) continue;
            const current = value(target, expr.prop);
            if (propFilter && !propMatches(expr.label)) continue;
            const dirty = !!staging.edits[editKey(target.key, expr.prop)] || (node.isNew && createFor(node).props[expr.prop] !== undefined);
            const c = card(expr.label, { dirty, noFilter: true });
            if (expr.lang === "dax" && write) {
                const fmt = button("slls-sx-btn small", "Format DAX", "wand", () => {
                    const text = value(target, expr.prop) || "";
                    if (!text.trim()) return;
                    fmt.disabled = true;
                    errEl.textContent = "";
                    request("format_dax", { expression: text }, (res) => {
                        fmt.disabled = false;
                        if (!res.ok) { errEl.textContent = res.error || "Formatting failed."; return; }
                        const formatted = (res.data || {}).formatted || "";
                        if (formatted && formatted !== text) setValue(target, expr.prop, formatted, { label: expr.label });
                    });
                }, "Format with DAX Formatter (daxformatter.com)");
                c.actions.appendChild(fmt);
            }
            const expand = button("slls-sx-btn small icon", "", "expand_diagonal", () => openExpressionEditor(node, target, expr), "Open in a larger editor");
            c.actions.appendChild(expand);
            const errEl = h("div", "slls-sx-code-error");
            if (c.open) {
                const ed = codeEditor(current || "", expr.lang, !write, (v) => {
                    setValue(target, expr.prop, v, { label: expr.label, coalesce: true, keepDetail: true });
                });
                c.body.append(ed.el, errEl);
            }
            container.appendChild(c.el);
        }
    }
    function openExpressionEditor(node, target, expr) {
        const write = mode === "write" && !saving && !isDeleted(node);
        const m = modal(`${expr.label} \u2014 ${objectLabel(node)}`, expr.lang === "m" ? "code" : "measure", { wide: true });
        const ed = codeEditor(value(target, expr.prop) || "", expr.lang, !write, (v) => {
            setValue(target, expr.prop, v, { label: expr.label, coalesce: true, keepDetail: true });
        });
        ed.el.style.height = "100%";
        m.body.style.padding = "0";
        ed.el.style.border = "none";
        ed.el.style.borderRadius = "0";
        m.body.appendChild(ed.el);
        m.onClose = () => renderDetail();
        const ta = ed.el.querySelector("textarea");
        if (ta) setTimeout(() => ta.focus(), 0);
    }

    function kvCard(title, node, prop, entries, editable) {
        const keys = Object.keys(entries);
        const c = card(title, { count: keys.length, dirty: !!staging.edits[editKey(node.key, prop)] });
        if (editable) {
            const add = button("slls-sx-btn small", "Add", "plus", () => {
                promptDialog(`Add ${title.replace(/s$/, "").toLowerCase()}`, "Name", (name) => {
                    const next = Object.assign({}, entries);
                    next[name] = next[name] || "";
                    setValue(node, prop, JSON.stringify(next), { label: title });
                });
            });
            c.actions.appendChild(add);
        }
        if (!c.open) return c.el;
        if (!keys.length) { c.body.appendChild(h("div", "slls-sx-kv-empty", `No ${title.toLowerCase()}.`)); return c.el; }
        const grid = h("div", "slls-sx-kv");
        for (const k of keys.sort()) {
            grid.appendChild(h("div", "k", k));
            if (editable) {
                const ta = h("textarea", "slls-sx-textarea");
                ta.rows = 1;
                ta.value = entries[k];
                ta.setAttribute("aria-label", k);
                ta.addEventListener("change", () => {
                    const next = Object.assign({}, entries);
                    next[k] = ta.value;
                    setValue(node, prop, JSON.stringify(next), { label: title });
                });
                grid.appendChild(ta);
                grid.appendChild(button("slls-sx-iconbtn", "", "trash", () => {
                    const next = Object.assign({}, entries);
                    delete next[k];
                    setValue(node, prop, JSON.stringify(next), { label: title });
                }, `Remove ${k}`));
            } else {
                grid.appendChild(h("div", "v", entries[k]));
                grid.appendChild(h("span", ""));
            }
        }
        c.body.appendChild(grid);
        return c.el;
    }
    function translationsCard(node, write) {
        const cultures = data.cultures || [];
        const tr = jsonValue(node, "Translations", {});
        const c = card("Translations", { count: Object.keys(tr).filter((k) => Object.values(tr[k] || {}).some(Boolean)).length,
            dirty: !!staging.edits[editKey(node.key, "Translations")] });
        if (!c.open) return c.el;
        const table = h("table", "slls-sx-grid");
        const fields = [["caption", "Caption"], ["description", "Description"], ["displayFolder", "Display folder"]]
            .filter(([f]) => f !== "displayFolder" || ["column", "measure", "hierarchy"].includes(node.type));
        table.innerHTML = `<thead><tr><th>Culture</th>${fields.map(([, l]) => `<th>${l}</th>`).join("")}</tr></thead>`;
        const tbody = h("tbody");
        for (const culture of cultures) {
            const tr2 = h("tr");
            tr2.appendChild(h("td", "", culture.name));
            for (const [f] of fields) {
                const td = h("td");
                const cur = ((tr[culture.name] || {})[f]) || "";
                if (write) {
                    const input = h("input", "slls-sx-input");
                    input.value = cur;
                    input.setAttribute("aria-label", `${culture.name} ${f}`);
                    input.addEventListener("change", () => {
                        const next = JSON.parse(JSON.stringify(tr));
                        next[culture.name] = Object.assign({}, next[culture.name] || {}, { [f]: input.value });
                        setValue(node, "Translations", JSON.stringify(next), { label: "Translations" });
                    });
                    td.appendChild(input);
                } else td.appendChild(cur ? document.createTextNode(cur) : Object.assign(h("span", "slls-sx-kv-empty", "\u2014")));
                tr2.appendChild(td);
            }
            tbody.appendChild(tr2);
        }
        table.appendChild(tbody);
        c.body.appendChild(table);
        return c.el;
    }
    function perspectivesCard(node, write) {
        const all = (data.perspectives || []);
        const member = new Set(jsonValue(node, "Perspectives", []));
        const c = card("Perspectives", { count: member.size, dirty: !!staging.edits[editKey(node.key, "Perspectives")] });
        if (!c.open) return c.el;
        const wrap = h("div", "slls-sx-checks");
        for (const p of all) {
            const chip = h("label", "slls-sx-check" + (member.has(p.name) ? " on" : "") + (write ? " editable" : ""));
            if (write) {
                const cb = h("input");
                cb.type = "checkbox";
                cb.checked = member.has(p.name);
                cb.addEventListener("change", () => {
                    const next = new Set(member);
                    if (cb.checked) next.add(p.name); else next.delete(p.name);
                    setValue(node, "Perspectives", JSON.stringify([...next].sort()), { label: "Perspectives" });
                });
                chip.appendChild(cb);
            }
            chip.appendChild(document.createTextNode(effName(p)));
            wrap.appendChild(chip);
        }
        c.body.appendChild(wrap);
        return c.el;
    }
    function subList(title, nodes, metaFn) {
        const c = card(title, { count: nodes.length });
        if (!c.open) return c.el;
        if (!nodes.length) { c.body.appendChild(h("div", "slls-sx-kv-empty", "None.")); return c.el; }
        const list = h("div", "slls-sx-sublist");
        for (const n of nodes) {
            const item = h("div", "slls-sx-subitem");
            item.append(iconEl(nodeIcon(n), ""), h("span", "", displayLabel(n)));
            const meta = metaFn ? metaFn(n) : "";
            if (meta) item.appendChild(h("span", "meta", meta));
            item.addEventListener("click", () => reveal(n.key));
            list.appendChild(item);
        }
        c.body.appendChild(list);
        return c.el;
    }
    function roleCards(node, container, write) {
        // Members
        const members = jsonValue(node, "Members", []);
        const mc = card("Members", { count: members.length, dirty: !!staging.edits[editKey(node.key, "Members")] });
        if (mc.open) {
            if (write) {
                const ta = h("textarea", "slls-sx-textarea");
                ta.rows = Math.min(8, Math.max(3, members.length + 1));
                ta.placeholder = "One user or group per line (e.g. user@contoso.com)";
                ta.value = members.join("\n");
                ta.setAttribute("aria-label", "Role members");
                ta.addEventListener("change", () => {
                    const list = ta.value.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
                    setValue(node, "Members", JSON.stringify(list), { label: "Members" });
                });
                mc.body.appendChild(ta);
            } else if (!members.length) mc.body.appendChild(h("div", "slls-sx-kv-empty", "No members."));
            else {
                const list = h("div", "slls-sx-sublist");
                for (const m of members) { const it = h("div", "slls-sx-subitem"); it.append(iconEl("user", ""), h("span", "", m)); list.appendChild(it); }
                mc.body.appendChild(list);
            }
        }
        container.appendChild(mc.el);

        // Row-level security (a null filter marks a saved filter being removed)
        const filters = jsonValue(node, "RlsFilters", {});
        const savedFilters = JSON.parse(originalValue(node, "RlsFilters"));
        const activeTables = Object.keys(filters).filter((k) => filters[k] !== null);
        const rc = card("Row-level security", { count: activeTables.length, dirty: !!staging.edits[editKey(node.key, "RlsFilters")] });
        if (write) {
            const sel = h("select", "slls-sx-select");
            sel.style.width = "200px";
            sel.setAttribute("aria-label", "Add a table filter");
            sel.appendChild(new Option("Add table filter\u2026", ""));
            for (const t of data.tables || []) if (filters[t.name] == null) sel.appendChild(new Option(effName(t), t.name));
            sel.addEventListener("change", () => {
                if (!sel.value) return;
                const next = Object.assign({}, filters, { [sel.value]: "TRUE()" });
                setValue(node, "RlsFilters", JSON.stringify(next), { label: "Row-level security" });
            });
            rc.actions.appendChild(sel);
        }
        if (rc.open) {
            const keys = activeTables.sort();
            if (!keys.length) rc.body.appendChild(h("div", "slls-sx-kv-empty", "No row-level security filters."));
            const wrap = h("div", "slls-sx-rls");
            for (const table of keys) {
                const block = h("div", "");
                const head = h("div", "slls-sx-rls-head");
                head.append(iconEl("table", ""), h("span", "", tableDisplayName(table)));
                if (write) {
                    const rm = button("slls-sx-iconbtn", "", "trash", () => {
                        const next = Object.assign({}, filters);
                        if (table in savedFilters) next[table] = null; else delete next[table];
                        setValue(node, "RlsFilters", JSON.stringify(next), { label: "Row-level security" });
                    }, `Remove the ${table} filter`);
                    rm.style.marginLeft = "auto";
                    head.appendChild(rm);
                }
                block.appendChild(head);
                const ed = codeEditor(filters[table], "dax", !write, (v) => {
                    const next = Object.assign({}, jsonValue(node, "RlsFilters", {}), { [table]: v });
                    setValue(node, "RlsFilters", JSON.stringify(next), { label: "Row-level security", coalesce: true, keepDetail: true });
                });
                block.appendChild(ed.el);
                wrap.appendChild(block);
            }
            rc.body.appendChild(wrap);
        }
        container.appendChild(rc.el);

        // Object-level security (read-only)
        const ols = [];
        for (const tp of node.table_permissions || []) {
            if (tp.permission && tp.permission !== "Default") ols.push([tableDisplayName(tp.table), "(table)", tp.permission]);
            for (const cp of tp.columns || []) ols.push([tableDisplayName(tp.table), columnDisplayName(tp.table, cp.column), cp.permission]);
        }
        const oc = card("Object-level security", { count: ols.length });
        if (oc.open) {
            if (!ols.length) oc.body.appendChild(h("div", "slls-sx-kv-empty", "No object-level security."));
            else {
                const table = h("table", "slls-sx-grid");
                table.innerHTML = "<thead><tr><th>Table</th><th>Column</th><th>Permission</th></tr></thead>";
                const tb = h("tbody");
                for (const r of ols) { const tr = h("tr"); for (const v of r) tr.appendChild(h("td", "", v)); tb.appendChild(tr); }
                table.appendChild(tb);
                oc.body.appendChild(table);
            }
        }
        container.appendChild(oc.el);
    }

    function renderDetail() {
        if (kind() === "view") { if (viewOpen()) renderViewDetail(); return; }
        // Re-rendering the same object (a card toggled, a property edited) keeps
        // the scroll position; only selecting another object starts at the top.
        const previous = detailBody ? { key: detailBodyKey, top: detailBody.scrollTop } : null;
        clear(detailPane);
        const node = getNode(selectedKey) || (data.model || null);
        if (!node) return;
        if (node.key !== selectedKey) selectedKey = node.key;
        const write = mode === "write" && !saving && !isDeleted(node);

        const head = h("div", "slls-sx-detail-head");
        head.appendChild(iconEl(nodeIcon(node), "slls-sx-objicon"));
        const titles = h("div", "slls-sx-objtitle");
        const nameEl = h("div", "slls-sx-objname", node.type === "model" ? (model.get("dataset_name") || "Model") : displayLabel(node));
        nameEl.title = nameEl.textContent;
        titles.appendChild(nameEl);
        const typeEl = h("div", "slls-sx-objtype", typeLabel(node) + (node.table && node.type !== "table" ? ` \u00b7 ${tableDisplayName(node.table)}` : ""));
        if (node.isNew) typeEl.appendChild(h("span", "slls-sx-pill new", "New"));
        if (isDeleted(node)) typeEl.appendChild(h("span", "slls-sx-pill danger", "Pending delete"));
        if (isDuplicated(node)) typeEl.appendChild(h("span", "slls-sx-pill warn", "Pending duplicate"));
        if (node.type !== "model" && node.type !== "level" && node.type !== "relationship" && isHidden(node)) typeEl.appendChild(h("span", "slls-sx-pill", "Hidden"));
        if (node.type === "relationship" && !value(node, "IsActive")) typeEl.appendChild(h("span", "slls-sx-pill", "Inactive"));
        if (node.type === "table" && node.mode) typeEl.appendChild(h("span", "slls-sx-pill", node.mode === "DirectLake" ? "Direct Lake" : node.mode));
        titles.appendChild(typeEl);
        head.appendChild(titles);
        const stats = h("div", "slls-sx-stats");
        const stat = (n, label) => { const s = h("div", "slls-sx-stat"); s.append(h("b", "", n.toLocaleString()), h("span", "", label)); stats.appendChild(s); };
        if (node.type === "model") {
            const tables = data.tables || [];
            stat(tables.length, "tables");
            stat(tables.reduce((a, t) => a + (t.columns || []).length, 0), "columns");
            stat(tables.reduce((a, t) => a + (t.measures || []).length, 0), "measures");
            stat((data.relationships || []).length, "relationships");
        } else if (node.type === "table") {
            stat((node.columns || []).length, "columns");
            stat((node.measures || []).length, "measures");
            stat((node.hierarchies || []).length, "hierarchies");
            stat((node.partitions || []).length, "partitions");
        }
        head.appendChild(stats);
        detailPane.appendChild(head);

        const actions = h("div", "slls-sx-detail-actions");
        if (!node.isNew) {
            actions.appendChild(button("slls-sx-btn small", "TMDL", "file", () => openTmdl(node), "View the TMDL definition"));
            if (["table", "column", "measure", "calculationItem", "hierarchy", "partition", "expression", "function", "relationship"].includes(node.type)) {
                actions.appendChild(button("slls-sx-btn small", "Dependencies", "workflow", () => openDependencies(node), "Show dependencies"));
            }
        }
        if (mode === "write" && !saving) {
            if (node.type === "table" && !isDeleted(node)) {
                actions.appendChild(button("slls-sx-btn small", "Measure", "plus", () => addObject("measure", node), "Add a measure to this table"));
                if (node.kind === "calculation_group") actions.appendChild(button("slls-sx-btn small", "Item", "plus", () => addObject("calculationItem", node), "Add a calculation item"));
                else actions.appendChild(button("slls-sx-btn small", "Column", "plus", () => addObject("column", node), "Add a calculated column"));
            }
            if (["table", "column", "measure", "hierarchy", "function"].includes(node.type) && !isDeleted(node)) {
                const hidden = isHidden(node);
                actions.appendChild(button("slls-sx-btn small", hidden ? "Unhide" : "Hide", hidden ? "eye" : "eye_off",
                    () => setValue(node, "IsHidden", !hidden, { label: "Hidden" })));
            }
            if (!node.isNew && ["table", "column", "measure", "hierarchy", "calculationItem", "role", "perspective", "expression"].includes(node.type) && !isDeleted(node)) {
                actions.appendChild(button("slls-sx-btn small", isDuplicated(node) ? "Undo duplicate" : "Duplicate", "plus", () => toggleOp(node, "duplicate")));
            }
            if (!["model", "culture"].includes(node.type)) {
                const del = staging.ops.some((o) => o.op === "delete" && o.key === node.key);
                if (del || !isDeleted(node)) {
                    actions.appendChild(button("slls-sx-btn small" + (del ? "" : " danger"), del ? "Restore" : "Delete", del ? "undo" : "trash", () => toggleOp(node, "delete")));
                }
            }
        }
        const ps = h("div", "slls-sx-search slls-sx-propsearch");
        ps.appendChild(iconEl("search", ""));
        const psInput = h("input", "slls-sx-input");
        psInput.type = "search";
        psInput.placeholder = "Filter properties\u2026";
        psInput.value = propFilter;
        psInput.addEventListener("input", () => {
            propFilter = psInput.value;
            renderDetailBody(node, write);
            psInput.focus();
        });
        ps.appendChild(psInput);
        actions.appendChild(ps);
        detailPane.appendChild(actions);

        detailBody = h("div", "slls-sx-detail-body");
        detailBodyKey = node.key;
        detailPane.appendChild(detailBody);
        renderDetailBody(node, write);
        if (previous && previous.key === node.key) detailBody.scrollTop = previous.top;
    }
    let detailBody = null;
    let detailBodyKey = "";
    function renderDetailBody(node, write) {
        clear(detailBody);
        const b = detailBody;
        if (node.type === "relationship") {
            const rel = h("div", "slls-sx-rel");
            const end = (t, c, card) => {
                const e = h("div", "slls-sx-rel-end");
                const tl = h("b", "slls-sx-link", tableDisplayName(t));
                tl.addEventListener("click", () => reveal(keyOf("table", "", "", t)));
                e.append(tl, h("span", "", `${columnDisplayName(t, c)} \u00b7 ${card === "Many" ? "Many (*)" : "One (1)"}`));
                return e;
            };
            const mid = h("div", "slls-sx-rel-mid");
            mid.appendChild(iconEl("relationship", ""));
            const cf = value(node, "CrossFilteringBehavior");
            mid.appendChild(h("span", "", cf === "BothDirections" ? "\u21c4 Both" : "\u2190 Single"));
            rel.append(end(node.from_table, node.from_column, value(node, "FromCardinality")), mid, end(node.to_table, node.to_column, value(node, "ToCardinality")));
            b.appendChild(rel);
        }
        if (node.type === "model" && (data.errors || []).length) {
            const err = h("div", "slls-sx-rel");
            err.style.cssText = "border-color: var(--ui-danger-border); background: var(--ui-danger-bg); color: var(--ui-danger-text);";
            err.append(iconEl("alert", ""), h("span", "", `${plural(data.errors.length, "object")} in an error state.`));
            const view = h("a", "slls-sx-link", "View errors");
            view.addEventListener("click", openErrors);
            err.appendChild(view);
            b.appendChild(err);
        }
        renderExpressions(node, b, false);
        // Secondary expressions follow the Formatting section (or General when
        // the object has no Formatting properties, e.g. calculation items).
        const groups = new Set((node.props || []).filter((r) => propMatches(r.label) || propMatches(r.key)).map((r) => r.group));
        const anchor = groups.has("Formatting") ? "Formatting" : "General";
        let placed = false;
        renderProps(node, b, (group) => {
            if (group === anchor) { renderExpressions(node, b, true); placed = true; }
        });
        if (!placed) renderExpressions(node, b, true);
        if (propFilter) return;
        if (node.type === "hierarchy") {
            b.appendChild(subList("Levels", node.levels || [], (lv) => columnDisplayName(node.table, lv.column)));
        }
        if (node.type === "table" && !node.isNew) {
            b.appendChild(subList("Partitions", node.partitions || [], (p) => `${p.mode} \u00b7 ${p.source_type}`));
        }
        if (node.type === "perspective" && node.counts) {
            const c = card("Members");
            if (c.open) {
                const grid = h("div", "slls-sx-props");
                for (const [k, label] of [["tables", "Tables"], ["columns", "Columns"], ["measures", "Measures"], ["hierarchies", "Hierarchies"]]) {
                    grid.append(h("div", "slls-sx-plabel", label), h("div", "slls-sx-pvalue", (node.counts[k] || 0).toLocaleString()));
                }
                c.body.appendChild(grid);
            }
            b.appendChild(c.el);
        }
        if (node.type === "culture") {
            const c = card("Translations");
            if (c.open) {
                const grid = h("div", "slls-sx-props");
                grid.append(h("div", "slls-sx-plabel", "Translated objects"), h("div", "slls-sx-pvalue", (node.translated_count || 0).toLocaleString()),
                    h("div", "slls-sx-plabel", "Linguistic metadata"), h("div", "slls-sx-pvalue", node.has_linguistic_metadata ? "Yes" : "No"));
                c.body.appendChild(grid);
            }
            b.appendChild(c.el);
        }
        if (node.type === "role") roleCards(node, b, write);
        if (["table", "column", "measure", "hierarchy"].includes(node.type) && (data.perspectives || []).length && !node.isNew) {
            b.appendChild(perspectivesCard(node, write));
        }
        if (["table", "column", "measure", "hierarchy", "level"].includes(node.type) && (data.cultures || []).length && !node.isNew) {
            b.appendChild(translationsCard(node, write));
        }
        if (!node.isNew) {
            b.appendChild(kvCard("Annotations", node, "Annotations", jsonValue(node, "Annotations", {}), write));
            const ext = node.extended_properties || {};
            if (Object.keys(ext).length) b.appendChild(kvCard("Extended properties", node, "ExtendedProperties", ext, false));
        }
    }

    // ------------------------------------------------------------------
    // Modals: TMDL, dependencies, history, errors
    // ------------------------------------------------------------------
    function modal(title, icon, opts) {
        const o = opts || {};
        const overlay = h("div", "slls-sx-overlay");
        const box = h("div", "slls-sx-modal" + (o.wide ? " wide" : ""));
        box.setAttribute("role", "dialog");
        box.setAttribute("aria-modal", "true");
        const head = h("div", "slls-sx-modal-head");
        head.appendChild(iconEl(icon, ""));
        head.appendChild(h("div", "slls-sx-modal-title", title));
        const actions = h("div", "slls-sx-card-actions");
        head.appendChild(actions);
        const result = { overlay, box, body: null, actions, onClose: null, close: null };
        const close = () => {
            overlay.remove();
            document.removeEventListener("keydown", onKey, true);
            if (result.onClose) result.onClose();
        };
        // Escape closes only the topmost dialog (one can open over another).
        const onKey = (ev) => {
            if (ev.key !== "Escape") return;
            const open = root.querySelectorAll(".slls-sx-overlay");
            if (open[open.length - 1] !== overlay) return;
            ev.stopPropagation();
            close();
        };
        head.appendChild(button("slls-sx-iconbtn", "", "close", close, "Close"));
        box.appendChild(head);
        if (o.tabs) box.appendChild(o.tabs);
        const bodyEl = h("div", "slls-sx-modal-body");
        box.appendChild(bodyEl);
        overlay.appendChild(box);
        overlay.addEventListener("mousedown", (ev) => { if (ev.target === overlay) close(); });
        root.appendChild(overlay);
        document.addEventListener("keydown", onKey, true);
        result.body = bodyEl;
        result.close = close;
        return result;
    }
    // In-widget replacements for window.confirm / window.prompt, which notebook
    // webviews (e.g. VS Code) do not support.
    function confirmDialog(message, okLabel, onOk) {
        const m = modal("Please confirm", "alert");
        m.body.appendChild(h("div", "", message));
        const foot = h("div", "slls-sx-modal-foot");
        const ok = button("slls-sx-btn primary", okLabel || "OK", "", () => { m.close(); onOk(); });
        foot.append(button("slls-sx-btn", "Cancel", "", () => m.close()), ok);
        m.box.appendChild(foot);
        setTimeout(() => ok.focus(), 0);
    }
    function promptDialog(title, label, onOk) {
        const m = modal(title, "plus");
        const row = h("div", "slls-sx-prompt-row");
        const input = h("input", "slls-sx-input");
        input.setAttribute("aria-label", label);
        input.placeholder = label;
        row.appendChild(input);
        m.body.appendChild(row);
        const foot = h("div", "slls-sx-modal-foot");
        const ok = button("slls-sx-btn primary", "Add", "", () => {
            const v = input.value.trim();
            if (!v) { input.focus(); return; }
            m.close();
            onOk(v);
        });
        input.addEventListener("keydown", (ev) => { if (ev.key === "Enter") ok.click(); });
        foot.append(button("slls-sx-btn", "Cancel", "", () => m.close()), ok);
        m.box.appendChild(foot);
        setTimeout(() => input.focus(), 0);
    }
    function loadingInto(container, text) {
        clear(container);
        const c = h("div", "slls-sx-center");
        const sp = iconEl("refresh", "");
        sp.querySelector("svg") && sp.querySelector("svg").classList.add("slls-sx-spin");
        c.append(sp, h("div", "", text));
        container.appendChild(c);
    }
    function errorInto(container, text) {
        clear(container);
        const c = h("div", "slls-sx-center");
        c.style.color = "var(--ui-danger-text)";
        c.append(iconEl("alert", ""), h("div", "", text));
        container.appendChild(c);
    }
    // ------------------------------------------------------------------
    // TMDL / JSON syntax highlighting
    // ------------------------------------------------------------------
    const TMDL_OBJECTS = new Set([
        "model", "database", "table", "column", "measure", "hierarchy", "level", "partition",
        "relationship", "expression", "role", "perspective", "perspectiveTable", "perspectiveColumn",
        "perspectiveMeasure", "perspectiveHierarchy", "culture", "linguisticMetadata", "annotation",
        "extendedProperty", "calculationGroup", "calculationItem", "tablePermission", "columnPermission",
        "member", "dataSource", "function", "formatStringDefinition", "detailRowsDefinition", "kpi",
        "refreshPolicy", "queryGroup", "variation", "changedProperty", "ref", "createOrReplace",
        "createOrUpdate", "alter", "delete", "translation", "calendar",
    ]);
    function tmdlIndent(ws) {
        let n = 0;
        for (const ch of ws) n += ch === "\t" ? 1 : 0.25;
        return Math.floor(n);
    }
    function tmdlScalar(text) {
        const t = text.trim();
        if (!t) return esc(text);
        const lead = text.slice(0, text.indexOf(t[0]));
        let kind = "";
        if (/^(true|false)$/i.test(t)) kind = "keyword";
        else if (/^-?\d+(\.\d+)?$/.test(t)) kind = "number";
        else if (/^".*"$/.test(t) || /^'.*'$/.test(t)) kind = "string";
        return esc(lead) + (kind ? `<span class="slls-sx-tk-${kind}">${esc(t)}</span>` : esc(t)) + esc(text.slice(lead.length + t.length));
    }
    // TMDL is line-oriented: an object line ("measure 'Sales' = <DAX>"), a
    // property line ("formatString: 0.00") or a flag ("isHidden"). Expressions
    // continue on lines indented deeper than the object's properties (or in a
    // ``` block); they are collected and colored with the DAX / M highlighter.
    function highlightTmdl(text) {
        const lines = String(text || "").split(/\r\n?|\n/);
        const out = [];
        let expr = null;
        let partitionLang = "dax";
        const flush = () => {
            if (!expr) return;
            if (expr.lines.length) out.push(highlight(expr.lines.join("\n"), expr.lang));
            expr = null;
        };
        const startExpr = (indent, lang, fenced) => { expr = { indent, lang, fenced, lines: [] }; };
        for (const line of lines) {
            const m = /^(\s*)(.*)$/.exec(line);
            const ws = m[1];
            const rest = m[2];
            if (expr) {
                if (expr.fenced) {
                    if (rest.trim() === "```") { flush(); out.push(esc(ws) + '<span class="slls-sx-tk-punctuation">```</span>'); continue; }
                    expr.lines.push(line);
                    continue;
                }
                if (!rest.trim() || tmdlIndent(ws) > expr.indent + 1) { expr.lines.push(line); continue; }
                flush();
            }
            if (rest.startsWith("///")) { out.push(esc(ws) + `<span class="slls-sx-tk-comment">${esc(rest)}</span>`); continue; }
            const w = /^([A-Za-z][A-Za-z0-9]*)(.*)$/.exec(rest);
            if (!w) { out.push(esc(line)); continue; }
            const word = w[1];
            let tail = w[2];
            const indent = tmdlIndent(ws);
            if (tail.startsWith(":")) {
                out.push(`${esc(ws)}<span class="slls-sx-tk-variable">${esc(word)}</span><span class="slls-sx-tk-punctuation">:</span>${tmdlScalar(tail.slice(1))}`);
                continue;
            }
            let html = esc(ws);
            const isObject = TMDL_OBJECTS.has(word);
            html += `<span class="slls-sx-tk-${isObject ? "keyword" : "variable"}">${esc(word)}</span>`;
            const eq = tail.indexOf("=");
            const namePart = eq >= 0 ? tail.slice(0, eq) : tail;
            if (isObject && namePart.trim()) html += `<span class="slls-sx-tmdl-name">${esc(namePart)}</span>`;
            else html += esc(namePart);
            if (eq < 0) { out.push(html); continue; }
            html += '<span class="slls-sx-tk-operator">=</span>';
            const value = tail.slice(eq + 1);
            if (word === "partition") {
                partitionLang = /^\s*m\s*$/i.test(value) ? "m" : "dax";
                out.push(html + tmdlScalar(value));
                continue;
            }
            const lang = word === "expression" || (word === "source" && partitionLang === "m") ? "m" : "dax";
            if (value.trim() === "```") {
                out.push(html + esc(value));
                startExpr(indent, lang, true);
                continue;
            }
            out.push(html + (value.trim() ? highlight(value, lang) : esc(value)));
            startExpr(indent, lang, false);
        }
        flush();
        return out.join("\n");
    }
    function highlightJson(text) {
        const re = /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;
        let out = "", cursor = 0, mt;
        const src = String(text || "");
        while ((mt = re.exec(src))) {
            out += esc(src.slice(cursor, mt.index));
            if (mt[1]) out += `<span class="slls-sx-tk-${mt[2] ? "variable" : "string"}">${esc(mt[1])}</span>${mt[2] ? esc(mt[2]) : ""}`;
            else if (mt[3]) out += `<span class="slls-sx-tk-keyword">${mt[3]}</span>`;
            else out += `<span class="slls-sx-tk-number">${mt[4]}</span>`;
            cursor = re.lastIndex;
        }
        return out + esc(src.slice(cursor));
    }

    function openTmdl(node) {
        const m = modal(`TMDL \u2014 ${objectLabel(node)}`, "file", { wide: true });
        loadingInto(m.body, "Serializing\u2026");
        request("tmdl", { target: targetOf(node) }, (res) => {
            if (!m.overlay.isConnected) return;
            if (!res.ok) { errorInto(m.body, res.error || "Could not serialize the object."); return; }
            clear(m.body);
            const data_ = res.data || {};
            const text = data_.text || "";
            if (isDirty(node)) {
                const note = h("div", "slls-sx-code-error", "Showing the saved definition \u2014 pending changes are not included.");
                note.style.margin = "0 0 8px";
                m.body.appendChild(note);
            }
            const pre = h("pre", "slls-sx-tmdl");
            pre.innerHTML = data_.language === "json" ? highlightJson(text) : highlightTmdl(text);
            m.body.appendChild(pre);
            const copy = button("slls-sx-btn small", "Copy", "", () => {
                try { navigator.clipboard.writeText(text); copy.textContent = "Copied"; } catch (e) { copy.textContent = "Copy failed"; }
            });
            m.actions.appendChild(copy);
        });
    }

    // ------------------------------------------------------------------
    // Dependencies (tree + node graph)
    // ------------------------------------------------------------------
    // The model's dependency graph is fetched once per model version; the
    // tree / graph for any object (in either direction) is derived from it.
    let depGraphCache = null;
    const DEP_MAX_DEPTH = 30;
    const DEP_MAX_NODES = 3000;
    const GRAPH = { w: 200, h: 46, colGap: 72, rowGap: 18, pad: 28 };
    function depCategory(type) {
        if (type === "measure") return "measure";
        if (type === "column") return "column";
        if (type === "hierarchy" || type === "level") return "hierarchy";
        if (type === "calculationItem") return "item";
        if (type === "table" || type === "partition") return "table";
        if (type === "role") return "role";
        return "other";
    }
    function depLabel(n) {
        const known = n.key ? getNode(n.key) : null;
        if (known) return objectLabel(known);
        if (n.type === "table" || !n.table) return n.name;
        return `'${n.table}'[${n.name}]`;
    }
    function depGraphLabel(n) {
        if (n.type === "measure") {
            const known = n.key ? getNode(n.key) : null;
            return `[${known ? effName(known) : n.name}]`;
        }
        return depLabel(n);
    }
    function depTypeLabel(n) {
        const known = n.key ? getNode(n.key) : null;
        return known ? typeLabel(known) : (TYPE_LABEL[n.type] || n.type);
    }
    function loadDependencyGraph(onDone) {
        const version = model.get("model_version");
        if (depGraphCache && depGraphCache.version === version) { onDone(depGraphCache, null); return; }
        request("dependencies", {}, (res) => {
            if (!res.ok) { onDone(null, res.error || "Could not read the dependencies."); return; }
            const raw = res.data || {};
            const nodes = (raw.nodes || []).map((n) => Object.assign({}, n));
            const byKey = new Map();
            nodes.forEach((n, i) => { if (n.key) byKey.set(n.key, i); });
            const forward = new Map(), reverse = new Map();
            for (const [a, b] of raw.edges || []) {
                if (!forward.has(a)) forward.set(a, []);
                forward.get(a).push(b);
                if (!reverse.has(b)) reverse.set(b, []);
                reverse.get(b).push(a);
            }
            depGraphCache = { version, nodes, byKey, forward, reverse };
            onDone(depGraphCache, null);
        });
    }
    // Expands the graph from `rootIdx` into a cycle-safe tree.
    function buildDepTree(g, rootIdx, direction) {
        const adj = direction === "depends_on" ? g.forward : g.reverse;
        let budget = DEP_MAX_NODES;
        let seq = 0;
        const tableIdx = (name) => {
            const key = keyOf("table", "", "", name);
            if (!g.byKey.has(key)) { g.nodes.push({ key, type: "table", table: "", name }); g.byKey.set(key, g.nodes.length - 1); }
            return g.byKey.get(key);
        };
        const sortKey = (i) => `${g.nodes[i].type}|${g.nodes[i].table}|${g.nodes[i].name}`.toLowerCase();
        const build = (i, ancestors, depth) => {
            const item = { id: "d" + seq++, idx: i, children: [], cycle: ancestors.has(i) };
            if (item.cycle || depth >= DEP_MAX_DEPTH) return item;
            const next = new Set(ancestors).add(i);
            const refs = [...(adj.get(i) || [])];
            // A column ultimately depends on the table it belongs to.
            const n = g.nodes[i];
            if (direction === "depends_on" && n.type === "column" && n.table) {
                const t = tableIdx(n.table);
                if (!refs.includes(t) && !ancestors.has(t)) refs.push(t);
            }
            const seen = new Set();
            for (const j of refs.sort((a, b) => sortKey(a).localeCompare(sortKey(b)))) {
                if (seen.has(j) || budget <= 0) continue;
                seen.add(j);
                budget -= 1;
                item.children.push(build(j, next, depth + 1));
            }
            return item;
        };
        return build(rootIdx, new Set(), 0);
    }
    // Lays a dependency tree out as a left-to-right layered graph: nodes are
    // deduplicated and placed in the column of their shortest distance from
    // the root. Edges always point from the referencing object to the
    // referenced one.
    function layoutDepGraph(tree, direction) {
        const nodes = new Map();
        const edges = new Set();
        const visit = (t, level) => {
            const existing = nodes.get(t.idx);
            if (!existing) nodes.set(t.idx, { idx: t.idx, level, x: 0, y: 0 });
            else existing.level = Math.min(existing.level, level);
            for (const c of t.children) {
                edges.add(direction === "depends_on" ? `${t.idx}>${c.idx}` : `${c.idx}>${t.idx}`);
                visit(c, level + 1);
            }
        };
        visit(tree, 0);
        const byLevel = new Map();
        for (const n of nodes.values()) {
            if (!byLevel.has(n.level)) byLevel.set(n.level, []);
            byLevel.get(n.level).push(n);
        }
        const levels = [...byLevel.keys()].sort((a, b) => a - b);
        const maxRows = Math.max(1, ...[...byLevel.values()].map((a) => a.length));
        const height = maxRows * GRAPH.h + (maxRows - 1) * GRAPH.rowGap;
        for (const lvl of levels) {
            const col = byLevel.get(lvl);
            const colHeight = col.length * GRAPH.h + (col.length - 1) * GRAPH.rowGap;
            col.forEach((n, i) => {
                n.x = lvl * (GRAPH.w + GRAPH.colGap);
                n.y = (height - colHeight) / 2 + i * (GRAPH.h + GRAPH.rowGap);
            });
        }
        const maxLevel = levels.length ? levels[levels.length - 1] : 0;
        return {
            nodes: [...nodes.values()],
            edges: [...edges].map((e) => e.split(">").map(Number)),
            pos: nodes,
            width: maxLevel * (GRAPH.w + GRAPH.colGap) + GRAPH.w,
            height,
        };
    }
    function edgePath(a, b) {
        const ay = a.y + GRAPH.h / 2, by = b.y + GRAPH.h / 2, gap = 3;
        let x1, x2, c;
        if (b.x > a.x) { x1 = a.x + GRAPH.w; x2 = b.x - gap; c = [(x1 + x2) / 2, (x1 + x2) / 2]; }
        else if (b.x < a.x) { x1 = a.x; x2 = b.x + GRAPH.w + gap; c = [(x1 + x2) / 2, (x1 + x2) / 2]; }
        else {
            // Same column (e.g. a column and its home table): bulge out to the right.
            x1 = a.x + GRAPH.w; x2 = b.x + GRAPH.w + gap;
            const bulge = Math.max(x1, x2) + 56;
            c = [bulge, bulge];
        }
        return `M ${x1} ${ay} C ${c[0]} ${ay}, ${c[1]} ${by}, ${x2} ${by}`;
    }

    function openDependencies(node) {
        let rootKey = node.key;
        let direction = "depends_on";
        let view = "tree";
        let zoom = 1;
        let graph = null;
        const collapsed = new Set();
        const toolbar = h("div", "slls-sx-dep-toolbar");
        const m = modal(`Dependencies \u2014 ${objectLabel(node)}`, "workflow", { wide: true, tabs: toolbar });
        const titleEl = m.box.querySelector(".slls-sx-modal-title");
        const segmented = (options, current, onPick) => {
            const seg = h("div", "slls-sx-seg");
            for (const [value, label, icon] of options) {
                const b = button(value === current ? "on" : "", label, icon || "", () => onPick(value));
                b.setAttribute("aria-pressed", String(value === current));
                seg.appendChild(b);
            }
            return seg;
        };
        const rootIndex = () => {
            if (graph.byKey.has(rootKey)) return graph.byKey.get(rootKey);
            const known = getNode(rootKey) || node;
            graph.nodes.push({ key: rootKey, type: known.type, table: known.table || "", name: known.name });
            graph.byKey.set(rootKey, graph.nodes.length - 1);
            return graph.nodes.length - 1;
        };
        const renderToolbar = () => {
            clear(toolbar);
            toolbar.appendChild(segmented([["depends_on", "Depends on"], ["referenced_by", "Referenced by"]], direction,
                (v) => { direction = v; collapsed.clear(); draw(); }));
            toolbar.appendChild(h("span", "slls-sx-dep-hint",
                direction === "depends_on" ? "Objects this object depends on" : "Objects that depend on this object"));
            const rootNode = getNode(rootKey);
            if (rootNode && rootKey !== selectedKey) {
                toolbar.appendChild(button("slls-sx-btn small", "Show in model view", "", () => { m.close(); reveal(rootKey); }));
            }
            toolbar.appendChild(segmented([["tree", "Tree", "list_tree"], ["graph", "Node graph", "workflow"]], view,
                (v) => { view = v; draw(); }));
        };
        const emptyText = () => (direction === "depends_on" ? "This object has no dependencies." : "No objects depend on this object.");
        const focus = (idx) => {
            const n = graph.nodes[idx];
            rootKey = n.key || rootKey;
            if (!n.key) return;
            collapsed.clear();
            titleEl.textContent = `Dependencies \u2014 ${depLabel(n)}`;
            draw();
        };
        const drawTree = (tree) => {
            m.body.className = "slls-sx-modal-body";
            const walk = (t, depth) => {
                const n = graph.nodes[t.idx];
                const row = h("div", "slls-sx-dep-row");
                row.style.paddingLeft = depth * 18 + "px";
                const caret = h("span", "slls-sx-caret" + (collapsed.has(t.id) ? "" : " open"));
                if (t.children.length) {
                    caret.innerHTML = ICON.caret_right;
                    caret.style.cursor = "pointer";
                    caret.addEventListener("click", () => {
                        if (collapsed.has(t.id)) collapsed.delete(t.id); else collapsed.add(t.id);
                        draw();
                    });
                }
                row.appendChild(caret);
                const known = n.key ? getNode(n.key) : null;
                const icon = iconEl(known ? nodeIcon(known) : "folder");
                icon.style.color = `var(--sx-cat-${depCategory(n.type)})`;
                row.appendChild(icon);
                if (known && depth > 0) {
                    const a = h("a", "slls-sx-link", depLabel(n));
                    a.title = "Show in model view";
                    a.addEventListener("click", () => { m.close(); reveal(n.key); });
                    row.appendChild(a);
                } else row.appendChild(h("span", depth === 0 ? "slls-sx-dep-root" : "", depLabel(n)));
                row.appendChild(h("span", "slls-sx-pill", depTypeLabel(n)));
                if (t.cycle) row.appendChild(h("span", "meta", "\u21a9 circular"));
                m.body.appendChild(row);
                if (!collapsed.has(t.id)) for (const c of t.children) walk(c, depth + 1);
            };
            walk(tree, 0);
            if (!tree.children.length) m.body.appendChild(h("div", "slls-sx-empty", emptyText()));
        };
        const drawGraph = (tree) => {
            m.body.className = "slls-sx-modal-body slls-sx-graph-body";
            const layout = layoutDepGraph(tree, direction);
            const canvasW = layout.width + GRAPH.pad * 2 + 80;
            // With nothing to draw, the message sits under the root node (inside
            // the graph so it pans/zooms with it) rather than over it.
            const empty = !tree.children.length;
            const canvasH = layout.height + GRAPH.pad * 2 + (empty ? 36 : 0);
            const canvas = h("div", "slls-sx-graph");
            const sizer = h("div", "slls-sx-graph-sizer");
            const layer = h("div", "slls-sx-graph-layer");
            const applyZoom = () => {
                sizer.style.width = canvasW * zoom + "px";
                sizer.style.height = canvasH * zoom + "px";
                layer.style.transform = `scale(${zoom})`;
                zoomInput.value = String(Math.round(zoom * 100));
            };
            layer.style.width = canvasW + "px";
            layer.style.height = canvasH + "px";
            const inner = h("div", "slls-sx-graph-inner");
            inner.style.cssText = `left:${GRAPH.pad}px;top:${GRAPH.pad}px;width:${layout.width}px;height:${layout.height}px`;
            const svgNs = "http://www.w3.org/2000/svg";
            const svg = document.createElementNS(svgNs, "svg");
            svg.setAttribute("class", "slls-sx-graph-edges");
            svg.setAttribute("width", String(layout.width));
            svg.setAttribute("height", String(layout.height));
            const markerId = "slls-sx-arrow-" + Math.random().toString(36).slice(2, 8);
            svg.innerHTML = `<defs><marker id="${markerId}" viewBox="0 0 10 10" refX="9" refY="5" ` +
                'markerWidth="7" markerHeight="7" orient="auto-start-reverse">' +
                '<path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor"/></marker></defs>';
            for (const [from, to] of layout.edges) {
                const a = layout.pos.get(from), b = layout.pos.get(to);
                if (!a || !b) continue;
                const path = document.createElementNS(svgNs, "path");
                path.setAttribute("d", edgePath(a, b));
                path.setAttribute("fill", "none");
                path.setAttribute("stroke", "currentColor");
                path.setAttribute("stroke-width", "1.5");
                path.setAttribute("marker-end", `url(#${markerId})`);
                svg.appendChild(path);
            }
            inner.appendChild(svg);
            const rootIdx = tree.idx;
            for (const gn of layout.nodes) {
                const n = graph.nodes[gn.idx];
                const known = n.key ? getNode(n.key) : null;
                const b = h("button", "slls-sx-gnode" + (gn.idx === rootIdx ? " root" : ""));
                b.type = "button";
                b.style.cssText = `left:${gn.x}px;top:${gn.y}px;width:${GRAPH.w}px;height:${GRAPH.h}px;--cat:var(--sx-cat-${depCategory(n.type)})`;
                const top = h("div", "slls-sx-gnode-top");
                top.append(iconEl(known ? nodeIcon(known) : "folder", "slls-sx-gnode-icon"), h("span", "slls-sx-gnode-label", depGraphLabel(n)));
                b.append(top, h("span", "slls-sx-gnode-type", depTypeLabel(n)));
                b.title = `${depLabel(n)} \u00b7 ${depTypeLabel(n)}${n.key ? " \u2014 click to focus" : ""}`;
                b.addEventListener("click", () => { if (gn.idx !== rootIdx) focus(gn.idx); });
                inner.appendChild(b);
            }
            if (empty) {
                const note = h("div", "slls-sx-graph-empty", emptyText());
                note.style.top = GRAPH.h + 12 + "px";
                inner.appendChild(note);
            }
            layer.appendChild(inner);
            sizer.appendChild(layer);
            canvas.appendChild(sizer);
            // Drag the empty background to pan.
            canvas.addEventListener("pointerdown", (ev) => {
                if (ev.target.closest(".slls-sx-gnode")) return;
                ev.preventDefault();
                const ox = ev.clientX, oy = ev.clientY, sl = canvas.scrollLeft, st = canvas.scrollTop;
                canvas.classList.add("panning");
                const move = (e) => { canvas.scrollLeft = sl - (e.clientX - ox); canvas.scrollTop = st - (e.clientY - oy); };
                const up = () => {
                    canvas.classList.remove("panning");
                    window.removeEventListener("pointermove", move);
                    window.removeEventListener("pointerup", up);
                };
                window.addEventListener("pointermove", move);
                window.addEventListener("pointerup", up);
            });
            const controls = h("div", "slls-sx-zoom");
            const zoomTo = (z) => { zoom = Math.min(2.5, Math.max(0.3, Math.round(z * 100) / 100)); applyZoom(); };
            const zoomInput = h("input", "");
            zoomInput.type = "text";
            zoomInput.inputMode = "numeric";
            zoomInput.setAttribute("aria-label", "Zoom percentage");
            zoomInput.addEventListener("change", () => {
                const v = parseInt(zoomInput.value, 10);
                if (Number.isFinite(v)) zoomTo(v / 100); else applyZoom();
            });
            const pct = h("div", "slls-sx-zoom-pct");
            pct.append(zoomInput, h("span", "", "%"));
            controls.append(button("slls-sx-iconbtn", "", "zoom_in", () => zoomTo(zoom * 1.2), "Zoom in"), pct,
                button("slls-sx-iconbtn", "", "zoom_out", () => zoomTo(zoom / 1.2), "Zoom out"));
            m.body.append(canvas, controls);
            applyZoom();
        };
        const draw = () => {
            renderToolbar();
            clear(m.body);
            m.box.classList.toggle("graph", view === "graph");
            const tree = buildDepTree(graph, rootIndex(), direction);
            if (view === "graph") drawGraph(tree); else drawTree(tree);
        };
        renderToolbar();
        loadingInto(m.body, "Analyzing dependencies\u2026");
        loadDependencyGraph((g, error) => {
            if (!m.overlay.isConnected) return;
            if (error) { errorInto(m.body, error); return; }
            graph = { nodes: g.nodes.map((n) => Object.assign({}, n)), byKey: new Map(g.byKey), forward: g.forward, reverse: g.reverse };
            draw();
        });
    }
    // ------------------------------------------------------------------
    // Tools menu + Field parameter manager
    // ------------------------------------------------------------------
    const TOOLS = [
        { label: "Create .vpax file", icon: "file_box", open: () => openCreateVpax() },
        { label: "DAXLib package manager", icon: "package", open: () => openDaxLibManager() },
        { label: "Description manager", icon: "file", open: () => openDescriptionManager() },
        { label: "Field parameter manager", icon: "field_parameter", open: () => openFieldParameterManager() },
        { label: "Perspective editor", icon: "perspective", open: () => openPerspectiveEditor() },
        { label: "Query scale-out manager", icon: "server", open: () => openQueryScaleOutManager() },
        { label: "Relationship diagram", icon: "network", open: () => openRelationshipDiagram() },
        { label: "Relationship manager", icon: "git_branch", open: () => openRelationshipManager() },
        { label: "Security manager", icon: "shield_check", open: () => openSecurityManager() },
        { label: "Translation manager", icon: "languages", open: () => openTranslationManager() },
        { label: "Warm Direct Lake cache", icon: "flame", open: () => openWarmCache() },
    ];
    // Open tool windows redraw when the explorer re-renders (mode switch,
    // staged change, save, reload); while typing only their actions refresh.
    const openTools = new Set();
    function registerTool(m, redraw, actions) {
        const entry = {
            redraw: () => { if (m.overlay.isConnected) redraw(); },
            actions: () => { if (m.overlay.isConnected && actions) actions(); },
        };
        openTools.add(entry);
        const previous = m.onClose;
        m.onClose = () => {
            openTools.delete(entry);
            if (previous) previous();
            // Edits typed in the tool skipped the detail pane; catch it up.
            if (loaded() && treePane.isConnected) { renderTree(); renderDetail(); }
        };
    }
    function redrawTools() { for (const t of [...openTools]) t.redraw(); }
    function refreshToolActions() { for (const t of [...openTools]) t.actions(); }
    function toolModeSwitch() {
        const sw = h("button", "slls-sx-mode" + (mode === "write" ? " write" : ""));
        sw.type = "button";
        sw.setAttribute("role", "switch");
        sw.setAttribute("aria-checked", String(mode === "write"));
        sw.title = mode === "write" ? "Read-write mode (editing enabled)" : "Read-only mode \u2014 click to enable editing";
        sw.innerHTML = (mode === "write" ? ICON.pencil : ICON.eye) + '<span class="slls-sx-track"></span>';
        sw.appendChild(document.createTextNode(mode === "write" ? "Edit" : "Read-only"));
        sw.disabled = saving;
        sw.addEventListener("click", () => setMode(mode === "write" ? "read" : "write"));
        return sw;
    }
    const FP_HIERARCHY_URL = "https://www.youtube.com/watch?v=5G_xSJy5muo";
    const FP_NEW = "\u0000new";
    const fpEscape = (s) => String(s).replace(/"/g, '""');
    const fpQuoteTable = (t) => `'${String(t).replace(/'/g, "''")}'`;
    function fpCanonicalRef(f) {
        return f.type === "measure" ? `[${f.name}]` : `${fpQuoteTable(f.table)}[${f.name}]`;
    }
    function fpRef(f) { return f.rawRef || fpCanonicalRef(f); }
    // The field parameter's calculated-table DAX for an ordered list of fields.
    // When any field has a "hierarchy" (group) value, every row gets the 4th
    // element so the table constructor stays rectangular.
    function fpBuildExpression(fields) {
        const grouped = fields.some((f) => (f.hierarchy || "").trim() !== "");
        const rows = fields.map((f, i) => {
            const base = `    ( "${fpEscape(f.label)}", NAMEOF ( ${fpRef(f)} ), ${i}`;
            return grouped ? `${base}, "${fpEscape((f.hierarchy || "").trim())}" )` : `${base} )`;
        });
        return `{\n${rows.join(",\n")}\n}`;
    }
    // Effective (pending-aware) tables, columns and measures, keyed lower-case.
    function fpModelIndex() {
        const tables = new Map();
        const measureOwner = new Map();
        for (const t of data.tables || []) {
            if (isDeleted(t)) continue;
            const name = effName(t);
            const entry = { name, kind: t.kind, columns: new Map(), measures: new Map() };
            for (const c of [...(t.columns || []), ...newNodes("column", t.key)]) {
                if (!isDeleted(c)) entry.columns.set(effName(c).toLowerCase(), { name: effName(c), calculated: c.column_type === "Calculated" });
            }
            for (const m of [...(t.measures || []), ...newNodes("measure", t.key)]) {
                if (isDeleted(m)) continue;
                entry.measures.set(effName(m).toLowerCase(), { name: effName(m) });
                measureOwner.set(effName(m).toLowerCase(), name);
            }
            tables.set(name.toLowerCase(), entry);
        }
        return { tables, measureOwner };
    }
    function fpField(type, table, name, label, calculated, originalRef) {
        const f = { key: `${type}|${table}|${name}`, type, table, name, label, calculated: !!calculated };
        // Keep the author's reference text verbatim unless it is canonical.
        if (originalRef && originalRef.trim() !== fpCanonicalRef(f)) f.rawRef = originalRef.trim();
        return f;
    }
    // Resolves a NAMEOF reference to a column or measure. Power BI writes both
    // as 'Table'[Name], so the model decides which it is.
    function fpResolve(ref, label, idx, index) {
        const trimmed = ref.trim();
        const qual = trimmed.match(/^(?:'((?:[^']|'')+)'|([^\s'[\]]+))\s*\[([^\]]+)\]$/);
        const unq = trimmed.match(/^\[([^\]]+)\]$/);
        let table = "", name = "";
        if (qual) { table = (qual[1] || qual[2] || "").replace(/''/g, "'"); name = qual[3]; }
        else if (unq) name = unq[1];
        else return { key: `raw|${index}|${trimmed}`, type: "column", table: "", name: trimmed, label, calculated: false, broken: true, rawRef: ref };
        const t = table ? idx.tables.get(table.toLowerCase()) : null;
        if (t && t.columns.has(name.toLowerCase())) {
            const c = t.columns.get(name.toLowerCase());
            return fpField("column", t.name, c.name, label, c.calculated, ref);
        }
        const owner = idx.measureOwner.get(name.toLowerCase());
        if (owner) return fpField("measure", owner, idx.tables.get(owner.toLowerCase()).measures.get(name.toLowerCase()).name, label, false, ref);
        if (!table) {
            for (const tt of idx.tables.values()) {
                if (tt.columns.has(name.toLowerCase())) {
                    const c = tt.columns.get(name.toLowerCase());
                    return fpField("column", tt.name, c.name, label, c.calculated, ref);
                }
            }
        }
        return { key: table ? `column|${table}|${name}` : `raw|${index}|${trimmed}`, type: "column", table, name: table ? name : trimmed,
            label, calculated: false, broken: true, rawRef: ref };
    }
    // Rows look like ( "label", NAMEOF ( ref ), ordinal [, "hierarchy"] ).
    function fpParse(expression, idx) {
        const rowRe = /\(\s*"((?:[^"]|"")*)"\s*,\s*NAMEOF\s*\(\s*([^)]*?)\s*\)\s*,\s*(-?\d+)(?:\s*,?\s*"((?:[^"]|"")*)")?/g;
        const rows = [];
        let mt;
        while ((mt = rowRe.exec(expression || "")) !== null) {
            rows.push({ label: mt[1].replace(/""/g, '"'), ref: mt[2].trim(), ordinal: parseInt(mt[3], 10),
                hierarchy: mt[4] !== undefined ? mt[4].replace(/""/g, '"') : undefined });
        }
        rows.sort((a, b) => a.ordinal - b.ordinal);
        const seen = new Set();
        const out = [];
        rows.forEach((r, i) => {
            const f = fpResolve(r.ref, r.label, idx, i);
            if (seen.has(f.key)) return;
            seen.add(f.key);
            if (r.hierarchy !== undefined) f.hierarchy = r.hierarchy;
            out.push(f);
        });
        return out;
    }
    function fpList() {
        const out = [];
        for (const t of data.tables || []) {
            if (t.kind !== "field_parameter" || isDeleted(t)) continue;
            const partition = (t.partitions || []).find((p) => p.source_type === "Calculated") || (t.partitions || [])[0];
            out.push({ id: t.key, name: effName(t), partition, isNew: false });
        }
        for (const c of staging.creates) {
            if (c.type === "fieldParameter") out.push({ id: "new:" + c.id, name: c.name, create: c, isNew: true });
        }
        return out.sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: "base" }));
    }
    function fpExpression(fp) {
        if (fp.isNew) return fp.create.props.Expression || "";
        return fp.partition ? String(value(fp.partition, "Expression") || "") : "";
    }
    function fpTakenNames() {
        const names = [...(data.tables || []).map(effName)];
        for (const c of staging.creates) if (c.type === "table" || c.type === "fieldParameter") names.push(c.name);
        return names;
    }
    function fpFieldIcon(f) { return f.type === "measure" ? "measure" : (f.calculated ? "calculated_column" : "column"); }

    function openFieldParameterManager() {
        let active = null;
        let draftName = "";
        let fields = [];
        let baseline = [];
        let search = "";
        let status = null;
        const openTables = new Set();
        const m = modal("Field parameter manager", "field_parameter", { wide: true });
        m.box.classList.add("slls-sx-fpm-box");
        m.body.className = "slls-sx-modal-body slls-sx-fpm-body";
        registerTool(m, () => draw(true));
        const sig = (f) => [f.type, f.table, f.name, f.label, f.rawRef || "", f.hierarchy || ""].join("\u0001");
        const sameFields = (a, b) => a.length === b.length && a.every((f, i) => sig(f) === sig(b[i]));
        const clone = (list) => list.map((f) => Object.assign({}, f));
        const writable = () => mode === "write" && !saving;
        const isNewDraft = () => active === FP_NEW;
        const nameTaken = () => {
            const n = draftName.trim().toLowerCase();
            return isNewDraft() && !!n && fpTakenNames().some((x) => x.toLowerCase() === n);
        };
        const dirty = () => (isNewDraft() ? fields.length > 0 || !!draftName.trim() : active !== null && !sameFields(fields, baseline));
        const hierarchyState = () => {
            const some = fields.some((f) => (f.hierarchy || "").trim() !== "");
            const all = fields.every((f) => (f.hierarchy || "").trim() !== "");
            return { some, incomplete: some && !all };
        };
        const canStage = () => writable() && fields.length > 0 && fields.every((f) => f.label.trim()) && !hierarchyState().incomplete
            && (isNewDraft() ? !!draftName.trim() && !nameTaken() : dirty());
        const load = (fp) => {
            fields = fpParse(fpExpression(fp), fpModelIndex());
            baseline = clone(fields);
            draftName = fp.name;
        };
        const select = (id) => {
            const fp = fpList().find((x) => x.id === id);
            if (!fp) return;
            active = id; status = null; search = "";
            load(fp);
            draw();
        };
        const startNew = () => {
            active = FP_NEW; status = null; search = "";
            draftName = uniqueName("Parameter", fpTakenNames());
            fields = []; baseline = [];
            draw();
        };
        const stage = () => {
            if (!canStage()) return;
            const expression = fpBuildExpression(fields);
            if (isNewDraft()) {
                pushUndo("");
                const id = ++createSeq + "-" + Date.now().toString(36);
                staging.creates.push({ id, type: "fieldParameter", parentKey: "", name: draftName.trim(), props: { Expression: expression } });
                active = "new:" + id;
                status = { kind: "success", text: `Staged new field parameter \u201c${draftName.trim()}\u201d. Use Save to apply it to the model.` };
                afterChange();
            } else {
                const fp = fpList().find((x) => x.id === active);
                if (!fp) return;
                if (fp.isNew) { pushUndo(""); fp.create.props.Expression = expression; afterChange(); }
                else setValue(fp.partition, "Expression", expression, { label: "Expression" });
                status = { kind: "success", text: `Staged changes to \u201c${fp.name}\u201d. Use Save to apply them to the model.` };
            }
            baseline = clone(fields);
            draw();
        };

        let stageBtn = null;
        let hierarchyNote = null;
        const refreshControls = () => {
            if (stageBtn) stageBtn.disabled = !canStage();
            const hs = hierarchyState();
            if (hierarchyNote) {
                hierarchyNote.className = "slls-sx-fpm-note" + (hs.incomplete ? " error" : "");
                hierarchyNote.textContent = hs.incomplete
                    ? "Set a Hierarchy value for every object, or clear them all \u2014 it's all-or-nothing."
                    : "Give objects the same Hierarchy name to group them into a (makeshift) hierarchy in Power BI. Leave blank for none.";
            }
            for (const input of m.body.querySelectorAll(".slls-sx-fpm-hier")) {
                input.classList.toggle("invalid", hs.incomplete && !input.value.trim());
            }
        };

        function drawSidebar(list) {
            const side = h("div", "slls-sx-fpm-side");
            const head = h("div", "slls-sx-fpm-side-head");
            head.appendChild(h("span", "slls-sx-pane-label", `Field parameters (${list.length})`));
            if (writable()) {
                const add = button("slls-sx-iconbtn" + (isNewDraft() ? " on" : ""), "", "plus", startNew, "Create a new field parameter");
                head.appendChild(add);
            }
            side.appendChild(head);
            const items = h("div", "slls-sx-fpm-list");
            if (isNewDraft()) {
                const b = button("slls-sx-fpm-item draft active", "", "plus", () => {});
                b.append(h("span", "name", draftName.trim() || "New field parameter"), h("span", "count", String(fields.length)));
                items.appendChild(b);
            }
            if (!list.length && !isNewDraft()) {
                items.appendChild(h("div", "slls-sx-empty", "This model has no field parameters." + (writable() ? " Use + to create one." : "")));
            }
            for (const fp of list) {
                const b = button("slls-sx-fpm-item" + (active === fp.id ? " active" : "") + (fp.isNew ? " new" : ""), "", "field_parameter", () => select(fp.id));
                const count = fpParse(fpExpression(fp), fpModelIndex()).length;
                b.append(h("span", "name", fp.name), h("span", "count", String(count)));
                b.title = fp.isNew ? `${fp.name} (new \u2014 not saved yet)` : fp.name;
                items.appendChild(b);
            }
            side.appendChild(items);
            return side;
        }
        function drawAvailable() {
            const pane = h("div", "slls-sx-fpm-pane");
            pane.appendChild(h("div", "slls-sx-pane-label", "Add columns & measures"));
            const sw = h("div", "slls-sx-search");
            sw.appendChild(iconEl("search", ""));
            const input = h("input", "slls-sx-input");
            input.type = "search";
            input.placeholder = "Search columns and measures\u2026";
            input.value = search;
            sw.appendChild(input);
            pane.appendChild(sw);
            const list = h("div", "slls-sx-fpm-scroll");
            pane.appendChild(list);
            const paint = () => {
                clear(list);
                const q = search.trim().toLowerCase();
                const chosen = new Set(fields.map((f) => f.key));
                let any = false;
                for (const t of [...(data.tables || [])].sort(byLabel)) {
                    if (isDeleted(t) || t.kind === "calculation_group" || t.kind === "field_parameter") continue;
                    const tname = effName(t);
                    const byName = (a, b) => effName(a).localeCompare(effName(b), undefined, { sensitivity: "base" });
                    let items = [
                        ...[...(t.measures || []), ...newNodes("measure", t.key)].filter((x) => !isDeleted(x)).sort(byName)
                            .map((x) => ({ type: "measure", name: effName(x), calculated: false })),
                        ...[...(t.columns || []), ...newNodes("column", t.key)].filter((x) => !isDeleted(x)).sort(byName)
                            .map((x) => ({ type: "column", name: effName(x), calculated: x.column_type === "Calculated" })),
                    ];
                    if (q && !tname.toLowerCase().includes(q)) items = items.filter((x) => x.name.toLowerCase().includes(q));
                    if (!items.length) continue;
                    any = true;
                    const isOpen = !!q || openTables.has(t.key);
                    const head = h("button", "slls-sx-fpm-row table");
                    head.type = "button";
                    const caret = h("span", "slls-sx-caret" + (isOpen ? " open" : ""));
                    caret.innerHTML = ICON.caret_right;
                    head.append(caret, iconEl(tableIcon(t), ""), h("span", "name", tname), h("span", "meta", String(items.length)));
                    head.addEventListener("click", () => { if (openTables.has(t.key)) openTables.delete(t.key); else openTables.add(t.key); paint(); });
                    list.appendChild(head);
                    if (!isOpen) continue;
                    for (const it of items) {
                        const f = fpField(it.type, tname, it.name, it.name, it.calculated);
                        const added = chosen.has(f.key);
                        const row = h("button", "slls-sx-fpm-row item" + (added ? " added" : ""));
                        row.type = "button";
                        row.disabled = added;
                        row.append(iconEl(fpFieldIcon(f), ""), h("span", "name", it.name));
                        if (added) row.appendChild(h("span", "meta", "Added"));
                        else row.appendChild(iconEl("plus", "slls-sx-ricon add"));
                        row.addEventListener("click", () => { fields.push(f); draw(); });
                        list.appendChild(row);
                    }
                }
                if (!any) list.appendChild(h("div", "slls-sx-empty", "No matching columns or measures."));
            };
            input.addEventListener("input", () => { search = input.value; paint(); });
            paint();
            return pane;
        }
        function drawObjects() {
            const pane = h("div", "slls-sx-fpm-pane");
            const head = h("div", "slls-sx-fpm-pane-head");
            head.appendChild(h("span", "slls-sx-pane-label", `Objects (${fields.length})`));
            const link = h("a", "slls-sx-link slls-sx-fpm-link");
            link.href = FP_HIERARCHY_URL;
            link.target = "_blank";
            link.rel = "noreferrer";
            link.title = "Watch a video about hierarchies in field parameters";
            link.append(document.createTextNode("About hierarchies"), iconEl("external_link", ""));
            head.appendChild(link);
            pane.appendChild(head);
            hierarchyNote = fields.length ? h("div", "slls-sx-fpm-note") : null;
            if (hierarchyNote) pane.appendChild(hierarchyNote);
            const list = h("div", "slls-sx-fpm-scroll");
            if (!fields.length) list.appendChild(h("div", "slls-sx-empty", "Add fields from the left. The order here is the order shown in the field parameter."));
            fields.forEach((f, i) => {
                const row = h("div", "slls-sx-fpm-obj");
                row.appendChild(h("span", "idx", String(i)));
                row.appendChild(iconEl(fpFieldIcon(f), ""));
                const main = h("div", "main");
                const label = h("input", "slls-sx-fpm-label");
                label.value = f.label;
                label.placeholder = "Label";
                label.setAttribute("aria-label", `Label for ${f.name}`);
                label.addEventListener("input", () => { f.label = label.value; refreshControls(); });
                const ref = h("span", "ref" + (f.broken ? " broken" : ""), fpRef(f) + (f.broken ? " \u00b7 missing" : ""));
                ref.title = f.broken ? `${fpRef(f)} \u2014 object not found in the model` : fpRef(f);
                main.append(label, ref);
                row.appendChild(main);
                const hier = h("input", "slls-sx-input slls-sx-fpm-hier");
                hier.value = f.hierarchy || "";
                hier.placeholder = "Hierarchy";
                hier.title = "Optional hierarchy / group name. All-or-nothing: set it for every object or leave them all blank.";
                hier.setAttribute("aria-label", `Hierarchy for ${f.name}`);
                hier.addEventListener("input", () => { f.hierarchy = hier.value; refreshControls(); });
                row.appendChild(hier);
                const up = button("slls-sx-iconbtn", "", "arrow_up", () => { [fields[i - 1], fields[i]] = [fields[i], fields[i - 1]]; draw(); }, "Move up");
                up.disabled = i === 0;
                const down = button("slls-sx-iconbtn", "", "arrow_down", () => { [fields[i + 1], fields[i]] = [fields[i], fields[i + 1]]; draw(); }, "Move down");
                down.disabled = i === fields.length - 1;
                const remove = button("slls-sx-iconbtn", "", "close", () => { fields.splice(i, 1); draw(); }, "Remove");
                row.append(up, down, remove);
                list.appendChild(row);
            });
            pane.appendChild(list);
            return pane;
        }
        function drawReadOnly() {
            const wrap = h("div", "slls-sx-fpm-readonly");
            const info = h("div", "slls-sx-fpm-info");
            info.append(iconEl("info", ""), h("span", "", "Read-only. Switch to edit mode to reorder, change objects or create new field parameters."));
            wrap.appendChild(info);
            if (!fields.length) wrap.appendChild(h("div", "slls-sx-empty", "This field parameter has no objects."));
            fields.forEach((f, i) => {
                const row = h("div", "slls-sx-fpm-obj readonly");
                row.append(h("span", "idx", String(i)), iconEl(fpFieldIcon(f), ""), h("span", "label", f.label));
                if (f.hierarchy) { const p = h("span", "slls-sx-pill", f.hierarchy); p.title = "Hierarchy / group"; row.appendChild(p); }
                const ref = h("span", "ref" + (f.broken ? " broken" : ""), fpRef(f));
                ref.title = fpRef(f);
                row.appendChild(ref);
                if (f.broken) { const w = iconEl("alert", "slls-sx-err"); w.title = "Object not found in the model"; row.appendChild(w); }
                wrap.appendChild(row);
            });
            return wrap;
        }
        function drawMain() {
            const main = h("div", "slls-sx-fpm-main");
            if (active === null) {
                const empty = h("div", "slls-sx-center");
                empty.append(iconEl("field_parameter", ""), h("div", "", "Select a field parameter to view its objects" + (writable() ? ", or create a new one." : ".")));
                if (!writable() && !saving) {
                    const link = h("a", "slls-sx-link", "Enable edit mode to reorder, change objects or create new field parameters");
                    link.addEventListener("click", () => setMode("write"));
                    empty.appendChild(link);
                }
                main.appendChild(empty);
                return main;
            }
            const bar = h("div", "slls-sx-fpm-toolbar");
            bar.appendChild(iconEl("field_parameter", "slls-sx-fpm-title-icon"));
            if (isNewDraft() && writable()) {
                const name = h("input", "slls-sx-input slls-sx-fpm-name" + (nameTaken() ? " invalid" : ""));
                name.value = draftName;
                name.placeholder = "Field parameter name";
                name.setAttribute("aria-label", "Field parameter name");
                name.addEventListener("input", () => {
                    draftName = name.value;
                    name.classList.toggle("invalid", nameTaken());
                    taken.style.display = nameTaken() ? "" : "none";
                    const item = m.body.querySelector(".slls-sx-fpm-item.draft .name");
                    if (item) item.textContent = draftName.trim() || "New field parameter";
                    refreshControls();
                });
                bar.appendChild(name);
            } else bar.appendChild(h("span", "slls-sx-fpm-name-text", draftName));
            const taken = h("span", "slls-sx-fpm-status error", "Name already in use");
            taken.style.display = nameTaken() ? "" : "none";
            bar.appendChild(taken);
            if (status) bar.appendChild(h("span", "slls-sx-fpm-status " + status.kind, status.text));
            if (writable()) {
                stageBtn = button("slls-sx-btn primary slls-sx-fpm-stage", isNewDraft() ? "Stage field parameter" : "Stage changes", "", stage);
                bar.appendChild(stageBtn);
            } else stageBtn = null;
            main.appendChild(bar);
            if (writable()) {
                const grid = h("div", "slls-sx-fpm-grid");
                grid.append(drawAvailable(), drawObjects());
                main.appendChild(grid);
            } else {
                hierarchyNote = null;
                main.appendChild(drawReadOnly());
            }
            return main;
        }
        function draw(external) {
            const list = fpList();
            if (active !== null && active !== FP_NEW && !list.some((x) => x.id === active)) {
                // A staged field parameter that was just saved is now a real table.
                const match = list.find((x) => x.name === draftName);
                active = match ? match.id : null;
                if (match && !dirty()) load(match);
            } else if (external && active !== null && active !== FP_NEW && !dirty()) {
                const fp = list.find((x) => x.id === active);
                if (fp) load(fp);
            }
            if (active === FP_NEW && !writable()) active = null;
            clear(m.actions);
            m.actions.appendChild(toolModeSwitch());
            clear(m.body);
            const layout = h("div", "slls-sx-fpm");
            layout.append(drawSidebar(list), drawMain());
            m.body.appendChild(layout);
            refreshControls();
        }
        draw();
    }

    // ------------------------------------------------------------------
    // DAXLib package manager
    // ------------------------------------------------------------------
    const DAXLIB_MIN_COMPAT = 1702;
    let dlCatalog = null;
    // Semver-ish comparison: numeric parts, then a release outranks a prerelease.
    function dlCompare(a, b) {
        const split = (v) => { const s = String(v || ""); const i = s.indexOf("-"); return i < 0 ? [s, ""] : [s.slice(0, i), s.slice(i + 1)]; };
        const [ca, pa] = split(a);
        const [cb, pb] = split(b);
        const na = ca.split(".").map((x) => parseInt(x, 10) || 0);
        const nb = cb.split(".").map((x) => parseInt(x, 10) || 0);
        for (let i = 0; i < Math.max(na.length, nb.length); i += 1) {
            const d = (na[i] || 0) - (nb[i] || 0);
            if (d) return d > 0 ? 1 : -1;
        }
        if (pa === pb) return 0;
        if (!pa) return 1;
        if (!pb) return -1;
        const xa = pa.split(".");
        const xb = pb.split(".");
        for (let i = 0; i < Math.max(xa.length, xb.length); i += 1) {
            if (xa[i] === undefined) return -1;
            if (xb[i] === undefined) return 1;
            const ia = /^\d+$/.test(xa[i]);
            const ib = /^\d+$/.test(xb[i]);
            if (ia && ib) { const d = Number(xa[i]) - Number(xb[i]); if (d) return d > 0 ? 1 : -1; }
            else if (ia !== ib) return ia ? -1 : 1;
            else if (xa[i] !== xb[i]) return xa[i] < xb[i] ? -1 : 1;
        }
        return 0;
    }
    function dlDownloads(n) {
        if (n >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/, "") + "M";
        if (n >= 1e3) return (n / 1e3).toFixed(1).replace(/\.0$/, "") + "K";
        return String(n || 0);
    }
    function dlDate(iso) {
        const d = iso ? new Date(iso) : null;
        return d && !Number.isNaN(d.getTime()) ? d.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" }) : "";
    }
    // Packages installed in the saved model, from the functions' annotations:
    // DAXLIB_PackageId / DAXLIB_PackageVersion, or sempy_labs' annotation named
    // after the package (holding the version).
    function dlInstalled() {
        const out = new Map();
        for (const f of data.functions || []) {
            const ann = f.annotations || {};
            const keys = Object.keys(ann);
            const find = (name) => keys.find((k) => k.toLowerCase() === name.toLowerCase());
            let id = "";
            let version = "";
            const idKey = find("DAXLIB_PackageId");
            if (idKey) {
                id = String(ann[idKey]);
                const vKey = find("DAXLIB_PackageVersion");
                version = vKey ? String(ann[vKey]) : "";
            } else {
                const k = keys.find((x) => f.name.toLowerCase().startsWith(x.toLowerCase() + "."));
                if (k) { id = k; version = String(ann[k]); }
            }
            if (!id) continue;
            const e = out.get(id.toLowerCase()) || { packageId: id, version, functionCount: 0, keys: [] };
            e.functionCount += 1;
            e.keys.push(f.key);
            if (!e.version) e.version = version;
            out.set(id.toLowerCase(), e);
        }
        return out;
    }
    function dlStaged() { return staging.creates.filter((c) => c.type === "daxlibPackage"); }
    function dlRows() {
        const installed = dlInstalled();
        const staged = new Map(dlStaged().map((c) => [c.name.toLowerCase(), c]));
        const rows = [];
        for (const [k, p] of installed) {
            const s = staged.get(k);
            rows.push({ packageId: p.packageId, version: s ? s.props.version : p.version,
                functionCount: s ? s.props.functions.length : p.functionCount, installed: true, staged: !!s });
        }
        for (const [k, s] of staged) {
            if (!installed.has(k)) rows.push({ packageId: s.name, version: s.props.version, functionCount: s.props.functions.length, installed: false, staged: true });
        }
        return rows.sort((a, b) => a.packageId.localeCompare(b.packageId, undefined, { sensitivity: "base" }));
    }
    // Stages a package version: written on Save, replacing the package's
    // existing functions. Pending edits / deletes of those functions would be
    // overwritten, so they are dropped.
    function dlStage(pkg, upgrade) {
        if (saving || mode !== "write") return;
        pushUndo("");
        const lower = pkg.packageId.toLowerCase();
        staging.creates = staging.creates.filter((c) => !(c.type === "daxlibPackage" && c.name.toLowerCase() === lower));
        const inst = dlInstalled().get(lower);
        const names = new Set(pkg.functions.map((f) => f.name.toLowerCase()));
        const keys = new Set(inst ? inst.keys : []);
        for (const f of data.functions || []) if (names.has(f.name.toLowerCase())) keys.add(f.key);
        for (const [id, e] of Object.entries(staging.edits)) if (keys.has(e.key)) delete staging.edits[id];
        staging.ops = staging.ops.filter((o) => !keys.has(o.key));
        const id = ++createSeq + "-" + Date.now().toString(36);
        staging.creates.push({ id, type: "daxlibPackage", parentKey: "", name: pkg.packageId, props: {
            version: pkg.version, functions: pkg.functions.map((f) => ({ name: f.name, definition: f.definition })),
            upgradeCompatibility: !!upgrade, installed: !!inst } });
        recomputeCascade();
        afterChange();
    }

    function openDaxLibManager() {
        let tab = null;
        let search = "";
        let tag = "";
        let pkg = null;
        let loading = false;
        let loadError = null;
        let catalogError = null;
        let status = null;
        let updatingId = null;
        let seq = 0;
        const openFns = new Set();
        const m = modal("DAXLib package manager", "package", { wide: true });
        m.box.classList.add("slls-sx-fpm-box");
        m.body.className = "slls-sx-modal-body slls-sx-dlm-body";
        registerTool(m, () => draw());
        const writable = () => mode === "write" && !saving;
        const catalogById = () => new Map((dlCatalog || []).map((p) => [p.id.toLowerCase(), p]));
        const hasUpdate = (row, byId) => {
            const latest = byId.get(row.packageId.toLowerCase());
            return !row.staged && !!latest && dlCompare(latest.version, row.version) > 0;
        };

        const fetchPackage = (id, version, onDone) => {
            const mine = ++seq;
            request("daxlib_package", { package: id, version: version || "" }, (res) => {
                if (mine !== seq || !m.overlay.isConnected) return;
                onDone(res);
            });
        };
        const load = (id, version) => {
            if (loading && !version) return;
            loading = true; loadError = null; status = null;
            draw();
            fetchPackage(id, version, (res) => {
                loading = false;
                if (res.ok) {
                    if (!pkg || pkg.packageId !== res.data.packageId) openFns.clear();
                    pkg = res.data;
                } else { pkg = null; loadError = res.error || "The package could not be loaded."; }
                draw();
            });
        };
        const commit = (p, upgrade) => {
            const existed = dlRows().some((r) => r.packageId.toLowerCase() === p.packageId.toLowerCase() && r.installed);
            dlStage(p, upgrade);
            status = { kind: "success", text: `Staged ${existed ? "an update of " : ""}\u201c${p.packageId}\u201d v${p.version} `
                + `(${plural(p.functions.length, "function")}). Use Save to apply it to the model.` };
            pkg = null;
            tab = "installed";
            draw();
        };
        // DAX user-defined functions need compatibility level 1702: confirm the
        // upgrade first (it is staged with the package and written on Save).
        const stage = (p) => {
            if (!writable() || !p.functions.length) return;
            const level = Number(data.compatibility_level || 0);
            if (level >= DAXLIB_MIN_COMPAT) { commit(p, false); return; }
            const c = modal("Compatibility level upgrade required", "alert");
            c.body.appendChild(h("p", "slls-sx-dlm-text",
                `DAX user-defined functions require the model to be at compatibility level ${DAXLIB_MIN_COMPAT} or higher. `
                + `This model is at compatibility level ${level || "unknown"}.`));
            c.body.appendChild(h("p", "slls-sx-dlm-note",
                `Staging this package also upgrades the model to compatibility level ${DAXLIB_MIN_COMPAT} when you Save. `
                + "Compatibility levels can only be increased, not decreased."));
            const foot = h("div", "slls-sx-modal-foot");
            const ok = button("slls-sx-btn primary", `Upgrade to ${DAXLIB_MIN_COMPAT} & stage`, "", () => { c.close(); commit(p, true); });
            foot.append(button("slls-sx-btn", "Cancel", "", () => c.close()), ok);
            c.box.appendChild(foot);
            setTimeout(() => ok.focus(), 0);
        };
        const update = (id) => {
            updatingId = id;
            draw();
            fetchPackage(id, "", (res) => {
                updatingId = null;
                if (res.ok) stage(res.data);
                else status = { kind: "error", text: res.error || "The package could not be loaded." };
                draw();
            });
        };

        let list = null;
        function packageIcon(p) {
            const wrap = h("span", "slls-sx-dlm-icon");
            wrap.innerHTML = ICON.package;
            if (p && p.hasIcon && p.version) {
                const img = document.createElement("img");
                img.alt = "";
                img.src = `https://api.daxlib.org/v1/package/${encodeURIComponent(p.id)}/${encodeURIComponent(p.version)}/icon`;
                img.addEventListener("load", () => { clear(wrap); wrap.appendChild(img); });
            }
            return wrap;
        }
        function drawInstalled(rows, byId) {
            const q = search.trim().toLowerCase();
            const shown = q ? rows.filter((r) => r.packageId.toLowerCase().includes(q)) : rows;
            if (!shown.length) {
                const empty = h("div", "slls-sx-empty");
                if (q) empty.textContent = "No installed packages match your search.";
                else {
                    empty.append(document.createTextNode("No DAXLib packages are installed in this model yet. Use "));
                    const link = h("a", "slls-sx-link", "Browse");
                    link.addEventListener("click", () => { tab = "browse"; draw(); });
                    empty.append(link, document.createTextNode(" to add one."));
                }
                return empty;
            }
            const ul = h("div", "slls-sx-dlm-list");
            for (const r of shown) {
                const row = h("div", "slls-sx-dlm-row installed");
                const open = h("button", "slls-sx-dlm-open");
                open.type = "button";
                open.title = "View this package";
                open.disabled = loading;
                open.addEventListener("click", () => load(r.packageId));
                open.append(packageIcon(byId.get(r.packageId.toLowerCase())), h("span", "slls-sx-dlm-id", r.packageId));
                if (r.staged) {
                    const p = h("span", "slls-sx-pill warn", "Staged");
                    p.title = r.installed ? "Staged update \u2014 written on Save" : "Staged add \u2014 written on Save";
                    open.appendChild(p);
                }
                open.append(h("span", "slls-sx-dlm-ver", "v" + (r.version || "?")), h("span", "slls-sx-dlm-count", plural(r.functionCount, "function")));
                row.appendChild(open);
                const latest = byId.get(r.packageId.toLowerCase());
                if (hasUpdate(r, byId)) {
                    if (writable()) {
                        const busy = updatingId === r.packageId;
                        const b = button("slls-sx-btn small primary", `Update to v${latest.version}`, "refresh", () => update(r.packageId),
                            `Update to v${latest.version} (staged, written on Save)`);
                        if (busy) b.querySelector("svg").classList.add("slls-sx-spin");
                        b.disabled = busy || updatingId !== null;
                        row.appendChild(b);
                    } else {
                        const p = h("span", "slls-sx-pill warn", `v${latest.version} available`);
                        p.title = "Switch to edit mode to update";
                        row.appendChild(p);
                    }
                } else if (!r.staged && latest) {
                    const ok = h("span", "slls-sx-dlm-ok");
                    ok.append(iconEl("check_circle", ""), document.createTextNode("Up to date"));
                    row.appendChild(ok);
                }
                ul.appendChild(row);
            }
            return ul;
        }
        function drawBrowse(installed) {
            if (catalogError) {
                const err = h("div", "slls-sx-dlm-error");
                err.append(iconEl("alert", ""), h("div", "", catalogError));
                return err;
            }
            if (!dlCatalog) {
                const c = h("div", "slls-sx-center");
                c.style.padding = "40px 0";
                const sp = iconEl("refresh", "");
                sp.querySelector("svg").classList.add("slls-sx-spin");
                c.append(sp, h("div", "", "Loading packages\u2026"));
                return c;
            }
            const q = search.trim().toLowerCase();
            const shown = dlCatalog.filter((p) => (!q || p.id.toLowerCase().includes(q) || p.description.toLowerCase().includes(q))
                && (!tag || p.tags.includes(tag)));
            if (!shown.length) return h("div", "slls-sx-empty", "No packages match your search.");
            const ul = h("div", "slls-sx-dlm-list");
            for (const p of shown) {
                const row = h("div", "slls-sx-dlm-row");
                const open = h("button", "slls-sx-dlm-open");
                open.type = "button";
                open.disabled = loading;
                open.addEventListener("click", () => load(p.id));
                const main = h("span", "slls-sx-dlm-main");
                const title = h("span", "slls-sx-dlm-title");
                title.appendChild(h("span", "slls-sx-dlm-id", p.id));
                if (installed.has(p.id.toLowerCase())) title.appendChild(h("span", "slls-sx-pill new", "Installed"));
                main.appendChild(title);
                if (p.description) { const d = h("span", "slls-sx-dlm-desc", p.description); d.title = p.description; main.appendChild(d); }
                const meta = h("span", "slls-sx-dlm-meta");
                if (p.authors.length) meta.appendChild(h("span", "", "by " + p.authors.join(", ")));
                const when = dlDate(p.published);
                if (when) {
                    const s = h("span", "");
                    s.title = "Last updated";
                    s.append(iconEl("calendar", ""), document.createTextNode("Updated " + when));
                    meta.appendChild(s);
                }
                if (meta.childNodes.length) main.appendChild(meta);
                if (p.tags.length) {
                    const tags = h("span", "slls-sx-dlm-tags");
                    for (const t of p.tags.slice(0, 6)) tags.appendChild(h("span", "slls-sx-dlm-tag", t));
                    main.appendChild(tags);
                }
                open.append(packageIcon(p), main);
                const side = h("div", "slls-sx-dlm-side");
                side.appendChild(h("span", "slls-sx-dlm-ver", "v" + p.version));
                const dl = h("span", "slls-sx-dlm-dl");
                dl.title = `${Number(p.downloads).toLocaleString()} downloads`;
                dl.append(iconEl("download", ""), document.createTextNode(dlDownloads(p.downloads)));
                side.appendChild(dl);
                if (p.owners.length) {
                    const owners = h("span", "slls-sx-dlm-owners");
                    for (const o of p.owners) {
                        const a = h("a", "");
                        a.href = `https://github.com/${encodeURIComponent(o)}`;
                        a.target = "_blank";
                        a.rel = "noreferrer";
                        a.title = `${o} on GitHub`;
                        const img = document.createElement("img");
                        img.alt = o;
                        img.src = `https://github.com/${encodeURIComponent(o)}.png?size=32`;
                        img.addEventListener("error", () => { img.remove(); a.textContent = "@" + o; });
                        a.appendChild(img);
                        owners.appendChild(a);
                    }
                    side.appendChild(owners);
                }
                row.append(open, side);
                ul.appendChild(row);
            }
            return ul;
        }
        function drawPackage(wrap) {
            const card = h("div", "slls-sx-dlm-card");
            const head = h("div", "slls-sx-dlm-card-head");
            const who = h("div", "slls-sx-dlm-main");
            who.appendChild(h("span", "slls-sx-dlm-name", pkg.packageId));
            if (pkg.authors) who.appendChild(h("span", "slls-sx-dlm-by", "by " + pkg.authors));
            head.appendChild(who);
            const actions = h("div", "slls-sx-dlm-actions");
            const label = h("label", "", "Version");
            const sel = h("select", "slls-sx-select");
            sel.setAttribute("aria-label", "Version");
            for (const v of pkg.versions) {
                const o = h("option", "", v);
                o.value = v;
                if (v === pkg.version) o.selected = true;
                sel.appendChild(o);
            }
            sel.disabled = loading;
            sel.addEventListener("change", () => load(pkg.packageId, sel.value));
            label.appendChild(sel);
            actions.appendChild(label);
            if (writable()) {
                const n = pkg.functions.length;
                const b = button("slls-sx-btn primary", `Stage ${plural(n, "function")}`, "plus", () => stage(pkg),
                    "Stage these functions \u2014 they are written to the model when you Save");
                b.disabled = loading || !n;
                actions.appendChild(b);
            }
            head.appendChild(actions);
            card.appendChild(head);
            const desc = pkg.description || ((dlCatalog || []).find((p) => p.id.toLowerCase() === pkg.packageId.toLowerCase()) || {}).description;
            if (desc) card.appendChild(h("p", "slls-sx-dlm-text", desc));
            const note = h("p", "slls-sx-dlm-note");
            if (writable()) {
                note.innerHTML = "Staging adds these functions to your unsaved changes; they are written on Save and replace any "
                    + `existing functions already installed from this package. Functions are named <code>${esc(pkg.packageId)}.*</code>.`;
            } else {
                note.append(document.createTextNode("Read-only. "));
                const link = h("a", "slls-sx-link", "Enable edit mode");
                link.addEventListener("click", () => setMode("write"));
                note.append(link, document.createTextNode(" to add this package to the model."));
            }
            card.appendChild(note);
            wrap.appendChild(card);
            wrap.appendChild(h("div", "slls-sx-dlm-section", `Functions (${pkg.functions.length})`));
            const fl = h("div", "slls-sx-dlm-list");
            for (const fn of pkg.functions) {
                const item = h("div", "slls-sx-dlm-fn");
                const b = h("button", "slls-sx-dlm-fn-head");
                b.type = "button";
                b.setAttribute("aria-expanded", String(openFns.has(fn.name)));
                const caret = h("span", "slls-sx-caret" + (openFns.has(fn.name) ? " open" : ""));
                caret.innerHTML = ICON.caret_right;
                b.append(caret, iconEl("zap", ""), h("span", "name", fn.name));
                b.addEventListener("click", () => {
                    if (openFns.has(fn.name)) openFns.delete(fn.name); else openFns.add(fn.name);
                    const scroller = m.body.querySelector(".slls-sx-dlm-scroll");
                    const top = scroller ? scroller.scrollTop : 0;
                    draw();
                    const next = m.body.querySelector(".slls-sx-dlm-scroll");
                    if (next) next.scrollTop = top;
                });
                item.appendChild(b);
                if (openFns.has(fn.name)) {
                    const pre = h("pre", "");
                    pre.innerHTML = highlight(fn.definition, "dax");
                    item.appendChild(pre);
                }
                fl.appendChild(item);
            }
            wrap.appendChild(fl);
        }
        function paintList() {
            if (!list) return;
            clear(list);
            const byId = catalogById();
            const rows = dlRows();
            if (tab === "installed") list.appendChild(drawInstalled(rows, byId));
            else list.appendChild(drawBrowse(dlInstalled()));
        }
        function draw() {
            const rows = dlRows();
            if (tab === null) tab = rows.length ? "installed" : "browse";
            clear(m.actions);
            const site = h("a", "slls-sx-btn small slls-sx-dlm-browse");
            site.href = "https://daxlib.org/packages/";
            site.target = "_blank";
            site.rel = "noreferrer";
            site.append(document.createTextNode("Browse daxlib.org"), iconEl("external_link", ""));
            m.actions.append(site, toolModeSwitch());

            clear(m.body);
            list = null;
            const bar = h("div", "slls-sx-dlm-toolbar");
            if (pkg || loadError) {
                bar.appendChild(button("slls-sx-btn small", "Back to packages", "arrow_left", () => {
                    pkg = null; loadError = null; seq += 1; loading = false; draw();
                }));
            } else {
                const sw = h("div", "slls-sx-search");
                sw.appendChild(iconEl("search", ""));
                const input = h("input", "slls-sx-input");
                input.type = "search";
                input.placeholder = "Search packages (e.g. DaxLib.SVG)";
                input.setAttribute("aria-label", "Search packages");
                input.value = search;
                input.addEventListener("input", () => { search = input.value; paintList(); });
                sw.appendChild(input);
                bar.appendChild(sw);
                if (tab === "browse") {
                    const tags = [...new Set((dlCatalog || []).flatMap((p) => p.tags))].sort((a, b) => a.localeCompare(b));
                    const sel = h("select", "slls-sx-select");
                    sel.setAttribute("aria-label", "Filter by tag");
                    const all = h("option", "", "All tags");
                    all.value = "";
                    sel.appendChild(all);
                    for (const t of tags) { const o = h("option", "", t); o.value = t; if (t === tag) o.selected = true; sel.appendChild(o); }
                    sel.addEventListener("change", () => { tag = sel.value; paintList(); });
                    bar.appendChild(sel);
                }
            }
            if (loading) {
                const s = h("span", "slls-sx-dlm-status");
                s.textContent = "Loading package\u2026";
                bar.appendChild(s);
            } else if (status) bar.appendChild(h("span", "slls-sx-dlm-status " + status.kind, status.text));
            m.body.appendChild(bar);

            const scroll = h("div", "slls-sx-dlm-scroll");
            const wrap = h("div", "slls-sx-dlm-wrap");
            scroll.appendChild(wrap);
            m.body.appendChild(scroll);
            if (loadError) {
                const err = h("div", "slls-sx-dlm-error");
                err.append(iconEl("alert", ""), h("div", "", loadError));
                wrap.appendChild(err);
            } else if (pkg) {
                drawPackage(wrap);
            } else {
                const byId = catalogById();
                const updates = rows.filter((r) => hasUpdate(r, byId)).length;
                const tabs = h("div", "slls-sx-tabs");
                tabs.setAttribute("role", "tablist");
                const mk = (id, text, icon, count, extra) => {
                    const b = h("button", "slls-sx-tab" + (tab === id ? " active" : ""));
                    b.type = "button";
                    b.setAttribute("role", "tab");
                    b.setAttribute("aria-selected", String(tab === id));
                    b.append(iconEl(icon, ""), document.createTextNode(text));
                    if (count !== null) b.appendChild(h("span", "slls-sx-pill", String(count)));
                    if (extra) b.appendChild(extra);
                    b.addEventListener("click", () => { tab = id; status = null; draw(); });
                    return b;
                };
                let badge = null;
                if (updates) {
                    badge = h("span", "slls-sx-pill warn", plural(updates, "update"));
                    badge.title = `${plural(updates, "update")} available`;
                }
                tabs.append(mk("installed", "Installed", "check_circle", rows.length || null, badge),
                    mk("browse", "Browse", "package", dlCatalog ? dlCatalog.length : null));
                wrap.appendChild(tabs);
                list = h("div", "");
                wrap.appendChild(list);
                paintList();
            }
            const foot = h("div", "slls-sx-dlm-foot");
            const a = h("a", "slls-sx-link", "DAXLib.org");
            a.href = "https://daxlib.org";
            a.target = "_blank";
            a.rel = "noreferrer";
            foot.append(document.createTextNode("Packages are published on "), a, document.createTextNode("."));
            wrap.appendChild(foot);
        }
        if (!dlCatalog) {
            request("daxlib_catalog", {}, (res) => {
                if (res.ok) dlCatalog = res.data.packages || [];
                else catalogError = res.error || "The DAXLib.org package list could not be loaded.";
                if (m.overlay.isConnected) draw();
            });
        }
        draw();
    }
    // ------------------------------------------------------------------
    // Shared tool-window helpers
    // ------------------------------------------------------------------
    const cmpText = (a, b) => String(a).localeCompare(String(b), undefined, { sensitivity: "base" });
    function toolWindow(title, icon, bodyClass) {
        const m = modal(title, icon, { wide: true });
        m.box.classList.add("slls-sx-fpm-box", "slls-sx-tool");
        m.body.className = "slls-sx-modal-body slls-sx-tool-body" + (bodyClass ? " " + bodyClass : "");
        return m;
    }
    function toolSubtitle(m, text) {
        let sub = m.box.querySelector(".slls-sx-modal-sub");
        if (!sub) {
            sub = h("div", "slls-sx-modal-sub");
            m.box.querySelector(".slls-sx-modal-title").after(sub);
        }
        sub.textContent = text;
        sub.title = text;
    }
    // Tool header actions: extra buttons, then (in edit mode) the pending
    // count, undo and save, then the read-only / edit switch.
    function toolActions(m, extras) {
        clear(m.actions);
        for (const el of extras || []) if (el) m.actions.appendChild(el);
        if (mode === "write") {
            const count = pendingCount();
            const busy = !!model.get("busy") || saving;
            if (count) {
                const p = h("span", "slls-sx-pill warn slls-sx-tool-pending", plural(count, "pending change"));
                p.title = "Staged changes are written to the model when you Save";
                m.actions.appendChild(p);
            }
            const u = button("slls-sx-iconbtn", "", "undo", () => undo(), "Undo");
            u.disabled = !undoStack.length || busy;
            const s = button("slls-sx-iconbtn", "", "save", () => save(), count ? `Save ${plural(count, "change")}` : "No changes to save");
            s.disabled = !count || busy;
            if (saving) s.querySelector("svg").classList.add("slls-sx-spin");
            m.actions.append(u, s);
        }
        m.actions.appendChild(toolModeSwitch());
    }
    // Redraws a tool's body, keeping its scroll positions.
    function keepScroll(container, fn) {
        const tops = [...container.querySelectorAll("[data-scroll]")].map((el) => [el.dataset.scroll, el.scrollTop, el.scrollLeft]);
        fn();
        for (const [k, top, left] of tops) {
            const el = container.querySelector(`[data-scroll="${k}"]`);
            if (el) { el.scrollTop = top; el.scrollLeft = left; }
        }
    }
    function selectEl(options, current, onChange, aria, placeholder) {
        const sel = h("select", "slls-sx-select");
        if (aria) sel.setAttribute("aria-label", aria);
        if (placeholder !== undefined) {
            const o = new Option(placeholder, "");
            o.disabled = true;
            sel.appendChild(o);
        }
        for (const opt of options) {
            const [v, l] = Array.isArray(opt) ? opt : [opt, opt];
            sel.appendChild(new Option(l, v));
        }
        sel.value = current == null ? "" : String(current);
        sel.addEventListener("change", () => onChange(sel.value));
        return sel;
    }
    function switchEl(on, label, onClick, text) {
        const sw = h("button", "slls-sx-switch" + (on ? " on" : ""));
        sw.type = "button";
        sw.setAttribute("role", "switch");
        sw.setAttribute("aria-checked", String(!!on));
        sw.setAttribute("aria-label", label);
        sw.append(h("span", "slls-sx-track"), document.createTextNode(text !== undefined ? text : (on ? "Yes" : "No")));
        sw.addEventListener("click", onClick);
        return sw;
    }
    function dirtyDot(on) {
        const d = h("span", "slls-sx-dot");
        d.title = "Unsaved change";
        if (!on) d.style.visibility = "hidden";
        return d;
    }
    function searchBox(placeholder, valueText, onInput) {
        const sw = h("div", "slls-sx-search");
        sw.appendChild(iconEl("search", ""));
        const input = h("input", "slls-sx-input");
        input.type = "search";
        input.placeholder = placeholder;
        input.setAttribute("aria-label", placeholder.replace(/\u2026$/, ""));
        input.value = valueText;
        input.addEventListener("input", () => onInput(input.value));
        sw.appendChild(input);
        return sw;
    }
    function field(label, control) {
        const f = h("label", "slls-sx-tool-field");
        f.append(h("span", "", label), control);
        return f;
    }
    function savedTables() { return (data.tables || []).filter((t) => !isDeleted(t)); }
    function liveColumns(t) { return (t.columns || []).filter((c) => !isDeleted(c)); }
    function columnType(table, column) {
        const n = index.get(keyOf("column", table, "", column));
        return n ? String(value(n, "DataType") || "") : "";
    }
    // A multi-select object-type dropdown (selected = shown types).
    function typeFilterEl(options, selected, emptyLabel, onChange) {
        const tf = h("div", "slls-sx-typefilter slls-sx-tool-typefilter");
        const btn = h("button", "slls-sx-typebtn");
        btn.type = "button";
        btn.title = "Filter by object type";
        const summary = () => {
            if (selected.size === 0) return emptyLabel;
            if (selected.size === options.length) return "All object types";
            if (selected.size === 1) return options.find((o) => selected.has(o[0]))[1];
            return plural(selected.size, "type");
        };
        const paintBtn = () => { btn.innerHTML = `<span>${ICON.sliders}${esc(summary())}</span>${ICON.chevron_down}`; };
        paintBtn();
        tf.appendChild(btn);
        let menu = null;
        const closeMenu = () => { if (menu) { menu.remove(); menu = null; document.removeEventListener("mousedown", outside, true); } };
        const outside = (ev) => { if (!tf.contains(ev.target)) closeMenu(); };
        const changed = () => { paintBtn(); onChange(); };
        btn.addEventListener("click", () => {
            if (menu) { closeMenu(); return; }
            menu = h("div", "slls-sx-menu");
            const mh = h("div", "slls-sx-menu-head");
            mh.appendChild(h("span", "", "Object types"));
            const all = h("button", "", "Select all");
            all.type = "button";
            const none = h("button", "", "Clear");
            none.type = "button";
            const boxes = [];
            all.addEventListener("click", () => { options.forEach((o) => selected.add(o[0])); boxes.forEach((b) => { b.checked = true; }); changed(); });
            none.addEventListener("click", () => { selected.clear(); boxes.forEach((b) => { b.checked = false; }); changed(); });
            mh.append(all, none);
            menu.appendChild(mh);
            for (const [id, label, icon] of options) {
                const item = h("label", "slls-sx-menu-item");
                const cb = h("input");
                cb.type = "checkbox";
                cb.checked = selected.has(id);
                cb.addEventListener("change", () => { if (cb.checked) selected.add(id); else selected.delete(id); changed(); });
                boxes.push(cb);
                item.append(cb, iconEl(icon, ""), document.createTextNode(label));
                menu.appendChild(item);
            }
            tf.appendChild(menu);
            document.addEventListener("mousedown", outside, true);
        });
        return tf;
    }
    // The table -> objects tree shared by the description and translation
    // managers. `kinds` orders a table's children.
    const OBJ_KINDS = {
        Table: ["Tables", "table"], Column: ["Columns", "column"], Measure: ["Measures", "measure"],
        Hierarchy: ["Hierarchies", "hierarchy"], CalculationItem: ["Calculation items", "calculation_item"], Level: ["Levels", "level"],
    };
    function objectTree(kinds) {
        const byN = (a, b) => cmpText(effName(a), effName(b));
        const out = [];
        for (const t of savedTables().sort(byLabel)) {
            const tHidden = isHidden(t);
            const row = { id: t.key, type: "Table", node: t, hidden: tHidden, children: [] };
            const leaf = (type, n, hidden) => ({ id: n.key, type, node: n, hidden, children: [] });
            for (const kind of kinds) {
                if (kind === "Measure") {
                    for (const x of [...(t.measures || []), ...newNodes("measure", t.key)].filter((n) => !isDeleted(n)).sort(byN)) {
                        row.children.push(leaf("Measure", x, tHidden || isHidden(x)));
                    }
                } else if (kind === "Column") {
                    for (const x of [...(t.columns || []), ...newNodes("column", t.key)].filter((n) => !isDeleted(n)).sort(byN)) {
                        row.children.push(leaf("Column", x, tHidden || isHidden(x)));
                    }
                } else if (kind === "CalculationItem") {
                    for (const x of [...(t.calculation_items || []), ...newNodes("calculationItem", t.key)].filter((n) => !isDeleted(n)).sort(byN)) {
                        row.children.push(leaf("CalculationItem", x, tHidden));
                    }
                } else if (kind === "Hierarchy") {
                    for (const x of (t.hierarchies || []).filter((n) => !isDeleted(n)).sort(byN)) {
                        const hh = tHidden || isHidden(x);
                        const hr = leaf("Hierarchy", x, hh);
                        for (const lv of x.levels || []) hr.children.push(leaf("Level", lv, hh));
                        row.children.push(hr);
                    }
                }
            }
            out.push(row);
        }
        return out;
    }
    function treeVisible(tree, matches, filtering, expandedIds) {
        const out = [];
        const vis = (n) => matches(n) || n.children.some(vis);
        const walk = (n, depth) => {
            if (!vis(n)) return;
            const kids = filtering ? n.children.filter(vis) : n.children;
            const open = expandedIds.has(n.id);
            out.push({ n, depth, hasChildren: kids.length > 0, open });
            if (kids.length && open) for (const k of kids) walk(k, depth + 1);
        };
        for (const n of tree) walk(n, 0);
        return out;
    }
    // When filtering, open every ancestor of a matching object.
    function autoExpand(tree, matches) {
        const ids = new Set();
        const visit = (n) => {
            let any = false;
            for (const k of n.children) if (visit(k)) any = true;
            if (any) ids.add(n.id);
            return matches(n) || any;
        };
        tree.forEach(visit);
        return ids;
    }
    function flatTree(tree) {
        const out = [];
        const walk = (n) => { out.push(n); n.children.forEach(walk); };
        tree.forEach(walk);
        return out;
    }
    function objIcon(n) {
        if (n.type === "Table") return tableIcon(n.node);
        if (n.type === "Column") return n.node.column_type === "Calculated" ? "calculated_column" : "column";
        return OBJ_KINDS[n.type][1];
    }
    function caretBtn(open, onClick) {
        const b = h("button", "slls-sx-tool-caret");
        b.type = "button";
        b.setAttribute("aria-label", open ? "Collapse" : "Expand");
        b.setAttribute("aria-expanded", String(open));
        const c = h("span", "slls-sx-caret" + (open ? " open" : ""));
        c.innerHTML = ICON.caret_right;
        b.appendChild(c);
        b.addEventListener("click", onClick);
        return b;
    }
    // A checkbox whose checked / indeterminate state follows a set of ids.
    function triCheckbox(ids, selected, title, onToggle) {
        const cb = h("input", "slls-sx-tool-check");
        cb.type = "checkbox";
        const n = ids.filter((id) => selected.has(id)).length;
        cb.checked = ids.length > 0 && n === ids.length;
        cb.indeterminate = n > 0 && n < ids.length;
        cb.disabled = !ids.length;
        cb.title = title;
        cb.setAttribute("aria-label", title);
        cb.sllsIds = ids;
        cb.sllsSelected = selected;
        cb.addEventListener("change", () => {
            const on = !(ids.length > 0 && ids.every((id) => selected.has(id)));
            for (const id of ids) { if (on) selected.add(id); else selected.delete(id); }
            onToggle();
        });
        return cb;
    }
    // Refreshes every selection checkbox in place. Rebuilding the rows instead
    // would collapse the scroller and jump the view back to the top.
    function syncTriCheckboxes(container) {
        for (const cb of container.querySelectorAll("input.slls-sx-tool-check")) {
            if (!cb.sllsIds) continue;
            const n = cb.sllsIds.filter((id) => cb.sllsSelected.has(id)).length;
            cb.checked = cb.sllsIds.length > 0 && n === cb.sllsIds.length;
            cb.indeterminate = n > 0 && n < cb.sllsIds.length;
        }
    }

    // ------------------------------------------------------------------
    // Relationships: effective (pending-aware) view + staging
    // ------------------------------------------------------------------
    const REL_CARDINALITY = ["One", "Many", "None"];
    const REL_CROSS = ["OneDirection", "BothDirections", "Automatic"];
    const REL_SECURITY = ["OneDirection", "BothDirections", "None"];
    const REL_JOIN = ["DateAndTime", "DatePartOnly"];
    function relView(node) {
        const v = (p, d) => { const x = value(node, p); return x === null || x === undefined || x === "" ? d : String(x); };
        return {
            key: node.key, node, isNew: !!node.isNew,
            from_table: node.from_table, to_table: node.to_table,
            from_column: node.isNew ? node.from_column : v("FromColumn", node.from_column),
            to_column: node.isNew ? node.to_column : v("ToColumn", node.to_column),
            from_card: v("FromCardinality", "Many"), to_card: v("ToCardinality", "One"),
            cross: v("CrossFilteringBehavior", "OneDirection"), security: v("SecurityFilteringBehavior", "OneDirection"),
            rely: !!value(node, "RelyOnReferentialIntegrity"), active: !!value(node, "IsActive"),
        };
    }
    function liveRelationships() {
        const out = (data.relationships || []).filter((r) => !isDeleted(r)).map(relView);
        for (const c of staging.creates) if (c.type === "relationship") out.push(relView(syntheticNode(c)));
        return out;
    }
    function stageRelationship(spec) {
        if (saving || mode !== "write") return;
        pushUndo("");
        const id = ++createSeq + "-" + Date.now().toString(36);
        staging.creates.push({ id, type: "relationship", parentKey: "",
            name: `'${spec.fromTable}'[${spec.fromColumn}] \u2192 '${spec.toTable}'[${spec.toColumn}]`,
            props: { fromTable: spec.fromTable, fromColumn: spec.fromColumn, toTable: spec.toTable, toColumn: spec.toColumn,
                FromCardinality: spec.fromCardinality || "Many", ToCardinality: spec.toCardinality || "One",
                CrossFilteringBehavior: spec.cross || "OneDirection", IsActive: spec.active !== false } });
        expanded.add("model");
        expanded.add("folder:relationships");
        afterChange();
    }
    function unstageCreate(id) {
        if (saving) return;
        pushUndo("");
        staging.creates = staging.creates.filter((c) => c.id !== id && c.parentKey !== "new:" + id);
        afterChange();
    }
    function columnOptions(tableName) {
        const t = tableByName.get(tableName);
        if (!t) return [];
        return liveColumns(t).sort(byLabel).map((c) => {
            const dt = value(c, "DataType");
            return [c.name, dt ? `${effName(c)} \u00b7 ${dt}` : effName(c)];
        });
    }

    // ------------------------------------------------------------------
    // Relationship manager
    // ------------------------------------------------------------------
    function openRelationshipManager() {
        const m = toolWindow("Relationship manager", "git_branch");
        let search = "";
        const open = new Set();
        const draft = { fromTable: "", fromColumn: "", fromCardinality: "Many", toTable: "", toColumn: "", toCardinality: "One",
            cross: "OneDirection", active: true };
        let list = null;
        const eligible = () => savedTables().filter((t) => t.kind !== "calculation_group").sort(byLabel).map((t) => [t.name, effName(t)]);
        const write = () => mode === "write" && !saving;

        function addForm() {
            const card = h("div", "slls-sx-tool-card");
            card.appendChild(h("div", "slls-sx-section-title", "Add a relationship"));
            const grid = h("div", "slls-sx-rm-form");
            const side = (label, tKey, cKey, cardKey) => {
                const row = h("div", "slls-sx-rm-side");
                row.appendChild(h("span", "slls-sx-rm-side-label", label));
                row.appendChild(field("Table", selectEl(eligible(), draft[tKey], (v) => { draft[tKey] = v; draft[cKey] = ""; draw(); }, `${label} table`, "Select\u2026")));
                const dt = draft[cKey] ? columnType(draft[tKey], draft[cKey]) : "";
                const cs = selectEl(columnOptions(draft[tKey]), draft[cKey], (v) => { draft[cKey] = v; draw(); }, `${label} column`, "Select\u2026");
                cs.disabled = !draft[tKey];
                row.appendChild(field(dt ? `Column \u00b7 ${dt}` : "Column", cs));
                row.appendChild(field("Cardinality", selectEl(REL_CARDINALITY, draft[cardKey], (v) => { draft[cardKey] = v; }, `${label} cardinality`)));
                return row;
            };
            grid.append(side("From", "fromTable", "fromColumn", "fromCardinality"), side("To", "toTable", "toColumn", "toCardinality"));
            card.appendChild(grid);
            if (draft.fromColumn && draft.toColumn) {
                const a = columnType(draft.fromTable, draft.fromColumn) || "unknown";
                const b = columnType(draft.toTable, draft.toColumn) || "unknown";
                const mismatch = a !== "unknown" && b !== "unknown" && a !== b;
                card.appendChild(h("div", "slls-sx-rm-types" + (mismatch ? " warn" : ""),
                    `Column data types: ${a} \u2192 ${b}${mismatch ? " \u00b7 types don't match" : ""}`));
            }
            const bottom = h("div", "slls-sx-rm-bottom");
            bottom.appendChild(field("Cross filtering behavior", selectEl(REL_CROSS, draft.cross, (v) => { draft.cross = v; }, "Cross filtering behavior")));
            const act = switchEl(draft.active, "Active", () => { draft.active = !draft.active; draw(); });
            bottom.appendChild(field("Active", act));
            const add = button("slls-sx-btn primary", "Add", "plus", () => {
                stageRelationship({ fromTable: draft.fromTable, fromColumn: draft.fromColumn, toTable: draft.toTable, toColumn: draft.toColumn,
                    fromCardinality: draft.fromCardinality, toCardinality: draft.toCardinality, cross: draft.cross, active: draft.active });
                draft.fromColumn = ""; draft.toColumn = "";
                draw();
            });
            add.disabled = !(draft.fromTable && draft.fromColumn && draft.toTable && draft.toColumn);
            bottom.appendChild(add);
            card.appendChild(bottom);
            return card;
        }
        function relCard(r) {
            const node = r.node;
            const deleted = isDeleted(node);
            const card = h("div", "slls-sx-rm-card" + (deleted ? " deleted" : ""));
            const head = h("div", "slls-sx-rm-card-head");
            const toggle = h("button", "slls-sx-rm-toggle");
            toggle.type = "button";
            toggle.setAttribute("aria-expanded", String(open.has(r.key)));
            const caret = h("span", "slls-sx-caret" + (open.has(r.key) ? " open" : ""));
            caret.innerHTML = ICON.caret_right;
            toggle.append(caret, iconEl("relationship", ""), h("span", "name", displayLabel(node)));
            if (isDirty(node) && !deleted) toggle.appendChild(dirtyDot(true));
            if (!relView(node).active) toggle.appendChild(h("span", "slls-sx-pill", "Inactive"));
            if (deleted) toggle.appendChild(h("span", "slls-sx-pill danger", "Deleted"));
            toggle.addEventListener("click", () => { if (open.has(r.key)) open.delete(r.key); else open.add(r.key); draw(); });
            head.appendChild(toggle);
            if (write()) {
                head.appendChild(deleted
                    ? button("slls-sx-iconbtn", "", "undo", () => toggleOp(node, "delete"), "Restore relationship")
                    : button("slls-sx-iconbtn danger", "", "trash", () => toggleOp(node, "delete"), "Delete relationship"));
            }
            card.appendChild(head);
            if (!open.has(r.key)) return card;
            const v = relView(node);
            const grid = h("div", "slls-sx-rm-grid");
            const editable = write() && !deleted;
            const row = (label, prop, current, kind, options) => {
                const cell = h("div", "slls-sx-rm-row");
                const lab = h("span", "label", label);
                if (prop) lab.appendChild(dirtyDot(!!staging.edits[editKey(node.key, prop)]));
                cell.appendChild(lab);
                let ctl;
                if (!editable || !prop) {
                    ctl = h("span", "val", kind === "bool" ? (current ? "Yes" : "No") : (current || "\u2014"));
                    if (kind === "column") ctl.textContent = columnDisplayName(options, current);
                } else if (kind === "bool") {
                    ctl = switchEl(current, label, () => setValue(node, prop, !current, { label }));
                } else if (kind === "column") {
                    ctl = selectEl(columnOptions(options), current, (nv) => setValue(node, prop, nv, { label }), label);
                } else {
                    ctl = selectEl(options, current, (nv) => setValue(node, prop, nv, { label }), label);
                }
                cell.appendChild(ctl);
                grid.appendChild(cell);
            };
            row("Name", "", node.name);
            row("Active", "IsActive", v.active, "bool");
            row("From column", "FromColumn", v.from_column, "column", v.from_table);
            row("From data type", "", columnType(v.from_table, v.from_column));
            row("To column", "ToColumn", v.to_column, "column", v.to_table);
            row("To data type", "", columnType(v.to_table, v.to_column));
            row("From cardinality", "FromCardinality", v.from_card, "select", REL_CARDINALITY);
            row("To cardinality", "ToCardinality", v.to_card, "select", REL_CARDINALITY);
            row("Cross filtering behavior", "CrossFilteringBehavior", v.cross, "select", REL_CROSS);
            row("Security filtering behavior", "SecurityFilteringBehavior", v.security, "select", REL_SECURITY);
            row("Rely on referential integrity", "RelyOnReferentialIntegrity", v.rely, "bool");
            row("Join on date behavior", "JoinOnDateBehavior", String(value(node, "JoinOnDateBehavior") || "DateAndTime"), "select", REL_JOIN);
            card.appendChild(grid);
            return card;
        }
        function paintList() {
            if (!list) return;
            clear(list);
            const q = search.trim().toLowerCase();
            const staged = staging.creates.filter((c) => c.type === "relationship");
            for (const c of staged) {
                const n = syntheticNode(c);
                const v = relView(n);
                const row = h("div", "slls-sx-rm-new");
                row.appendChild(h("span", "slls-sx-pill new", "New"));
                const fromDt = columnType(v.from_table, v.from_column);
                const toDt = columnType(v.to_table, v.to_column);
                row.appendChild(h("span", "name",
                    `'${tableDisplayName(v.from_table)}'[${columnDisplayName(v.from_table, v.from_column)}]${fromDt ? ` (${fromDt})` : ""} \u2192 `
                    + `'${tableDisplayName(v.to_table)}'[${columnDisplayName(v.to_table, v.to_column)}]${toDt ? ` (${toDt})` : ""}`
                    + ` \u00b7 ${v.from_card}:${v.to_card} \u00b7 ${v.cross}${v.active ? "" : " \u00b7 inactive"}`));
                if (write()) row.appendChild(button("slls-sx-iconbtn", "", "close", () => unstageCreate(c.id), "Remove staged relationship"));
                list.appendChild(row);
            }
            const rels = (data.relationships || []).filter((r) => !q || displayLabel(r).toLowerCase().includes(q)).sort(byLabel);
            for (const r of rels) list.appendChild(relCard(relView(r)));
            if (!(data.relationships || []).length && !staged.length) list.appendChild(h("div", "slls-sx-empty", "No relationships in this model."));
            else if (!rels.length && q) list.appendChild(h("div", "slls-sx-empty", `No relationships match \u201c${search}\u201d.`));
        }
        function draw() {
            const total = (data.relationships || []).length;
            const staged = staging.creates.filter((c) => c.type === "relationship").length;
            toolSubtitle(m, plural(total, "relationship") + (staged ? ` \u00b7 ${staged} new` : ""));
            toolActions(m);
            keepScroll(m.body, () => {
                clear(m.body);
                const scroll = h("div", "slls-sx-tool-scroll");
                scroll.dataset.scroll = "main";
                const wrap = h("div", "slls-sx-tool-wrap");
                scroll.appendChild(wrap);
                if (write()) wrap.appendChild(addForm());
                else {
                    const info = h("div", "slls-sx-fpm-info");
                    info.append(iconEl("info", ""), h("span", "", "Read-only. Switch to edit mode to add, change or delete relationships."));
                    wrap.appendChild(info);
                }
                wrap.appendChild(searchBox("Search relationships\u2026", search, (v) => { search = v; paintList(); }));
                list = h("div", "slls-sx-rm-list");
                wrap.appendChild(list);
                paintList();
                m.body.appendChild(scroll);
            });
        }
        registerTool(m, draw, () => toolActions(m));
        draw();
    }

    // ------------------------------------------------------------------
    // Security manager
    // ------------------------------------------------------------------
    const OLS_PERMISSIONS = ["Default", "Read", "None"];
    const MODEL_PERMISSIONS = ["None", "Read", "ReadRefresh", "Refresh", "Administrator"];
    const RLS_DOCS_URL = "https://learn.microsoft.com/fabric/security/service-admin-row-level-security";
    // Compares two {table: filter} maps, treating blank filters as absent.
    function canonFilters(map) {
        return JSON.stringify(Object.keys(map).filter((k) => String(map[k] || "").trim()).sort().map((k) => [k, map[k]]));
    }
    function openSecurityManager() {
        const m = toolWindow("Security manager", "shield_check");
        let active = null;
        let memberDraft = "";
        let view = "rls";
        let rlsSearch = "";
        let olsSearch = "";
        let olsFilter = "all";
        const openTables = new Set();
        const write = () => mode === "write" && !saving;
        const roles = () => [...(data.roles || []), ...newNodes("role", "")].sort((a, b) => cmpText(effName(a), effName(b)));
        const roleNode = () => roles().find((r) => r.key === active) || null;
        const members = (r) => jsonValue(r, "Members", []);
        const filters = (r) => jsonValue(r, "RlsFilters", {});
        const ols = (r) => jsonValue(r, "Ols", []);
        const olsKey = (t, c) => (c ? `${t}\u0000${c}` : t);
        const olsMap = (entries) => new Map(entries.map((o) => [olsKey(o.table, o.column || ""), o.permission]));
        const rlsTables = () => savedTables().filter((t) => t.kind !== "calculation_group" && t.kind !== "field_parameter").sort(byLabel);
        let sideNames = new Map();

        function setFilter(r, table, text) {
            const orig = JSON.parse(originalValue(r, "RlsFilters") || "{}");
            const next = Object.assign({}, filters(r), { [table]: text });
            if (!text && !(table in orig)) delete next[table];
            const nv = canonFilters(next) === canonFilters(orig) ? originalValue(r, "RlsFilters") : JSON.stringify(next);
            setValue(r, "RlsFilters", nv, { label: "Row-level security", coalesce: true, keepDetail: true });
        }
        function setOls(r, table, column, perm) {
            const map = olsMap(ols(r));
            const k = olsKey(table, column);
            if (perm === "Default") map.delete(k); else map.set(k, perm);
            const next = sortOls([...map.entries()].map(([key, p]) => {
                const i = key.indexOf("\u0000");
                return i < 0 ? { table: key, column: "", permission: p } : { table: key.slice(0, i), column: key.slice(i + 1), permission: p };
            }));
            setValue(r, "Ols", JSON.stringify(next), { label: "Object-level security" });
        }
        function roleSummary(r) {
            const entries = ols(r);
            const rls = Object.values(filters(r)).filter((f) => String(f || "").trim()).length;
            return [`${plural(members(r).length, "member")} \u00b7 ${rls} RLS`,
                `OLS: ${plural(new Set(entries.map((o) => o.table)).size, "table")}, ${plural(entries.filter((o) => o.column).length, "column")}`];
        }
        function drawSidebar() {
            const side = h("div", "slls-sx-fpm-side");
            const head = h("div", "slls-sx-fpm-side-head");
            const list = roles();
            head.appendChild(h("span", "slls-sx-pane-label", `Roles (${list.length})`));
            if (write()) {
                head.appendChild(button("slls-sx-iconbtn", "", "plus", () => {
                    addObject("role", null);
                    active = selectedKey;
                    memberDraft = ""; rlsSearch = ""; olsSearch = ""; openTables.clear();
                    draw();
                }, "Create a new role"));
            }
            side.appendChild(head);
            const items = h("div", "slls-sx-fpm-list");
            items.dataset.scroll = "roles";
            if (!list.length) items.appendChild(h("div", "slls-sx-empty", "No roles." + (write() ? " Add one to get started." : "")));
            sideNames = new Map();
            for (const r of list) {
                const deleted = isDeleted(r);
                const b = h("button", "slls-sx-fpm-item slls-sx-sm-role" + (active === r.key ? " active" : "") + (r.isNew ? " new" : "") + (deleted ? " deleted" : ""));
                b.type = "button";
                const body = h("span", "slls-sx-sm-role-body");
                const top = h("span", "slls-sx-sm-role-top");
                const nameEl = h("span", "name", effName(r));
                sideNames.set(r.key, nameEl);
                top.appendChild(nameEl);
                if (isDirty(r) && !deleted) top.appendChild(dirtyDot(true));
                if (r.isNew) top.appendChild(h("span", "slls-sx-pill new", "New"));
                if (deleted) top.appendChild(h("span", "slls-sx-pill danger", "Deleted"));
                const [l1, l2] = roleSummary(r);
                body.append(top, h("span", "meta", l1), h("span", "meta", l2));
                b.append(iconEl("shield_check", ""), body);
                b.addEventListener("click", () => { active = r.key; memberDraft = ""; draw(); });
                if (write()) {
                    const del = h("span", "slls-sx-sm-del");
                    del.setAttribute("role", "button");
                    del.title = deleted ? "Restore role" : "Delete role";
                    del.setAttribute("aria-label", `${deleted ? "Restore" : "Delete"} role ${effName(r)}`);
                    del.innerHTML = deleted ? ICON.undo : ICON.trash;
                    del.addEventListener("click", (ev) => {
                        ev.stopPropagation();
                        if (deleted) { toggleOp(r, "delete"); return; }
                        confirmDialog(`Delete the role \u2018${effName(r)}\u2019? Its members, row-level and object-level security will be removed when you Save.`,
                            "Delete role", () => {
                                const c = createFor(r);
                                if (c) unstageCreate(c.id); else toggleOp(r, "delete");
                            });
                    });
                    b.appendChild(del);
                }
                items.appendChild(b);
            }
            side.appendChild(items);
            return side;
        }
        function drawRls(r, editable, wrap) {
            const map = filters(r);
            const orig = JSON.parse(originalValue(r, "RlsFilters") || "{}");
            const p = h("p", "slls-sx-dlm-note");
            p.append(document.createTextNode("A DAX filter expression limits which rows a member can see for that table."
                + (editable ? " Leave a table blank for no filter. " : " ")));
            const a = h("a", "slls-sx-link slls-sx-fpm-link", "How to implement RLS");
            a.href = RLS_DOCS_URL; a.target = "_blank"; a.rel = "noreferrer";
            a.appendChild(iconEl("external_link", ""));
            p.appendChild(a);
            wrap.appendChild(p);
            const listEl = h("div", "slls-sx-sm-rls");
            const paint = () => {
                clear(listEl);
                const q = rlsSearch.trim().toLowerCase();
                const tables = rlsTables().filter((t) => !q || effName(t).toLowerCase().includes(q));
                if (!tables.length) listEl.appendChild(h("div", "slls-sx-empty", "No tables match."));
                for (const t of tables) {
                    const val = map[t.name] || "";
                    const row = h("div", "slls-sx-sm-rls-row");
                    const name = h("span", "slls-sx-sm-table");
                    const dot = dirtyDot(String(val).trim() !== String(orig[t.name] || "").trim());
                    name.append(dot, iconEl(tableIcon(t), ""), h("span", "name", effName(t)));
                    name.title = effName(t);
                    row.appendChild(name);
                    if (editable) {
                        const input = h("input", "slls-sx-input slls-sx-mono");
                        input.value = val;
                        input.placeholder = '[Region] = "West"';
                        input.setAttribute("aria-label", `Row-level security filter for ${effName(t)}`);
                        input.addEventListener("input", () => {
                            map[t.name] = input.value;
                            setFilter(r, t.name, input.value);
                            dot.style.visibility = input.value.trim() !== String(orig[t.name] || "").trim() ? "" : "hidden";
                        });
                        row.appendChild(input);
                    } else {
                        const code = h("code", "slls-sx-sm-code", val || "\u2014");
                        code.title = val;
                        row.appendChild(code);
                    }
                    row.appendChild(button("slls-sx-iconbtn", "", "network", () => openRelationshipDiagram(t.name),
                        `Show '${effName(t)}' in the relationship diagram to see how row-level security flows to related tables`));
                    listEl.appendChild(row);
                }
            };
            wrap.appendChild(searchBox("Search tables\u2026", rlsSearch, (v) => { rlsSearch = v; paint(); }));
            wrap.appendChild(listEl);
            paint();
        }
        function drawOls(r, editable, wrap) {
            wrap.appendChild(h("p", "slls-sx-dlm-note",
                "Set a table or column permission to None to hide it from role members (Default = not secured)."));
            const bar = h("div", "slls-sx-sm-olsbar");
            const filterSel = selectEl([["all", "All"], ...OLS_PERMISSIONS.map((p) => [p, p])], olsFilter, (v) => { olsFilter = v; paint(); },
                "Filter object-level security by permission");
            bar.append(field("Permission", filterSel), searchBox("Search tables\u2026", olsSearch, (v) => { olsSearch = v; paint(); }));
            wrap.appendChild(bar);
            const listEl = h("div", "slls-sx-sm-ols");
            wrap.appendChild(listEl);
            const permCtl = (table, column, label) => {
                const cur = olsMap(ols(r)).get(olsKey(table, column)) || "Default";
                if (!editable) return h("span", "slls-sx-sm-perm", cur);
                return selectEl(OLS_PERMISSIONS, cur, (v) => setOls(r, table, column, v), label);
            };
            function paint() {
                clear(listEl);
                const current = olsMap(ols(r));
                const base = olsMap(JSON.parse(originalValue(r, "Ols") || "[]"));
                const changed = (t, c) => (current.get(olsKey(t, c)) || "Default") !== (base.get(olsKey(t, c)) || "Default");
                const q = olsSearch.trim().toLowerCase();
                let any = false;
                for (const t of savedTables().sort(byLabel)) {
                    if (q && !effName(t).toLowerCase().includes(q)) continue;
                    const cols = liveColumns(t).sort(byLabel);
                    const tablePerm = current.get(olsKey(t.name, "")) || "Default";
                    let shown = cols;
                    const filtered = olsFilter !== "all";
                    if (filtered) {
                        shown = cols.filter((c) => (current.get(olsKey(t.name, c.name)) || "Default") === olsFilter);
                        if (tablePerm !== olsFilter && !shown.length) continue;
                    }
                    any = true;
                    const isOpen = filtered || openTables.has(t.name);
                    const row = h("div", "slls-sx-sm-ols-row table");
                    if (filtered) row.appendChild(h("span", "slls-sx-tool-caret"));
                    else row.appendChild(caretBtn(isOpen, () => { if (openTables.has(t.name)) openTables.delete(t.name); else openTables.add(t.name); paint(); }));
                    row.append(iconEl(tableIcon(t), ""), h("span", "name", effName(t)), h("span", "meta", `${cols.length} col${cols.length === 1 ? "" : "s"}`),
                        dirtyDot(changed(t.name, "")), permCtl(t.name, "", `Object-level security for ${effName(t)}`));
                    listEl.appendChild(row);
                    if (!isOpen) continue;
                    if (!shown.length && !filtered) listEl.appendChild(h("div", "slls-sx-sm-ols-row col empty", "No columns."));
                    for (const c of shown) {
                        const cr = h("div", "slls-sx-sm-ols-row col");
                        cr.append(iconEl(c.column_type === "Calculated" ? "calculated_column" : "column", ""), h("span", "name", effName(c)),
                            dirtyDot(changed(t.name, c.name)), permCtl(t.name, c.name, `Object-level security for ${effName(c)}`));
                        cr.title = `'${effName(t)}'[${effName(c)}]`;
                        listEl.appendChild(cr);
                    }
                }
                if (!any) listEl.appendChild(h("div", "slls-sx-empty", `No tables match${olsFilter !== "all" ? ` with a \u201c${olsFilter}\u201d permission` : ""}.`));
            }
            paint();
        }
        function drawMain() {
            const main = h("div", "slls-sx-fpm-main");
            const r = roleNode();
            if (!r) {
                const empty = h("div", "slls-sx-center");
                empty.append(iconEl("shield_check", ""), h("div", "", "Select a role to view or edit its security."));
                main.appendChild(empty);
                return main;
            }
            const deleted = isDeleted(r);
            const editable = write() && !deleted;
            const scroll = h("div", "slls-sx-tool-scroll");
            scroll.dataset.scroll = "role";
            const wrap = h("div", "slls-sx-tool-wrap");
            scroll.appendChild(wrap);
            if (deleted) {
                const info = h("div", "slls-sx-fpm-info");
                info.append(iconEl("trash", ""), h("span", "", "This role is staged for deletion. Restore it to make changes."));
                wrap.appendChild(info);
            }
            const general = h("div", "slls-sx-sm-general");
            const nameLabel = h("span", "", "Name");
            nameLabel.appendChild(dirtyDot(!!staging.edits[editKey(r.key, "Name")]));
            const nameInput = h("input", "slls-sx-input");
            nameInput.value = effName(r);
            nameInput.disabled = !editable;
            nameInput.setAttribute("aria-label", "Role name");
            nameInput.addEventListener("input", () => {
                setValue(r, "Name", nameInput.value, { label: "Name", coalesce: true, keepDetail: true });
                const el = sideNames.get(r.key);
                if (el) el.textContent = nameInput.value;
            });
            const nf = h("label", "slls-sx-tool-field");
            nf.append(nameLabel, nameInput);
            const permLabel = h("span", "", "Model permission");
            permLabel.appendChild(dirtyDot(!!staging.edits[editKey(r.key, "ModelPermission")]));
            const perm = selectEl(MODEL_PERMISSIONS, String(value(r, "ModelPermission") || "Read"),
                (v) => setValue(r, "ModelPermission", v, { label: "Model permission" }), "Model permission");
            perm.disabled = !editable;
            const pf = h("label", "slls-sx-tool-field");
            pf.append(permLabel, perm);
            general.append(nf, pf);
            wrap.appendChild(general);

            const list = members(r);
            const mt = h("div", "slls-sx-section-title", `Members (${list.length})`);
            mt.appendChild(dirtyDot(!!staging.edits[editKey(r.key, "Members")]));
            wrap.appendChild(mt);
            if (editable) {
                const add = h("div", "slls-sx-sm-add");
                const input = h("input", "slls-sx-input");
                input.placeholder = "user@contoso.com or group name";
                input.setAttribute("aria-label", "New member");
                input.value = memberDraft;
                const addBtn = button("slls-sx-btn primary", "Add", "plus", () => {
                    const v = input.value.trim();
                    if (!v || list.includes(v)) return;
                    memberDraft = "";
                    setValue(r, "Members", JSON.stringify([...list, v]), { label: "Members" });
                });
                addBtn.disabled = !memberDraft.trim();
                input.addEventListener("input", () => { memberDraft = input.value; addBtn.disabled = !input.value.trim(); });
                input.addEventListener("keydown", (ev) => { if (ev.key === "Enter") { ev.preventDefault(); addBtn.click(); } });
                add.append(input, addBtn);
                wrap.appendChild(add);
            }
            if (!list.length) wrap.appendChild(h("div", "slls-sx-kv-empty", "No members."));
            else {
                const ul = h("div", "slls-sx-sm-members");
                for (const mem of list) {
                    const li = h("div", "slls-sx-sm-member");
                    li.append(iconEl("user", ""), h("span", "name", mem));
                    li.title = mem;
                    if (editable) li.appendChild(button("slls-sx-iconbtn", "", "close",
                        () => setValue(r, "Members", JSON.stringify(list.filter((x) => x !== mem)), { label: "Members" }), "Remove member"));
                    ul.appendChild(li);
                }
                wrap.appendChild(ul);
            }

            const sec = h("div", "slls-sx-sm-sechead");
            sec.appendChild(h("div", "slls-sx-section-title", "Table security"));
            const seg = h("div", "slls-sx-seg");
            for (const [id, label, title, prop] of [["rls", "RLS", "Row-level security", "RlsFilters"], ["ols", "OLS", "Object-level security", "Ols"]]) {
                const b = h("button", view === id ? "on" : "", label);
                b.type = "button";
                b.title = title;
                if (staging.edits[editKey(r.key, prop)]) b.appendChild(dirtyDot(true));
                b.addEventListener("click", () => { view = id; draw(); });
                seg.appendChild(b);
            }
            sec.appendChild(seg);
            const nFilters = Object.values(filters(r)).filter((f) => String(f || "").trim()).length;
            sec.appendChild(h("span", "slls-sx-sm-count", view === "rls" ? plural(nFilters, "table filter") : plural(ols(r).length, "secured object")));
            wrap.appendChild(sec);
            if (view === "rls") drawRls(r, editable, wrap);
            else drawOls(r, editable, wrap);
            main.appendChild(scroll);
            return main;
        }
        function draw() {
            const list = roles();
            if (active === null || !list.some((r) => r.key === active)) active = list.length ? list[0].key : null;
            toolSubtitle(m, "Manage roles, members, row-level and object-level security."
                + (mode === "write" ? " Changes are staged and applied when you Save." : ""));
            toolActions(m);
            keepScroll(m.body, () => {
                clear(m.body);
                const layout = h("div", "slls-sx-fpm");
                layout.append(drawSidebar(), drawMain());
                m.body.appendChild(layout);
            });
        }
        registerTool(m, draw, () => toolActions(m));
        draw();
    }

    // ------------------------------------------------------------------
    // Description manager
    // ------------------------------------------------------------------
    function openDescriptionManager() {
        const m = toolWindow("Description manager", "file");
        const KINDS = ["Table", "Measure", "CalculationItem", "Column", "Hierarchy", "Level"];
        const typeOptions = KINDS.map((k) => [k, OBJ_KINDS[k][0], OBJ_KINDS[k][1]]);
        const types = new Set(KINDS);
        let search = "";
        let hideHidden = false;
        let expandedIds = new Set();
        const selected = new Set();
        let generating = false;
        let genError = null;
        let tree = [];
        let rowsEl = null;
        let countEl = null;
        let headCheck = null;
        const write = () => mode === "write" && !saving;
        const filtering = () => !!search.trim() || types.size !== KINDS.length || hideHidden;
        const matches = (n) => {
            if (!types.has(n.type)) return false;
            if (hideHidden && n.hidden) return false;
            const q = search.trim().toLowerCase();
            if (!q) return true;
            const hay = `${n.type} ${effName(n.node)} ${value(n.node, "Description") || ""}`.toLowerCase();
            return hay.includes(q);
        };
        const measureIds = (list) => list.filter((n) => n.type === "Measure").map((n) => n.id);
        const filterChanged = () => { expandedIds = filtering() ? autoExpand(tree, matches) : new Set(); paintRows(); };

        function generate() {
            const all = flatTree(tree).filter((n) => n.type === "Measure" && selected.has(n.id));
            if (!all.length || generating) return;
            generating = true;
            genError = null;
            draw();
            const byKey = new Map(all.map((n) => [`${n.node.table}\u0000${n.node.name}`, n]));
            request("generate_descriptions", {
                measures: all.map((n) => ({ table: n.node.table, name: n.node.name, expression: String(value(n.node, "Expression") || "") })),
            }, (res) => {
                generating = false;
                if (!res.ok) genError = res.error || "Descriptions could not be generated.";
                else {
                    const items = [];
                    for (const g of res.data.descriptions || []) {
                        const n = byKey.get(`${g.table}\u0000${g.name}`);
                        if (n && g.description) items.push({ node: n.node, prop: "Description", value: g.description, label: "Description" });
                    }
                    if (!items.length) genError = "No descriptions were generated.";
                    selected.clear();
                    stageMany(items);
                }
                if (m.overlay.isConnected) draw();
            });
        }
        function paintRows() {
            if (!rowsEl) return;
            clear(rowsEl);
            const editable = write();
            const rows = treeVisible(tree, matches, filtering(), expandedIds);
            const shownMatches = flatTree(tree).filter(matches);
            if (countEl) countEl.textContent = `${shownMatches.length.toLocaleString()} shown`;
            if (headCheck) {
                const fresh = triCheckbox(measureIds(shownMatches), selected,
                    "Select all measures (matching the current filters) for AI description generation", () => { syncTriCheckboxes(m.body); refreshGenerate(); });
                headCheck.replaceWith(fresh);
                headCheck = fresh;
            }
            if (!rows.length) { rowsEl.appendChild(h("div", "slls-sx-empty", "No objects match the current filters.")); return; }
            for (const { n, depth, hasChildren, open } of rows) {
                const row = h("div", "slls-sx-dm-row");
                const obj = h("div", "slls-sx-dm-obj");
                obj.style.paddingLeft = 8 + depth * 18 + "px";
                obj.appendChild(hasChildren ? caretBtn(open, () => {
                    if (expandedIds.has(n.id)) expandedIds.delete(n.id); else expandedIds.add(n.id);
                    paintRows();
                }) : h("span", "slls-sx-tool-caret"));
                if (editable) {
                    if (n.type === "Measure") obj.appendChild(triCheckbox([n.id], selected, "Select for AI description generation", () => { syncTriCheckboxes(m.body); refreshGenerate(); }));
                    else if (n.type === "Table" && measureIds(n.children).length) {
                        obj.appendChild(triCheckbox(measureIds(n.children), selected, "Select all measures in this table for AI description generation",
                            () => { syncTriCheckboxes(m.body); refreshGenerate(); }));
                    } else obj.appendChild(h("span", "slls-sx-tool-check"));
                }
                obj.appendChild(iconEl(objIcon(n), ""));
                const name = h("span", "name", effName(n.node));
                name.title = effName(n.node);
                obj.appendChild(name);
                const original = String(originalValue(n.node, "Description") || "");
                const current = String(value(n.node, "Description") || "");
                const dot = dirtyDot(!!staging.edits[editKey(n.node.key, "Description")]);
                obj.appendChild(dot);
                if (n.hidden) { const e = iconEl("eye_off", "slls-sx-tool-hidden"); e.title = "Hidden"; obj.appendChild(e); }
                row.appendChild(obj);
                if (editable) {
                    const ta = h("textarea", "slls-sx-textarea slls-sx-dm-text" + (current !== original && !n.node.isNew ? " changed" : ""));
                    ta.rows = 1;
                    ta.value = current;
                    ta.placeholder = "\u2014";
                    ta.setAttribute("aria-label", `Description of ${effName(n.node)}`);
                    ta.addEventListener("input", () => {
                        setValue(n.node, "Description", ta.value, { label: "Description", coalesce: true, keepDetail: true });
                        const dirty = !!staging.edits[editKey(n.node.key, "Description")];
                        dot.style.visibility = dirty ? "" : "hidden";
                        ta.classList.toggle("changed", dirty);
                    });
                    row.appendChild(ta);
                } else {
                    const d = h("span", "slls-sx-dm-desc" + (current ? "" : " empty"), current || "\u2014");
                    if (current) d.title = current;
                    row.appendChild(d);
                }
                rowsEl.appendChild(row);
            }
        }
        let genBtn = null;
        function refreshGenerate() {
            if (!genBtn) return;
            const n = flatTree(tree).filter((x) => x.type === "Measure" && selected.has(x.id)).length;
            genBtn.lastChild.textContent = n ? `Generate (${n})` : "Generate";
            genBtn.disabled = generating || !n;
        }
        function draw() {
            tree = objectTree(["Measure", "CalculationItem", "Column", "Hierarchy"]);
            const live = new Set(flatTree(tree).map((n) => n.id));
            for (const id of [...selected]) if (!live.has(id)) selected.delete(id);
            toolSubtitle(m, `${plural(live.size, "object")} in this model`);
            genBtn = null;
            if (write()) {
                genBtn = button("slls-sx-btn", "Generate", "sparkles", generate,
                    "Auto-generate descriptions for the selected measures using AI (requires an F64 or higher capacity)");
                if (generating) genBtn.querySelector("svg").classList.add("slls-sx-spin");
                refreshGenerate();
            }
            toolActions(m, [genBtn]);
            keepScroll(m.body, () => {
                clear(m.body);
                if (genError) {
                    const err = h("div", "slls-sx-tool-error");
                    err.append(iconEl("alert", ""), h("span", "", genError));
                    m.body.appendChild(err);
                }
                const bar = h("div", "slls-sx-tool-toolbar");
                bar.appendChild(typeFilterEl(typeOptions, types, "No object types", filterChanged));
                bar.appendChild(searchBox("Search objects or descriptions\u2026", search, (v) => { search = v; filterChanged(); }));
                const hh = switchEl(hideHidden, "Hide hidden", () => {
                    hideHidden = !hideHidden;
                    hh.classList.toggle("on", hideHidden);
                    hh.setAttribute("aria-checked", String(hideHidden));
                    filterChanged();
                }, "Hide hidden");
                hh.title = "Hide objects that are hidden in the model";
                bar.appendChild(hh);
                bar.appendChild(button("slls-sx-iconbtn", "", "expand_rows", () => {
                    expandedIds = new Set(flatTree(tree).filter((n) => n.type === "Table" || n.type === "Hierarchy").map((n) => n.id));
                    paintRows();
                }, "Expand all"));
                bar.appendChild(button("slls-sx-iconbtn", "", "collapse_rows", () => { expandedIds = new Set(); paintRows(); }, "Collapse all"));
                countEl = h("span", "slls-sx-tool-count");
                bar.appendChild(countEl);
                m.body.appendChild(bar);
                const scroll = h("div", "slls-sx-tool-scroll");
                scroll.dataset.scroll = "rows";
                const head = h("div", "slls-sx-dm-row head");
                const ho = h("div", "slls-sx-dm-obj");
                headCheck = null;
                if (write()) { headCheck = h("span", "slls-sx-tool-check"); ho.appendChild(headCheck); }
                ho.appendChild(h("span", "", "Object"));
                head.append(ho, h("span", "", "Description"));
                scroll.appendChild(head);
                rowsEl = h("div", "slls-sx-dm-rows");
                scroll.appendChild(rowsEl);
                m.body.appendChild(scroll);
                paintRows();
            });
        }
        registerTool(m, draw, () => toolActions(m, [genBtn]));
        draw();
    }

    // ------------------------------------------------------------------
    // Translation manager
    // ------------------------------------------------------------------
    // Model cultures are region-qualified (e.g. fr-FR); Azure AI Translator
    // wants a language code.
    const CULTURE_TO_TRANSLATOR = {
        "zh-cn": "zh-Hans", "zh-sg": "zh-Hans", "zh-hans": "zh-Hans", "zh-tw": "zh-Hant", "zh-hk": "zh-Hant",
        "zh-mo": "zh-Hant", "zh-hant": "zh-Hant", "pt-br": "pt", "pt-pt": "pt-pt", "fr-ca": "fr-ca",
        "sr-cyrl-rs": "sr-Cyrl", "sr-latn-rs": "sr-Latn",
    };
    function translatorCode(culture) {
        const lower = String(culture).trim().toLowerCase();
        return CULTURE_TO_TRANSLATOR[lower] || lower.split("-")[0];
    }
    // Kept for this widget session only (never persisted).
    const translator = { key: "", region: "" };
    const TRANS_FIELDS = ["caption", "description", "displayFolder"];
    // Compares translation maps, treating blank values as absent.
    function canonTranslations(map) {
        const out = [];
        for (const culture of Object.keys(map || {}).sort()) {
            for (const f of TRANS_FIELDS) {
                const v = ((map[culture] || {})[f] || "").trim();
                if (v) out.push([culture, f, v]);
            }
        }
        return JSON.stringify(out);
    }
    function stageTranslationItem(node, updates) {
        const orig = JSON.parse(originalValue(node, "Translations") || "{}");
        const next = JSON.parse(JSON.stringify(jsonValue(node, "Translations", {})));
        for (const u of updates) next[u.culture] = Object.assign({}, next[u.culture], { [u.field]: u.value });
        const nv = canonTranslations(next) === canonTranslations(orig) ? originalValue(node, "Translations") : JSON.stringify(next);
        return { node, prop: "Translations", value: nv, label: "Translations" };
    }
    function openTranslationManager() {
        const m = toolWindow("Translation manager", "languages");
        const KINDS = ["Table", "Column", "Measure", "Hierarchy", "Level"];
        const typeOptions = KINDS.map((k) => [k, OBJ_KINDS[k][0], OBJ_KINDS[k][1]]);
        const types = new Set();
        const cultures = () => (data.cultures || []).map((c) => c.name).sort(cmpText);
        let culture = viewCulture && cultures().includes(viewCulture) ? viewCulture : (cultures()[0] || "");
        let search = "";
        let shown = "all";
        let hideHidden = false;
        let expandedIds = new Set();
        const selected = new Set();
        let tree = [];
        let tbody = null;
        let countEl = null;
        let headCheck = null;
        const widths = new Map();
        const write = () => mode === "write" && !saving;
        const hasFolder = (n) => ["Column", "Measure", "Hierarchy"].includes(n.type);
        const trOf = (n) => (jsonValue(n.node, "Translations", {})[culture] || {});
        const translated = (n) => { const t = trOf(n); return !!(t.caption || t.description || t.displayFolder); };
        const filtering = () => !!search.trim() || types.size > 0 || shown !== "all" || hideHidden;
        const matches = (n) => {
            if (types.size && !types.has(n.type)) return false;
            if (hideHidden && n.hidden) return false;
            if (shown === "translated" && !translated(n)) return false;
            if (shown === "missing" && translated(n)) return false;
            const q = search.trim().toLowerCase();
            if (!q) return true;
            const t = trOf(n);
            return `${n.type} ${effName(n.node)} ${t.caption || ""} ${t.description || ""} ${t.displayFolder || ""}`.toLowerCase().includes(q);
        };
        const filterChanged = () => { expandedIds = filtering() ? autoExpand(tree, matches) : new Set(); paintRows(); };

        function cellInput(n, fieldName, placeholder, disabled) {
            const td = h("td", "");
            const v = trOf(n)[fieldName] || "";
            if (!write() || disabled) {
                const span = h("span", "slls-sx-tm-val" + (v ? "" : " empty"), disabled ? "" : (v || "\u2014"));
                if (v) span.title = v;
                td.appendChild(span);
                return td;
            }
            const input = h("input", "slls-sx-input slls-sx-tm-input");
            input.value = v;
            input.placeholder = placeholder;
            input.setAttribute("aria-label", `${{ caption: "Translated name", description: "Translated description", displayFolder: "Translated display folder" }[fieldName]} of ${effName(n.node)}`);
            input.addEventListener("input", () => {
                const item = stageTranslationItem(n.node, [{ culture, field: fieldName, value: input.value }]);
                setValue(n.node, "Translations", item.value, { label: "Translations", coalesce: true, keepDetail: true });
                const dot = input.closest("tr").querySelector(".slls-sx-dot");
                if (dot) dot.style.visibility = staging.edits[editKey(n.node.key, "Translations")] ? "" : "hidden";
            });
            td.appendChild(input);
            return td;
        }
        function original(text, blank) {
            const td = h("td", "");
            const span = h("span", "slls-sx-tm-orig" + (text ? "" : " empty"), blank ? "" : (text || "\u2014"));
            if (text) span.title = text;
            td.appendChild(span);
            return td;
        }
        function paintRows() {
            if (!tbody) return;
            clear(tbody);
            const all = flatTree(tree);
            const matching = all.filter(matches);
            if (countEl) countEl.textContent = `${matching.length.toLocaleString()} shown`;
            if (headCheck) {
                const fresh = triCheckbox(matching.map((n) => n.id), selected, "Select all objects (matching the current filters) to translate",
                    () => { syncTriCheckboxes(m.body); refreshTranslate(); });
                headCheck.replaceWith(fresh);
                headCheck = fresh;
            }
            const rows = treeVisible(tree, matches, filtering(), expandedIds);
            if (!rows.length) {
                const tr = h("tr", "");
                const td = h("td", "slls-sx-empty", culture ? "No objects match the current filters." : "This model has no cultures.");
                td.colSpan = 6;
                tr.appendChild(td);
                tbody.appendChild(tr);
                return;
            }
            for (const { n, depth, hasChildren, open } of rows) {
                const tr = h("tr", "");
                const td = h("td", "");
                const obj = h("div", "slls-sx-dm-obj");
                obj.style.paddingLeft = 6 + depth * 18 + "px";
                obj.appendChild(hasChildren ? caretBtn(open, () => {
                    if (expandedIds.has(n.id)) expandedIds.delete(n.id); else expandedIds.add(n.id);
                    paintRows();
                }) : h("span", "slls-sx-tool-caret"));
                if (write()) {
                    const ids = n.type === "Table" ? flatTree([n]).map((x) => x.id) : [n.id];
                    obj.appendChild(triCheckbox(ids, selected, n.type === "Table" ? "Select all objects in this table to translate" : "Select to translate",
                        () => { syncTriCheckboxes(m.body); refreshTranslate(); }));
                }
                obj.appendChild(iconEl(objIcon(n), ""));
                const name = h("span", "name", effName(n.node));
                name.title = effName(n.node);
                obj.append(name, dirtyDot(!!staging.edits[editKey(n.node.key, "Translations")]));
                td.appendChild(obj);
                tr.appendChild(td);
                tr.appendChild(cellInput(n, "caption", effName(n.node), false));
                tr.appendChild(original(String(value(n.node, "Description") || "")));
                tr.appendChild(cellInput(n, "description", "\u2014", false));
                tr.appendChild(original(hasFolder(n) ? String(value(n.node, "DisplayFolder") || "") : "", !hasFolder(n)));
                tr.appendChild(cellInput(n, "displayFolder", "\u2014", !hasFolder(n)));
                tbody.appendChild(tr);
            }
        }
        let trBtn = null;
        function refreshTranslate() {
            if (!trBtn) return;
            const n = flatTree(tree).filter((x) => selected.has(x.id)).length;
            trBtn.lastChild.textContent = n ? `Translate (${n})` : "Translate";
            // Machine translation is switched off for now.
            trBtn.disabled = true;
        }
        function openTranslate() {
            const rows = flatTree(tree).filter((x) => selected.has(x.id));
            if (!rows.length) return;
            const names = cultures();
            const targets = new Set(culture ? [culture] : []);
            const props = { caption: true, description: true, displayFolder: true };
            let busy = false;
            let error = null;
            const d = modal("Translate objects", "sparkles");
            d.box.classList.add("slls-sx-tm-dialog");
            const paint = () => {
                clear(d.body);
                d.body.appendChild(h("div", "slls-sx-dlm-note", `${plural(rows.length, "object")} selected \u00b7 Azure AI Translator`));
                const th = h("div", "slls-sx-tm-dhead");
                th.appendChild(h("div", "slls-sx-section-title", "Target languages"));
                const all = button("slls-sx-btn small", "Select all", "", () => { names.forEach((c) => targets.add(c)); paint(); });
                const none = button("slls-sx-btn small", "Clear", "", () => { targets.clear(); paint(); });
                th.append(all, none);
                d.body.appendChild(th);
                const chips = h("div", "slls-sx-tm-chips");
                for (const c of names) {
                    const b = h("button", "slls-sx-tm-chip" + (targets.has(c) ? " on" : ""), c);
                    b.type = "button";
                    b.setAttribute("aria-pressed", String(targets.has(c)));
                    b.addEventListener("click", () => { if (targets.has(c)) targets.delete(c); else targets.add(c); paint(); });
                    chips.appendChild(b);
                }
                d.body.appendChild(chips);
                d.body.appendChild(h("div", "slls-sx-section-title", "Properties to translate"));
                const checks = h("div", "slls-sx-tm-props");
                for (const [f, label] of [["caption", "Name"], ["description", "Description"], ["displayFolder", "Display folder"]]) {
                    const l = h("label", "");
                    const cb = h("input");
                    cb.type = "checkbox";
                    cb.checked = props[f];
                    cb.addEventListener("change", () => { props[f] = cb.checked; paint(); });
                    l.append(cb, document.createTextNode(label));
                    checks.appendChild(l);
                }
                d.body.appendChild(checks);
                d.body.appendChild(h("div", "slls-sx-dlm-note", "Description and display folder are only translated for objects where that property is set."));
                d.body.appendChild(h("div", "slls-sx-section-title", "Azure AI Translator"));
                const key = h("input", "slls-sx-input");
                key.type = "password";
                key.autocomplete = "off";
                key.placeholder = "Translator subscription key";
                key.value = translator.key;
                const region = h("input", "slls-sx-input");
                region.placeholder = "e.g. westeurope (optional for a global resource)";
                region.value = translator.region;
                d.body.append(field("Key", key), field("Region", region));
                d.body.appendChild(h("div", "slls-sx-dlm-note", "Uses your own Azure AI Translator resource. The key is kept only for this session."));
                if (error) {
                    const err = h("div", "slls-sx-tool-error");
                    err.append(iconEl("alert", ""), h("span", "", error));
                    d.body.appendChild(err);
                }
                const foot = d.box.querySelector(".slls-sx-modal-foot") || h("div", "slls-sx-modal-foot");
                clear(foot);
                const go = button("slls-sx-btn primary", "Translate", "sparkles", run);
                if (busy) go.querySelector("svg").classList.add("slls-sx-spin");
                const ready = () => !busy && targets.size && key.value.trim() && (props.caption || props.description || props.displayFolder);
                go.disabled = !ready();
                key.addEventListener("input", () => { translator.key = key.value; go.disabled = !ready(); });
                region.addEventListener("input", () => { translator.region = region.value; });
                foot.append(button("slls-sx-btn", "Cancel", "", () => d.close()), go);
                d.box.appendChild(foot);
            };
            function run() {
                const fields = TRANS_FIELDS.filter((f) => props[f]);
                const tasks = [];
                for (const n of rows) {
                    const src = {
                        caption: effName(n.node),
                        description: String(value(n.node, "Description") || ""),
                        displayFolder: hasFolder(n) ? String(value(n.node, "DisplayFolder") || "") : "",
                    };
                    for (const f of fields) if (src[f].trim()) tasks.push({ n, field: f, text: src[f] });
                }
                if (!tasks.length) { error = "Nothing to translate \u2014 the selected properties are blank on the selected objects."; paint(); return; }
                busy = true;
                error = null;
                paint();
                const targetList = [...targets];
                request("translate_texts", {
                    key: translator.key, region: translator.region,
                    texts: [...new Set(tasks.map((t) => t.text))],
                    languages: [...new Set(targetList.map(translatorCode))],
                }, (res) => {
                    busy = false;
                    if (!res.ok) { error = res.error || "Translation failed."; if (d.overlay.isConnected) paint(); return; }
                    const map = res.data.translations || {};
                    const byNode = new Map();
                    for (const t of tasks) {
                        for (const c of targetList) {
                            const text = (map[t.text] || {})[translatorCode(c).toLowerCase()];
                            if (!text) continue;
                            if (!byNode.has(t.n.id)) byNode.set(t.n.id, { node: t.n.node, updates: [] });
                            byNode.get(t.n.id).updates.push({ culture: c, field: t.field, value: text });
                        }
                    }
                    selected.clear();
                    d.close();
                    stageMany([...byNode.values()].map((x) => stageTranslationItem(x.node, x.updates)));
                });
            }
            paint();
        }
        function draw() {
            if (culture && !cultures().includes(culture)) culture = cultures()[0] || "";
            tree = objectTree(["Column", "Measure", "Hierarchy"]);
            const all = flatTree(tree);
            const live = new Set(all.map((n) => n.id));
            for (const id of [...selected]) if (!live.has(id)) selected.delete(id);
            toolSubtitle(m, culture ? `${culture} \u00b7 ${all.filter(translated).length.toLocaleString()} of ${plural(all.length, "object")} translated`
                : "No cultures defined in this model");
            trBtn = null;
            if (write()) {
                trBtn = button("slls-sx-btn", "Translate", "sparkles", openTranslate,
                    "Machine translation is not available yet");
                refreshTranslate();
            }
            toolActions(m, [trBtn]);
            keepScroll(m.body, () => {
                clear(m.body);
                const bar = h("div", "slls-sx-tool-toolbar");
                const cs = selectEl(cultures().length ? cultures() : [["", "No cultures"]], culture, (v) => { culture = v; draw(); }, "Culture");
                cs.disabled = !cultures().length;
                bar.appendChild(field("Culture", cs));
                bar.appendChild(typeFilterEl(typeOptions, types, "All object types", filterChanged));
                bar.appendChild(searchBox("Search objects or translations\u2026", search, (v) => { search = v; filterChanged(); }));
                const seg = h("div", "slls-sx-seg");
                seg.setAttribute("role", "radiogroup");
                seg.setAttribute("aria-label", "Translation filter");
                for (const [id, label] of [["translated", "Only translated"], ["all", "All"], ["missing", "Missing"]]) {
                    const b = h("button", shown === id ? "on" : "", label);
                    b.type = "button";
                    b.setAttribute("role", "radio");
                    b.setAttribute("aria-checked", String(shown === id));
                    b.addEventListener("click", () => {
                        shown = id;
                        seg.querySelectorAll("button").forEach((x) => { const on = x === b; x.classList.toggle("on", on); x.setAttribute("aria-checked", String(on)); });
                        filterChanged();
                    });
                    seg.appendChild(b);
                }
                bar.appendChild(seg);
                const hh = switchEl(hideHidden, "Hide hidden", () => {
                    hideHidden = !hideHidden;
                    hh.classList.toggle("on", hideHidden);
                    hh.setAttribute("aria-checked", String(hideHidden));
                    filterChanged();
                }, "Hide hidden");
                hh.title = "Hide objects that are hidden in the model";
                bar.appendChild(hh);
                bar.appendChild(button("slls-sx-iconbtn", "", "expand_rows", () => {
                    expandedIds = new Set(all.filter((n) => n.type === "Table" || n.type === "Hierarchy").map((n) => n.id));
                    paintRows();
                }, "Expand all"));
                bar.appendChild(button("slls-sx-iconbtn", "", "collapse_rows", () => { expandedIds = new Set(); paintRows(); }, "Collapse all"));
                countEl = h("span", "slls-sx-tool-count");
                bar.appendChild(countEl);
                m.body.appendChild(bar);
                const scroll = h("div", "slls-sx-tool-scroll");
                scroll.dataset.scroll = "rows";
                const table = h("table", "slls-sx-tm-table");
                const thead = h("thead", "");
                const hr = h("tr", "");
                headCheck = null;
                [["Object", 300], ["Translated name", 200], ["Original description", 240], ["Translated description", 240],
                    ["Original display folder", 190], ["Translated display folder", 210]].forEach(([label, w], i) => {
                    const th = h("th", "");
                    th.dataset.columnWidth = String(w);
                    if (i === 0 && write()) {
                        const wrapEl = h("span", "slls-sx-tm-th");
                        headCheck = h("span", "slls-sx-tool-check");
                        wrapEl.append(headCheck, document.createTextNode(label));
                        th.appendChild(wrapEl);
                    } else th.textContent = label;
                    hr.appendChild(th);
                });
                thead.appendChild(hr);
                tbody = h("tbody", "");
                table.append(thead, tbody);
                scroll.appendChild(table);
                m.body.appendChild(scroll);
                sllsInstallColumnResizers(table, { widths, key: "translations", minWidth: 80 });
                paintRows();
            });
        }
        registerTool(m, draw, () => toolActions(m, [trBtn]));
        draw();
    }

    // ------------------------------------------------------------------
    // Relationship diagram
    // ------------------------------------------------------------------
    const RD = { HEADER_H: 34, ROW_H: 22, BODY_PAD: 11, W: 240, H: 220, MIN_W: 160, MIN_H: 120, MARGIN: 240 };
    const RD_SOURCE_COLORS = 10;
    const RD_MODE_ICON = { import: ["Import", "database"], directquery: ["DirectQuery", "cable"], dual: ["Dual", "layers"],
        directlake: ["Direct Lake", "waves"], push: ["Push", "database"] };
    const RD_FABRIC_KINDS = { lakehouse: "Lakehouse", warehouse: "Warehouse", kqldatabase: "KQL database",
        mirroreddatabase: "Mirrored database", eventhouse: "Eventhouse", sqldatabase: "SQL database", workspace: "Workspace" };
    const rdMark = (card, fallback) => (!card ? fallback : (/one/i.test(card) ? "1" : "*"));
    const rdPair = (a, b) => (a < b ? `${a}\u0000${b}` : `${b}\u0000${a}`);
    const rdEnd = (table, column) => `'${tableDisplayName(table)}'[${columnDisplayName(table, column)}]`;
    function rdTipGrid(rows) {
        const grid = h("div", "slls-sx-rd-tip-grid");
        for (const [k, v] of rows) grid.append(h("span", "k", k), h("span", "v", v || "\u2014"));
        return grid;
    }
    const rdPath = (r) => `${rdEnd(r.from_table, r.from_column)} \u2192 ${rdEnd(r.to_table, r.to_column)}`;
    function rdEdgePoint(x, y, w, hh, tx, ty) {
        const cx = x + w / 2, cy = y + hh / 2, dx = tx - cx, dy = ty - cy;
        if (dx === 0 && dy === 0) return { x: cx, y: cy };
        const s = Math.min(w / 2 / Math.abs(dx || 1e-6), hh / 2 / Math.abs(dy || 1e-6));
        return { x: cx + dx * s, y: cy + dy * s };
    }
    function rdArrow(cx, cy, ux, uy, size) {
        const px = -uy, py = ux, back = size * 0.95, half = size * 0.62, notch = size * 0.3;
        return [[cx + ux * size, cy + uy * size], [cx - ux * back + px * half, cy - uy * back + py * half],
            [cx - ux * notch, cy - uy * notch], [cx - ux * back - px * half, cy - uy * back - py * half]].map((p) => p.join(",")).join(" ");
    }
    // Import / Dual / Direct Lake tables share the VertiPaq island; each
    // DirectQuery source is its own. Crossing islands makes a limited relationship.
    function rdIslands(t) {
        const out = new Set();
        const parts = t ? t.partitions || [] : [];
        if (!parts.length) out.add("vertipaq");
        for (const p of parts) {
            const mo = String(p.mode || "").toLowerCase();
            if (mo === "directquery") out.add("dq:" + (p.data_source || "?"));
            else if (mo === "dual") { out.add("vertipaq"); out.add("dq:" + (p.data_source || "?")); }
            else out.add("vertipaq");
        }
        return out;
    }
    function rdSourceKey(t) {
        for (const p of (t && t.partitions) || []) {
            const es = originalValue(p, "Source.ExpressionSource");
            if (es) return "expr:" + es;
            if (p.data_source) return "ds:" + p.data_source;
            const expr = String(originalValue(p, "Expression") || "");
            const sql = expr.match(/Sql\.Database\(\s*"([^"]+)"\s*,\s*"([^"]+)"/i);
            if (sql) return `sql:${sql[1]}\u0000${sql[2]}`;
            const lake = expr.match(/AzureStorage\.DataLake\(\s*"([^"]+)"/i);
            if (lake) return "lake:" + lake[1];
            const ids = [...expr.matchAll(/\b([A-Za-z]+)Id\s*=\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"/g)];
            const item = ids.find((x) => x[1].toLowerCase() !== "workspace");
            const ws = ids.find((x) => x[1].toLowerCase() === "workspace");
            if (item) return `${item[1].toLowerCase()}:${ws ? ws[2] : ""}\u0000${item[2]}`;
            if (ws) return "workspace:" + ws[2];
            const ref = expr.match(/Source\s*=\s*(#"[^"]+"|[A-Za-z_]\w*)\s*[,\r\n]/);
            if (ref) return "ref:" + ref[1].replace(/^#"|"$/g, "");
        }
        return null;
    }
    function rdSourceLabel(key) {
        const i = key.indexOf(":");
        const kind = key.slice(0, i), rest = key.slice(i + 1);
        if (kind === "expr" || kind === "ds" || kind === "ref") return rest;
        if (kind === "sql") return rest.split("\u0000")[1] || rest.replace("\u0000", " / ");
        if (kind === "lake") { const seg = rest.split("/").filter(Boolean).pop(); return seg ? `OneLake \u00b7 ${seg.slice(0, 8)}\u2026` : "OneLake"; }
        const label = RD_FABRIC_KINDS[kind] || kind.charAt(0).toUpperCase() + kind.slice(1);
        const parts = rest.split("\u0000");
        const short = (parts[1] || parts[0] || "").slice(0, 8);
        return short ? `${label} \u00b7 ${short}\u2026` : label;
    }
    // Chains of strong one-to-many relationships (Fact -> Dim -> Secondary dim).
    function rdSnowflake(rels) {
        const parents = new Map(), inDegree = new Map();
        const ensure = (t) => { if (!parents.has(t)) parents.set(t, new Set()); if (!inDegree.has(t)) inDegree.set(t, 0); };
        for (const r of rels) {
            if (r.from_table === r.to_table) continue;
            const f = rdMark(r.from_card, "*"), t = rdMark(r.to_card, "1");
            let child, parent;
            if (f === "*" && t === "1") { child = r.from_table; parent = r.to_table; }
            else if (f === "1" && t === "*") { child = r.to_table; parent = r.from_table; }
            else continue;
            ensure(child); ensure(parent);
            if (!parents.get(child).has(parent)) { parents.get(child).add(parent); inDegree.set(parent, inDegree.get(parent) + 1); }
        }
        const chains = [];
        const walk = (node, path, seen) => {
            if (chains.length >= 200) return;
            const next = [...parents.get(node)].filter((p) => !seen.has(p));
            if (!next.length) { if (path.length >= 3) chains.push([...path]); return; }
            for (const p of next) { seen.add(p); path.push(p); walk(p, path, seen); path.pop(); seen.delete(p); }
        };
        for (const node of parents.keys()) if (!inDegree.get(node) && parents.get(node).size) walk(node, [node], new Set([node]));
        const tables = new Set(), pairs = new Set();
        for (const c of chains) c.forEach((t, i) => { tables.add(t); if (i) pairs.add(rdPair(c[i - 1], t)); });
        return { chains, tables, pairs };
    }
    // Isolated tables sharing a column (same name and data type) with another table.
    function rdMissing(tables, rels) {
        const related = new Set();
        for (const r of rels) { related.add(r.from_table); related.add(r.to_table); }
        const relatable = tables.filter((t) => t.kind !== "calculation_group" && t.kind !== "field_parameter");
        const colInfo = (t, c) => ({ table: t.name, column: c.name, dataType: String(value(c, "DataType") || ""),
            isKey: !!(value(c, "IsKey") || value(c, "IsUnique")) });
        const byName = new Map();
        for (const t of relatable) {
            for (const c of liveColumns(t)) {
                if (/^RowNumber/i.test(c.name)) continue;
                const k = c.name.toLowerCase();
                if (!byName.has(k)) byName.set(k, []);
                byName.get(k).push(colInfo(t, c));
            }
        }
        const out = [];
        for (const t of relatable) {
            if (related.has(t.name)) continue;
            const suggestions = [];
            for (const c of liveColumns(t)) {
                if (/^RowNumber/i.test(c.name)) continue;
                const self = colInfo(t, c);
                for (const o of byName.get(c.name.toLowerCase()) || []) {
                    if (o.table === t.name) continue;
                    if (self.dataType && o.dataType && self.dataType.toLowerCase() !== o.dataType.toLowerCase()) continue;
                    suggestions.push({ column: c.name, otherTable: o.table, otherColumn: o.column, dataType: self.dataType || o.dataType, otherIsKey: o.isKey });
                }
            }
            if (suggestions.length) {
                suggestions.sort((a, b) => Number(b.otherIsKey) - Number(a.otherIsKey) || cmpText(a.otherTable, b.otherTable) || cmpText(a.column, b.column));
                out.push({ table: t.name, suggestions });
            }
        }
        return out.sort((a, b) => cmpText(a.table, b.table));
    }
    // Force-directed layout keeping related tables together; unrelated ones are
    // stacked to the right. Panels start collapsed.
    function rdLayout(tables, rels, relCols) {
        const set = new Set(tables);
        const collapsedH = (t) => RD.HEADER_H + Math.max(1, (relCols[t] || []).length) * RD.ROW_H + RD.BODY_PAD;
        const seen = new Set(), edges = [];
        for (const r of rels) {
            if (r.from_table === r.to_table || !set.has(r.from_table) || !set.has(r.to_table)) continue;
            const k = rdPair(r.from_table, r.to_table);
            if (seen.has(k)) continue;
            seen.add(k);
            edges.push([r.from_table, r.to_table]);
        }
        const degree = {};
        tables.forEach((t) => { degree[t] = 0; });
        edges.forEach(([a, b]) => { degree[a]++; degree[b]++; });
        const connected = tables.filter((t) => degree[t] > 0), isolated = tables.filter((t) => !degree[t]);
        const layout = {}, pos = {}, n = connected.length;
        connected.forEach((t, i) => {
            const ang = (2 * Math.PI * i) / Math.max(1, n), radius = n > 1 ? n * 0.6 : 0.001;
            pos[t] = { x: Math.cos(ang) * radius, y: Math.sin(ang) * radius };
        });
        if (n > 1) {
            const k = 1.1;
            let temp = n * 0.5;
            for (let it = 0; it < 500; it++) {
                const disp = {};
                connected.forEach((t) => { disp[t] = { x: 0, y: 0 }; });
                for (let i = 0; i < n; i++) {
                    for (let j = i + 1; j < n; j++) {
                        const u = connected[i], v = connected[j];
                        const dx = pos[u].x - pos[v].x, dy = pos[u].y - pos[v].y, dist = Math.hypot(dx, dy) || 0.01;
                        const f = (k * k) / dist;
                        disp[u].x += (dx / dist) * f; disp[u].y += (dy / dist) * f;
                        disp[v].x -= (dx / dist) * f; disp[v].y -= (dy / dist) * f;
                    }
                }
                for (const [a, b] of edges) {
                    const dx = pos[a].x - pos[b].x, dy = pos[a].y - pos[b].y, dist = Math.hypot(dx, dy) || 0.01;
                    const f = (dist * dist) / k;
                    disp[a].x -= (dx / dist) * f; disp[a].y -= (dy / dist) * f;
                    disp[b].x += (dx / dist) * f; disp[b].y += (dy / dist) * f;
                }
                for (const t of connected) {
                    const d = disp[t], dl = Math.hypot(d.x, d.y) || 0.01;
                    pos[t].x += (d.x / dl) * Math.min(dl, temp);
                    pos[t].y += (d.y / dl) * Math.min(dl, temp);
                }
                temp = Math.max(0.01, temp * 0.985);
            }
        }
        if (n) {
            const minX = Math.min(...connected.map((t) => pos[t].x)), minY = Math.min(...connected.map((t) => pos[t].y));
            for (const t of connected) layout[t] = { x: (pos[t].x - minX) * 150 + 40, y: (pos[t].y - minY) * 150 + 40, w: RD.W, h: RD.H, collapsed: true };
            for (let pass = 0; pass < 80; pass++) {
                let moved = false;
                for (let i = 0; i < n; i++) {
                    for (let j = i + 1; j < n; j++) {
                        const A = layout[connected[i]], B = layout[connected[j]];
                        const ah = collapsedH(connected[i]), bh = collapsedH(connected[j]);
                        const ox = Math.min(A.x + A.w, B.x + B.w) - Math.max(A.x, B.x) + 28;
                        const oy = Math.min(A.y + ah, B.y + bh) - Math.max(A.y, B.y) + 28;
                        if (ox > 0 && oy > 0) {
                            moved = true;
                            if (ox < oy) { const s = ox / 2; if (A.x <= B.x) { A.x -= s; B.x += s; } else { A.x += s; B.x -= s; } }
                            else { const s = oy / 2; if (A.y <= B.y) { A.y -= s; B.y += s; } else { A.y += s; B.y -= s; } }
                        }
                    }
                }
                if (!moved) break;
            }
            const sx = Math.min(...connected.map((t) => layout[t].x)), sy = Math.min(...connected.map((t) => layout[t].y));
            for (const t of connected) { layout[t].x += 40 - sx; layout[t].y += 40 - sy; }
        }
        if (isolated.length) {
            if (!n) {
                const cols = Math.max(1, Math.ceil(Math.sqrt(isolated.length)));
                const rowH = Math.max(...isolated.map(collapsedH)) + 28;
                isolated.forEach((t, i) => { layout[t] = { x: 40 + (i % cols) * (RD.W + 40), y: 40 + Math.floor(i / cols) * rowH, w: RD.W, h: RD.H, collapsed: true }; });
            } else {
                const x = Math.max(40, ...connected.map((t) => layout[t].x + layout[t].w)) + 80;
                let y = 40;
                for (const t of isolated) { layout[t] = { x, y, w: RD.W, h: RD.H, collapsed: true }; y += collapsedH(t) + 28; }
            }
        }
        return layout;
    }

    function openRelationshipDiagram(initialFocus) {
        const m = toolWindow("Relationship diagram", "network", "slls-sx-rd-body");
        let focused = new Set(initialFocus ? [initialFocus] : []);
        let zoom = 1;
        const hl = { mm: false, bidi: false, limited: false, nonInt: false };
        let sidePanel = null;
        let reverse = false;
        let listFilter = "";
        let sidebarW = 256;
        let sidebarCollapsed = false;
        let inferOn = false, inferLoading = false, inferError = null;
        let inferMap = new Map();
        let panels = {};
        let zOrder = [];
        let layoutSig = null;
        let hoveredKey = null;
        let g = null;
        let els = null;
        let drag = null;
        const write = () => mode === "write" && !saving;

        function derive() {
            const tables = savedTables();
            const byName = new Map(tables.map((t) => [t.name, t]));
            const rels = liveRelationships().filter((r) => byName.has(r.from_table) && byName.has(r.to_table));
            const relCols = {}, neighbors = {};
            for (const t of tables) { relCols[t.name] = new Set(); neighbors[t.name] = new Set(); }
            for (const r of rels) {
                relCols[r.from_table].add(r.from_column);
                relCols[r.to_table].add(r.to_column);
                if (r.from_table !== r.to_table) { neighbors[r.from_table].add(r.to_table); neighbors[r.to_table].add(r.from_table); }
            }
            for (const k of Object.keys(relCols)) relCols[k] = [...relCols[k]].sort(cmpText);
            const names = tables.slice().sort(byLabel).map((t) => t.name);
            let visible = names;
            if (focused.size) {
                const vis = new Set();
                for (const seed of focused) { if (!byName.has(seed)) continue; vis.add(seed); for (const nb of neighbors[seed]) vis.add(nb); }
                visible = names.filter((t) => vis.has(t));
            }
            const keyRole = {}, nonInt = {};
            for (const r of rels) {
                const set = (t, c, role) => { const m2 = (keyRole[t] = keyRole[t] || {}); if (m2[c] !== "pk") m2[c] = role; };
                set(r.from_table, r.from_column, rdMark(r.from_card, "*") === "1" ? "pk" : "fk");
                set(r.to_table, r.to_column, rdMark(r.to_card, "1") === "1" ? "pk" : "fk");
                for (const [t, c] of [[r.from_table, r.from_column], [r.to_table, r.to_column]]) {
                    const dt = columnType(t, c);
                    if (dt && dt !== "Int64") (nonInt[t] = nonInt[t] || new Set()).add(c);
                }
            }
            const srcKeys = {}, order = [];
            for (const t of tables) { const k = rdSourceKey(t); if (k) { srcKeys[t.name] = k; if (!order.includes(k)) order.push(k); } }
            const colorOf = {};
            if (order.length > 1) for (const [t, k] of Object.entries(srcKeys)) colorOf[t] = order.indexOf(k) % RD_SOURCE_COLORS;
            return { tables, byName, rels, relCols, neighbors, names, visible, keyRole, nonInt,
                legend: order.length > 1 ? order.map((k, i) => ({ label: rdSourceLabel(k), color: i % RD_SOURCE_COLORS })) : [],
                colorOf, snowflake: rdSnowflake(rels), missing: rdMissing(tables, rels) };
        }
        const panelH = (t) => {
            const p = panels[t];
            return p.collapsed ? RD.HEADER_H + Math.max(1, (g.relCols[t] || []).length) * RD.ROW_H + RD.BODY_PAD : p.h;
        };
        function ensureLayout() {
            const sig = g.visible.join("\u0001");
            if (sig !== layoutSig) {
                panels = rdLayout(g.visible, g.rels, g.relCols);
                zOrder = g.visible.slice();
                layoutSig = sig;
            }
            for (const t of g.visible) if (!panels[t]) { panels[t] = { x: 40, y: 40, w: RD.W, h: RD.H, collapsed: true }; zOrder.push(t); }
        }
        function canvasSize() {
            let w = 1000, hh = 700;
            for (const t of g.visible) { const p = panels[t]; w = Math.max(w, p.x + p.w); hh = Math.max(hh, p.y + panelH(t)); }
            return { w: w + RD.MARGIN, h: hh + RD.MARGIN };
        }
        function connectors() {
            const vis = new Set(g.visible);
            return g.rels.filter((r) => vis.has(r.from_table) && vis.has(r.to_table)).map((r) => {
                const a = panels[r.from_table], b = panels[r.to_table];
                const aH = panelH(r.from_table), bH = panelH(r.to_table);
                const p1 = rdEdgePoint(a.x, a.y, a.w, aH, b.x + b.w / 2, b.y + bH / 2);
                const p2 = rdEdgePoint(b.x, b.y, b.w, bH, a.x + a.w / 2, a.y + aH / 2);
                const fromMark = rdMark(r.from_card, "*"), toMark = rdMark(r.to_card, "1");
                const mm = fromMark === "*" && toMark === "*";
                const fi = rdIslands(g.byName.get(r.from_table)), ti = rdIslands(g.byName.get(r.to_table));
                const crosses = ![...fi].some((x) => ti.has(x));
                return { r, key: r.key, p1, p2, fromMark, toMark, mm, bidi: /both/i.test(r.cross), limited: mm || crosses };
            });
        }
        function paintEdges() {
            if (!els) return;
            const size = canvasSize();
            els.sizer.style.width = size.w * zoom + "px";
            els.sizer.style.height = size.h * zoom + "px";
            els.layer.style.width = size.w + "px";
            els.layer.style.height = size.h + "px";
            els.svg.setAttribute("width", size.w);
            els.svg.setAttribute("height", size.h);
            let out = "";
            for (const c of connectors()) {
                const dx = c.p2.x - c.p1.x, dy = c.p2.y - c.p1.y, len = Math.hypot(dx, dy) || 1, ux = dx / len, uy = dy / len;
                const off = Math.min(22, len / 2 - 2);
                const sf = sidePanel === "snowflake" && g.snowflake.pairs.has(rdPair(c.r.from_table, c.r.to_table));
                const cls = ["slls-sx-rd-edge", c.key === hoveredKey ? "hov" : "", hl.mm && c.mm ? "mm" : "", hl.bidi && c.bidi ? "bidi" : "",
                    hl.limited && c.limited ? "lim" : "", sf ? "sf" : "", c.r.active ? "" : "inactive", c.r.isNew ? "new" : ""].filter(Boolean).join(" ");
                const strong = cls.includes(" hov") || cls.includes(" mm") || cls.includes(" bidi") || cls.includes(" lim") || cls.includes(" sf");
                const size2 = strong ? 8 : 7, aOff = Math.min(off + 16, len / 2);
                const toHead = [c.p2.x - ux * aOff, c.p2.y - uy * aOff, ux, uy];
                const fromHead = [c.p1.x + ux * aOff, c.p1.y + uy * aOff, -ux, -uy];
                const heads = c.bidi ? [fromHead, toHead] : [reverse ? fromHead : toHead];
                const mk = (x, y, mark) => `<g class="card"><circle cx="${x}" cy="${y}" r="10"/>`
                    + `<text x="${x}" y="${y}" dy="${mark === "*" ? "0.5em" : "0.35em"}" text-anchor="middle">${mark}</text></g>`;
                const title = rdPath(c.r) + (c.r.active ? "" : " (inactive)");
                out += `<g class="${cls}" aria-label="${esc(title)}">`
                    + `<line class="ln" x1="${c.p1.x}" y1="${c.p1.y}" x2="${c.p2.x}" y2="${c.p2.y}"`
                    + ` stroke-width="${c.key === hoveredKey ? 3 : strong ? 2.5 : 1.5}"${c.r.active ? "" : ' stroke-dasharray="5 4"'}/>`
                    + heads.map((hd) => `<polygon class="arrow" points="${rdArrow(hd[0], hd[1], hd[2], hd[3], size2)}"/>`).join("")
                    + `<circle class="end" cx="${c.p1.x}" cy="${c.p1.y}" r="${strong ? 4 : 3}"/><circle class="end" cx="${c.p2.x}" cy="${c.p2.y}" r="${strong ? 4 : 3}"/>`
                    + mk(c.p1.x + ux * off, c.p1.y + uy * off, c.fromMark) + mk(c.p2.x - ux * off, c.p2.y - uy * off, c.toMark)
                    + `<line class="hit" data-key="${esc(c.key)}" x1="${c.p1.x}" y1="${c.p1.y}" x2="${c.p2.x}" y2="${c.p2.y}"/></g>`;
            }
            els.svg.innerHTML = out;
            paintEdgeTip();
        }
        function paintEdgeTip() {
            if (els.edgeTip) { els.edgeTip.remove(); els.edgeTip = null; }
            for (const p of els.panelEls.values()) for (const row of p.cols.values()) row.classList.remove("hl");
            const c = hoveredKey ? connectors().find((x) => x.key === hoveredKey) : null;
            if (!c) return;
            for (const [t, col] of [[c.r.from_table, c.r.from_column], [c.r.to_table, c.r.to_column]]) {
                const p = els.panelEls.get(t);
                const row = p && p.cols.get(col);
                if (row) row.classList.add("hl");
            }
            const tip = h("div", "slls-sx-rd-tip");
            const head = h("div", "slls-sx-rd-tip-head");
            head.append(iconEl("relationship", ""), h("span", "name", c.r.isNew ? "New relationship" : (c.r.node.name || "Relationship")));
            if (!c.r.active) head.appendChild(h("span", "slls-sx-pill", "Inactive"));
            tip.appendChild(head);
            tip.appendChild(h("div", "slls-sx-rd-tip-path", rdPath(c.r)));
            tip.appendChild(rdTipGrid([["Cardinality", `${c.fromMark === "*" ? "Many" : "One"} \u2192 ${c.toMark === "*" ? "Many" : "One"}`],
                ["Cross filter", c.r.cross], ["Strength", c.limited ? "Limited (weak)" : "Strong"], ["Security", c.r.security],
                ["Assume referential integrity", c.r.rely ? "Yes" : "No"], ["Active", c.r.active ? "Yes" : "No"]]));
            tip.style.left = ((c.p1.x + c.p2.x) / 2) * zoom + 14 + "px";
            tip.style.top = ((c.p1.y + c.p2.y) / 2) * zoom + 14 + "px";
            els.sizer.appendChild(tip);
            els.edgeTip = tip;
        }
        function showTableTip(t) {
            hideTableTip();
            const node = g.byName.get(t), p = panels[t];
            if (!node || !p) return;
            const tip = h("div", "slls-sx-rd-tip");
            const head = h("div", "slls-sx-rd-tip-head");
            head.append(iconEl(tableIcon(node), ""), h("span", "name", effName(node)), h("span", "kind", TABLE_KIND_LABEL[node.kind] || "Table"));
            tip.appendChild(head);
            const parts = node.partitions || [];
            const key = rdSourceKey(node);
            const withEntity = parts.find((x) => originalValue(x, "Source.EntityName"));
            const entity = withEntity ? [originalValue(withEntity, "Source.SchemaName"), originalValue(withEntity, "Source.EntityName")].filter(Boolean).join(".") : "";
            const counts = new Map();
            for (const x of parts) { const label = (RD_MODE_ICON[String(x.mode || "").toLowerCase()] || [x.mode || "Unknown"])[0]; counts.set(label, (counts.get(label) || 0) + 1); }
            const modeText = !parts.length ? "\u2014" : counts.size === 1 ? [...counts.keys()][0] : [...counts].map(([k, n]) => `${k} \u00d7 ${n}`).join(", ");
            const rows = [["Source", key ? rdSourceLabel(key) : (parts[0] ? parts[0].source_type : "\u2014")]];
            if (entity) rows.push(["Object", entity]);
            rows.push(["Query mode", modeText], ["Partitions", String(parts.length)]);
            tip.appendChild(rdTipGrid(rows));
            tip.style.left = (p.x + p.w) * zoom + 10 + "px";
            tip.style.top = p.y * zoom + "px";
            els.sizer.appendChild(tip);
            els.tableTip = tip;
        }
        function hideTableTip() { if (els && els.tableTip) { els.tableTip.remove(); els.tableTip = null; } }
        function placePanel(t) {
            const pe = els.panelEls.get(t), p = panels[t];
            if (!pe) return;
            pe.el.style.left = p.x + "px";
            pe.el.style.top = p.y + "px";
            pe.el.style.width = p.w + "px";
            pe.el.style.height = panelH(t) + "px";
        }
        function bringToFront(t) {
            zOrder = [...zOrder.filter((x) => x !== t), t];
            const pe = els.panelEls.get(t);
            if (pe) els.layer.appendChild(pe.el);
        }
        function startDrag(ev, t, kind) {
            ev.preventDefault();
            ev.stopPropagation();
            bringToFront(t);
            const p = panels[t];
            drag = { t, kind, x0: ev.clientX, y0: ev.clientY, px: p.x, py: p.y, pw: p.w, ph: p.h };
            const move = (e) => {
                const dx = (e.clientX - drag.x0) / zoom, dy = (e.clientY - drag.y0) / zoom;
                if (drag.kind === "move") { p.x = Math.max(0, drag.px + dx); p.y = Math.max(0, drag.py + dy); }
                else { p.w = Math.max(RD.MIN_W, drag.pw + dx); p.h = Math.max(RD.MIN_H, drag.ph + dy); }
                placePanel(t);
                paintEdges();
            };
            const up = () => { drag = null; window.removeEventListener("pointermove", move); window.removeEventListener("pointerup", up); };
            window.addEventListener("pointermove", move);
            window.addEventListener("pointerup", up);
        }
        let colDrag = null;
        function columnRow(t, c, flagged) {
            const role = (g.keyRole[t] || {})[c.name];
            const row = h("div", "slls-sx-rd-col" + (flagged ? " flagged" : ""));
            row.style.height = RD.ROW_H + "px";
            row.appendChild(iconEl(role ? "key_round" : (c.column_type === "Calculated" ? "calculated_column" : "column"), role ? "key " + role : ""));
            row.appendChild(h("span", "name", effName(c)));
            const dt = String(value(c, "DataType") || "");
            if (dt) row.appendChild(h("span", "dt", dt));
            row.title = effName(c);
            if (write()) {
                row.draggable = true;
                row.title = `${effName(c)} \u2014 drag onto another table to create a relationship`;
                row.addEventListener("dragstart", (ev) => {
                    colDrag = { fromTable: t, fromColumn: c.name, dataType: dt };
                    ev.dataTransfer.effectAllowed = "link";
                    ev.dataTransfer.setData("text/plain", `${t}[${c.name}]`);
                });
                row.addEventListener("dragend", () => { colDrag = null; els.layer.querySelectorAll(".drop").forEach((x) => x.classList.remove("drop")); });
            }
            return row;
        }
        function linkDialog(spec) {
            const d = modal("Create relationship", "relationship");
            const target = g.byName.get(spec.toTable);
            const cols = liveColumns(target).sort(byLabel);
            const lc = spec.fromColumn.toLowerCase();
            const dtOf = (c) => String(value(c, "DataType") || "");
            const guess = cols.find((c) => c.name.toLowerCase() === lc && (!spec.dataType || dtOf(c) === spec.dataType))
                || cols.find((c) => c.name.toLowerCase() === lc) || (spec.dataType && cols.find((c) => dtOf(c) === spec.dataType)) || cols[0];
            const from = h("div", "slls-sx-rd-link-from");
            from.append(h("span", "slls-sx-tool-label", "From"),
                h("span", "slls-sx-mono", `'${tableDisplayName(spec.fromTable)}'[${columnDisplayName(spec.fromTable, spec.fromColumn)}]`));
            if (spec.dataType) from.appendChild(h("span", "slls-sx-pill", spec.dataType));
            d.body.appendChild(from);
            let toColumn = guess ? guess.name : "";
            let card = "manyOne";
            d.body.appendChild(field(`To column in '${effName(target)}'`, selectEl(columnOptions(spec.toTable), toColumn, (v) => { toColumn = v; }, "To column")));
            d.body.appendChild(field("Cardinality", selectEl([["manyOne", "Many to one (*:1)"], ["oneMany", "One to many (1:*)"], ["oneOne", "One to one (1:1)"],
                ["manyMany", "Many to many (*:*)"]], card, (v) => { card = v; }, "Cardinality")));
            d.body.appendChild(h("p", "slls-sx-dlm-note", "Staged as a pending change \u2014 commit with Save. The target column is "
                + "pre-selected by matching name and data type; adjust it or the cardinality above."));
            const foot = h("div", "slls-sx-modal-foot");
            const ok = button("slls-sx-btn primary", "Create", "plus", () => {
                if (!toColumn) return;
                const [fc, tc] = { manyOne: ["Many", "One"], oneMany: ["One", "Many"], oneOne: ["One", "One"], manyMany: ["Many", "Many"] }[card];
                d.close();
                stageRelationship({ fromTable: spec.fromTable, fromColumn: spec.fromColumn, toTable: spec.toTable, toColumn, fromCardinality: fc, toCardinality: tc });
            });
            ok.disabled = !cols.length;
            foot.append(button("slls-sx-btn", "Cancel", "", () => d.close()), ok);
            d.box.appendChild(foot);
        }
        function drawPanel(t) {
            const node = g.byName.get(t), p = panels[t];
            const relCols = g.relCols[t] || [];
            const el = h("div", "slls-sx-rd-panel" + (p.collapsed ? " collapsed" : "")
                + (sidePanel === "snowflake" && g.snowflake.tables.has(t) ? " snow" : "")
                + (g.colorOf[t] !== undefined ? ` src src-${g.colorOf[t]}` : ""));
            const head = h("div", "slls-sx-rd-head");
            head.style.height = RD.HEADER_H + "px";
            const stop = (b) => { b.addEventListener("pointerdown", (e) => e.stopPropagation()); return b; };
            head.appendChild(stop(caretBtn(!p.collapsed, () => { p.collapsed = !p.collapsed; drawPanels(); })));
            head.appendChild(iconEl(tableIcon(node), "kind"));
            const name = h("span", "name", effName(node));
            head.appendChild(name);
            const modes = [...new Set((node.partitions || []).map((x) => String(x.mode || "")).filter((x) => x && x.toLowerCase() !== "default"))].sort();
            for (const mo of modes) {
                const meta = RD_MODE_ICON[mo.toLowerCase()] || [mo, "database"];
                const i = iconEl(meta[1], "mode");
                i.title = `Storage mode: ${meta[0]}`;
                i.setAttribute("aria-label", i.title);
                head.appendChild(i);
            }
            head.appendChild(stop(button("slls-sx-iconbtn", "", "focus", () => { focused = new Set([t]); draw(); }, "Focus on this table and its related tables")));
            head.addEventListener("pointerdown", (e) => startDrag(e, t, "move"));
            head.addEventListener("mouseenter", () => { if (!drag) showTableTip(t); });
            head.addEventListener("mouseleave", hideTableTip);
            el.appendChild(head);
            const body = h("div", "slls-sx-rd-cols");
            const cols = new Map();
            const flagged = hl.nonInt ? g.nonInt[t] : null;
            const list = p.collapsed ? relCols.map((c) => index.get(keyOf("column", t, "", c))).filter(Boolean) : liveColumns(node);
            if (!list.length) body.appendChild(h("div", "slls-sx-rd-none", p.collapsed ? "No relationship columns" : "No columns"));
            for (const c of list) {
                const row = columnRow(t, c, flagged && flagged.has(c.name));
                cols.set(c.name, row);
                body.appendChild(row);
            }
            el.appendChild(body);
            if (!p.collapsed) {
                const rz = h("div", "slls-sx-rd-resize");
                rz.title = "Resize";
                rz.innerHTML = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><path d="M11 15L15 11M7 15L15 7"/></svg>';
                rz.addEventListener("pointerdown", (e) => startDrag(e, t, "resize"));
                el.appendChild(rz);
            }
            el.addEventListener("pointerdown", () => bringToFront(t));
            if (write()) {
                el.addEventListener("dragover", (ev) => {
                    if (colDrag && colDrag.fromTable !== t) { ev.preventDefault(); ev.dataTransfer.dropEffect = "link"; el.classList.add("drop"); }
                });
                el.addEventListener("dragleave", () => el.classList.remove("drop"));
                el.addEventListener("drop", (ev) => {
                    el.classList.remove("drop");
                    if (!colDrag || colDrag.fromTable === t) return;
                    ev.preventDefault();
                    const spec = Object.assign({ toTable: t }, colDrag);
                    colDrag = null;
                    linkDialog(spec);
                });
            }
            els.panelEls.set(t, { el, cols });
            els.layer.appendChild(el);
            placePanel(t);
        }
        function drawPanels() {
            els.layer.querySelectorAll(".slls-sx-rd-panel").forEach((x) => x.remove());
            els.panelEls = new Map();
            hideTableTip();
            for (const t of zOrder.filter((x) => g.visible.includes(x))) drawPanel(t);
            paintEdges();
        }
        function drawSidebar(main) {
            const side = h("div", "slls-sx-rd-side" + (sidebarCollapsed ? " collapsed" : ""));
            if (!sidebarCollapsed) side.style.width = sidebarW + "px";
            const reverseBtn = (labelled) => {
                const b = labelled ? switchEl(reverse, "Reverse arrow direction", () => { reverse = !reverse; draw(); }, "Reverse arrows")
                    : button("slls-sx-iconbtn" + (reverse ? " on" : ""), "", "arrow_left_right", () => { reverse = !reverse; draw(); }, "Reverse arrow direction");
                b.title = reverse ? "Direction arrows point to \u2192 from. Click to point from \u2192 to." : "Direction arrows point from \u2192 to. Click to reverse (to \u2192 from).";
                return b;
            };
            const reset = () => button("slls-sx-iconbtn", "", "reset", () => { layoutSig = null; draw(); }, "Reset layout");
            if (sidebarCollapsed) {
                side.append(button("slls-sx-iconbtn", "", "panel_expand", () => { sidebarCollapsed = false; draw(); }, "Expand table list"),
                    h("span", "slls-sx-rd-rail", "Tables"), reverseBtn(false), reset());
                main.appendChild(side);
                return;
            }
            const head = h("div", "slls-sx-rd-side-head");
            head.appendChild(h("span", "slls-sx-pane-label", "Tables"));
            if (focused.size) head.appendChild(button("slls-sx-btn small", `Clear (${focused.size})`, "", () => { focused = new Set(); draw(); }));
            head.append(reset(), button("slls-sx-iconbtn", "", "panel_collapse", () => { sidebarCollapsed = true; draw(); }, "Collapse table list"));
            side.appendChild(head);
            const listEl = h("div", "slls-sx-rd-tables");
            listEl.dataset.scroll = "tables";
            const paint = () => {
                clear(listEl);
                const q = listFilter.trim().toLowerCase();
                const names = g.names.filter((t) => !q || effName(g.byName.get(t)).toLowerCase().includes(q));
                if (!names.length) listEl.appendChild(h("div", "slls-sx-empty", "No tables match."));
                for (const t of names) {
                    const node = g.byName.get(t);
                    const row = h("div", "slls-sx-rd-trow" + (focused.has(t) ? " on" : ""));
                    const cb = h("input", "slls-sx-tool-check");
                    cb.type = "checkbox";
                    cb.checked = focused.has(t);
                    cb.setAttribute("aria-label", `Focus on ${effName(node)}`);
                    const toggle = () => { if (focused.has(t)) focused.delete(t); else focused.add(t); draw(); };
                    cb.addEventListener("change", toggle);
                    const ic = iconEl(tableIcon(node), "");
                    ic.title = TABLE_KIND_LABEL[node.kind] || "Table";
                    const nb = h("button", "name", effName(node));
                    nb.type = "button";
                    nb.title = effName(node);
                    nb.addEventListener("click", toggle);
                    row.append(cb, ic, nb, h("span", "meta", String(g.neighbors[t].size)),
                        button("slls-sx-iconbtn", "", "focus", () => { focused = new Set([t]); draw(); }, "Focus only on this table"));
                    listEl.appendChild(row);
                }
            };
            side.appendChild(searchBox("Search tables\u2026", listFilter, (v) => { listFilter = v; paint(); }));
            side.appendChild(h("p", "slls-sx-dlm-note", "Select tables to focus on them and their related tables."));
            side.appendChild(listEl);
            const foot = h("div", "slls-sx-rd-side-foot");
            foot.appendChild(reverseBtn(true));
            side.appendChild(foot);
            const grip = h("div", "slls-sx-rd-grip");
            grip.setAttribute("role", "separator");
            grip.setAttribute("aria-label", "Resize table list");
            grip.addEventListener("pointerdown", (ev) => {
                ev.preventDefault();
                const x0 = ev.clientX, w0 = side.getBoundingClientRect().width;
                const move = (e) => { sidebarW = Math.min(480, Math.max(200, w0 + e.clientX - x0)); side.style.width = sidebarW + "px"; };
                const up = () => { window.removeEventListener("pointermove", move); window.removeEventListener("pointerup", up); };
                window.addEventListener("pointermove", move);
                window.addEventListener("pointerup", up);
            });
            side.appendChild(grip);
            paint();
            main.appendChild(side);
        }
        function runInfer() {
            if (inferOn) { inferOn = false; draw(); return; }
            const pairs = [];
            for (const c of g.missing) for (const s of c.suggestions.slice(0, 8)) {
                pairs.push({ from_table: c.table, from_column: s.column, to_table: s.otherTable, to_column: s.otherColumn });
            }
            if (!pairs.length) { inferOn = true; draw(); return; }
            inferLoading = true;
            inferError = null;
            draw();
            request("infer_relationships", { pairs }, (res) => {
                inferLoading = false;
                if (res.ok) {
                    inferMap = new Map();
                    (res.data.results || []).forEach((r, i) => {
                        const p = pairs[i];
                        if (p) inferMap.set(`${p.from_table}\u0001${p.from_column}\u0001${p.to_table}\u0001${p.to_column}`, r);
                    });
                    inferOn = true;
                } else inferError = res.error || "Inference failed.";
                if (m.overlay.isConnected) draw();
            });
        }
        function drawSidePanel(main) {
            if (!sidePanel) return;
            const panel = h("div", "slls-sx-rd-sidepanel");
            const head = h("div", "slls-sx-rd-sp-head");
            const snow = sidePanel === "snowflake";
            head.append(iconEl(snow ? "snowflake" : "lightbulb", ""), h("span", "title", snow ? "Snowflake schema" : "Missing relationships"),
                button("slls-sx-iconbtn", "", "close", () => { sidePanel = null; draw(); }, "Close"));
            panel.appendChild(head);
            const body = h("div", "slls-sx-rd-sp-body");
            body.dataset.scroll = "side";
            const tableBtn = (t) => {
                const node = g.byName.get(t);
                const b = h("button", "slls-sx-rd-tbtn");
                b.type = "button";
                b.title = `Focus ${effName(node)} on the canvas`;
                b.append(iconEl(tableIcon(node), ""), h("span", "", effName(node)));
                b.addEventListener("click", () => { focused = new Set([t]); draw(); });
                return b;
            };
            if (snow) {
                body.appendChild(h("p", "slls-sx-dlm-note", "Dimensions that relate to a further secondary dimension. Each chain "
                    + "follows one-to-many relationships from the fact through its normalised dimensions. The tables on these chains "
                    + "are outlined on the canvas."));
                if (!g.snowflake.chains.length) body.appendChild(h("div", "slls-sx-empty", "No snowflake patterns found \u2014 every dimension relates directly to a fact (a star schema)."));
                for (const chain of g.snowflake.chains) {
                    const item = h("div", "slls-sx-rd-chain");
                    item.appendChild(h("div", "roles", chain.map((_, i) => (i === 0 ? "Fact" : i === chain.length - 1 ? "Secondary dim" : "Dim")).join(" \u2192 ")));
                    const path = h("div", "path");
                    chain.forEach((t, i) => { if (i) path.appendChild(h("span", "arrow", "\u2192")); path.appendChild(tableBtn(t)); });
                    item.appendChild(path);
                    body.appendChild(item);
                }
            } else {
                body.appendChild(h("p", "slls-sx-dlm-note", "Tables with no relationship that share a column (same name and data "
                    + "type) with another table. Calculation groups and field parameters are excluded."));
                if (g.missing.length) {
                    const ib = button("slls-sx-btn small" + (inferOn ? " on" : ""), inferLoading ? "Inferring with DAX\u2026"
                        : inferOn ? "Hide inferred direction & cardinality" : "Infer direction & cardinality (DAX)", inferLoading ? "refresh" : "wand", runInfer,
                        "Query the model with DAX to infer each relationship's direction and from/to cardinality from real column uniqueness");
                    if (inferLoading) ib.querySelector("svg").classList.add("slls-sx-spin");
                    ib.disabled = inferLoading;
                    ib.setAttribute("aria-pressed", String(inferOn));
                    body.appendChild(ib);
                    if (inferError) body.appendChild(h("div", "slls-sx-tool-error", inferError));
                } else body.appendChild(h("div", "slls-sx-empty", "No isolated tables have a plausible relationship."));
                for (const c of g.missing) {
                    const item = h("div", "slls-sx-rd-cand");
                    const top = tableBtn(c.table);
                    top.appendChild(h("span", "meta", `${c.suggestions.length} match${c.suggestions.length === 1 ? "" : "es"}`));
                    item.appendChild(top);
                    for (const s of c.suggestions.slice(0, 8)) {
                        const li = h("div", "slls-sx-rd-sug");
                        const line = h("div", "line");
                        line.append(h("span", "slls-sx-mono", `[${columnDisplayName(c.table, s.column)}]`), iconEl("link", ""),
                            h("span", "slls-sx-mono", `'${tableDisplayName(s.otherTable)}'[${columnDisplayName(s.otherTable, s.otherColumn)}]`));
                        if (s.dataType) line.appendChild(h("span", "slls-sx-pill", s.dataType));
                        if (s.otherIsKey) line.appendChild(h("span", "slls-sx-pill new", "key"));
                        li.appendChild(line);
                        const inf = inferOn ? inferMap.get(`${c.table}\u0001${s.column}\u0001${s.otherTable}\u0001${s.otherColumn}`) : null;
                        if (inf && inf.error) li.appendChild(h("div", "slls-sx-tool-error", inf.error));
                        else if (inf) {
                            const res = h("div", "inferred");
                            const stat = (t, c, d, n, u) => `'${t}'[${c}]: ${d} distinct / ${n} rows (${u ? "unique" : "not unique"})`;
                            res.title = `Measured uniqueness \u2014 ${stat(inf.from_table, inf.from_column, inf.from_distinct, inf.from_rows, inf.from_unique)}; `
                                + `${stat(inf.to_table, inf.to_column, inf.to_distinct, inf.to_rows, inf.to_unique)}.`;
                            res.append(h("span", "slls-sx-mono", `'${tableDisplayName(inf.from_table)}'[${columnDisplayName(inf.from_table, inf.from_column)}] (${inf.from_cardinality})`),
                                h("span", "", "\u2192"),
                                h("span", "slls-sx-mono", `'${tableDisplayName(inf.to_table)}'[${columnDisplayName(inf.to_table, inf.to_column)}] (${inf.to_cardinality})`),
                                h("span", "slls-sx-pill", inf.label));
                            if (!inf.confident) res.appendChild(h("span", "slls-sx-pill warn", "unverified"));
                            if (write()) {
                                res.appendChild(button("slls-sx-btn small primary", "Create", "plus", () => stageRelationship({
                                    fromTable: inf.from_table, fromColumn: inf.from_column, toTable: inf.to_table, toColumn: inf.to_column,
                                    fromCardinality: inf.from_cardinality, toCardinality: inf.to_cardinality,
                                }), "Create this relationship (staged \u2014 commit with Save)"));
                            }
                            li.appendChild(res);
                        }
                        item.appendChild(li);
                    }
                    if (c.suggestions.length > 8) item.appendChild(h("div", "slls-sx-rd-more", `+${c.suggestions.length - 8} more\u2026`));
                    body.appendChild(item);
                }
            }
            panel.appendChild(body);
            main.appendChild(panel);
        }
        function drawCanvas(main) {
            const wrapEl = h("div", "slls-sx-rd-canvas");
            const scroller = h("div", "slls-sx-rd-scroll");
            scroller.dataset.scroll = "canvas";
            const sizer = h("div", "slls-sx-rd-sizer");
            const layer = h("div", "slls-sx-rd-layer");
            layer.style.transform = `scale(${zoom})`;
            const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
            svg.setAttribute("class", "slls-sx-rd-svg");
            layer.appendChild(svg);
            sizer.appendChild(layer);
            scroller.appendChild(sizer);
            wrapEl.appendChild(scroller);
            els = { scroller, sizer, layer, svg, panelEls: new Map(), edgeTip: null, tableTip: null };
            svg.addEventListener("mouseover", (ev) => {
                const k = ev.target.getAttribute && ev.target.getAttribute("data-key");
                if (k && k !== hoveredKey) { hoveredKey = k; paintEdges(); }
            });
            svg.addEventListener("mouseout", (ev) => {
                const k = ev.target.getAttribute && ev.target.getAttribute("data-key");
                if (k && hoveredKey === k) { hoveredKey = null; paintEdges(); }
            });
            svg.addEventListener("contextmenu", (ev) => {
                const k = ev.target.getAttribute && ev.target.getAttribute("data-key");
                const r = k && g.rels.find((x) => x.key === k);
                if (!r || !write()) return;
                ev.preventDefault();
                showMenu(ev.clientX, ev.clientY, [[r.active ? "Make inactive" : "Make active", r.active ? "unlink" : "link",
                    () => setValue(r.node, "IsActive", !r.active, { label: "Active" })]]);
            });
            scroller.addEventListener("pointerdown", (ev) => {
                if (ev.target.closest(".slls-sx-rd-panel") || (ev.target.getAttribute && ev.target.getAttribute("data-key"))) return;
                ev.preventDefault();
                const x0 = ev.clientX, y0 = ev.clientY, l0 = scroller.scrollLeft, t0 = scroller.scrollTop;
                scroller.classList.add("panning");
                const move = (e) => { scroller.scrollLeft = l0 - (e.clientX - x0); scroller.scrollTop = t0 - (e.clientY - y0); };
                const up = () => { scroller.classList.remove("panning"); window.removeEventListener("pointermove", move); window.removeEventListener("pointerup", up); };
                window.addEventListener("pointermove", move);
                window.addEventListener("pointerup", up);
            });
            if (g.legend.length) {
                const legend = h("div", "slls-sx-rd-legend");
                const pill = h("div", "pill");
                pill.append(iconEl("database", ""), h("span", "", plural(g.legend.length, "source")));
                const dots = h("span", "dots");
                g.legend.slice(0, 8).forEach((s) => dots.appendChild(h("span", `sw src-${s.color}`)));
                pill.appendChild(dots);
                const full = h("div", "full");
                full.appendChild(h("div", "slls-sx-pane-label", "Sources"));
                for (const s of g.legend) {
                    const li = h("div", "item");
                    li.append(h("span", `sw src-${s.color}`), h("span", "label", s.label));
                    li.title = s.label;
                    full.appendChild(li);
                }
                legend.append(pill, full);
                wrapEl.appendChild(legend);
            }
            const zoomBox = h("div", "slls-sx-rd-zoom");
            const zin = h("input", "slls-sx-input");
            zin.value = String(Math.round(zoom * 100));
            zin.setAttribute("aria-label", "Zoom percentage");
            zin.title = "Enter a zoom percentage";
            const setZoom = (z) => {
                const keepX = (scroller.scrollLeft + scroller.clientWidth / 2) / zoom, keepY = (scroller.scrollTop + scroller.clientHeight / 2) / zoom;
                zoom = Math.min(2.5, Math.max(0.3, Math.round(z * 100) / 100));
                layer.style.transform = `scale(${zoom})`;
                zin.value = String(Math.round(zoom * 100));
                paintEdges();
                scroller.scrollLeft = keepX * zoom - scroller.clientWidth / 2;
                scroller.scrollTop = keepY * zoom - scroller.clientHeight / 2;
            };
            zin.addEventListener("keydown", (ev) => { if (ev.key === "Enter") zin.blur(); });
            zin.addEventListener("change", () => { const n = parseInt(zin.value, 10); if (Number.isFinite(n)) setZoom(n / 100); else zin.value = String(Math.round(zoom * 100)); });
            zoomBox.append(button("slls-sx-iconbtn", "", "zoom_in", () => setZoom(zoom * 1.2), "Zoom in"), zin, h("span", "", "%"),
                button("slls-sx-iconbtn", "", "zoom_out", () => setZoom(zoom / 1.2), "Zoom out"));
            wrapEl.appendChild(zoomBox);
            main.appendChild(wrapEl);
        }
        function chip(label, cls, on, onClick, title, count, icon) {
            const b = h("button", `slls-sx-rd-chip ${cls}` + (on ? " on" : ""));
            b.type = "button";
            b.title = title;
            b.setAttribute("aria-pressed", String(!!on));
            if (icon) b.appendChild(iconEl(icon, ""));
            else b.appendChild(h("span", "sw"));
            b.appendChild(document.createTextNode(label));
            if (count) b.appendChild(h("span", "count", String(count)));
            b.addEventListener("click", onClick);
            return b;
        }
        function draw() {
            g = derive();
            if (focused.size && ![...focused].some((t) => g.byName.has(t))) focused = new Set();
            ensureLayout();
            toolSubtitle(m, focused.size ? `Focused on ${plural(focused.size, "table")} \u00b7 showing ${g.visible.length} of ${g.names.length}`
                : `${plural(g.names.length, "table")} \u00b7 ${plural(g.rels.length, "relationship")}`);
            toolActions(m, [button("slls-sx-btn", "Relationship manager", "git_branch", () => openRelationshipManager(), "Open the relationship manager")]);
            keepScroll(m.body, () => {
                clear(m.body);
                const bar = h("div", "slls-sx-tool-toolbar slls-sx-rd-toolbar");
                if (focused.size) bar.appendChild(button("slls-sx-btn small", "Clear focus", "close", () => { focused = new Set(); draw(); }, "Show all tables"));
                const nonIntCount = Object.values(g.nonInt).reduce((s, x) => s + x.size, 0);
                const toggleHighlight = (flag, cls) => {
                    hl[flag] = !hl[flag];
                    paintEdges();
                    bar.querySelector("." + cls).classList.toggle("on", hl[flag]);
                };
                bar.append(
                    chip("Many-to-many", "mm", hl.mm, () => toggleHighlight("mm", "mm"), "Highlight many-to-many relationships"),
                    chip("Bi-directional", "bidi", hl.bidi, () => toggleHighlight("bidi", "bidi"), "Highlight bi-directional (BothDirections) relationships"),
                    chip("Limited", "lim", hl.limited, () => toggleHighlight("limited", "lim"),
                        "Highlight limited (weak) relationships: many-to-many cardinality or cross-island (different storage source groups)"),
                    chip("Non-Int64 keys", "nonint", hl.nonInt, () => { hl.nonInt = !hl.nonInt; draw(); },
                        "Highlight relationship columns whose data type is not Int64 (whole-number keys give the best storage and query performance)", nonIntCount),
                    chip("Snowflake", "sf", sidePanel === "snowflake", () => { sidePanel = sidePanel === "snowflake" ? null : "snowflake"; draw(); },
                        "Identify snowflake-schema chains (Fact \u2192 Dim \u2192 Secondary dim): colour the tables and list each pattern", g.snowflake.chains.length, "snowflake"),
                    chip("Find missing relationships", "miss", sidePanel === "missing", () => { sidePanel = sidePanel === "missing" ? null : "missing"; draw(); },
                        "Find tables that have no relationship but plausibly could (matching column name + data type)", g.missing.length, "lightbulb"));
                if (write()) bar.appendChild(h("span", "slls-sx-tool-count", "Drag a column onto another table to create a relationship \u00b7 right-click a line to (de)activate it"));
                m.body.appendChild(bar);
                const main = h("div", "slls-sx-rd-main");
                drawSidebar(main);
                drawCanvas(main);
                drawSidePanel(main);
                m.body.appendChild(main);
                drawPanels();
            });
        }
        registerTool(m, draw, () => toolActions(m, [m.actions.querySelector(".slls-sx-btn")]));
        draw();
    }
    // ------------------------------------------------------------------
    // Perspective editor
    // ------------------------------------------------------------------
    function openPerspectiveEditor() {
        const m = toolWindow("Perspective editor", "perspective");
        let active = null;
        let search = "";
        const openTables = new Set();
        const write = () => mode === "write" && !saving;
        const perspectives = () => [...(data.perspectives || []), ...newNodes("perspective", "")]
            .sort((a, b) => cmpText(effName(a), effName(b)));
        const current = () => perspectives().find((p) => p.key === active) || null;
        // Membership lists hold *saved* perspective names (renames apply last);
        // a staged new perspective is referenced by its create name.
        const memberName = (p) => (p.isNew ? createFor(p).name : p.name);
        const listOf = (node) => jsonValue(node, "Perspectives", []);
        const savedListOf = (node) => (node.isNew ? [] : JSON.parse(originalValue(node, "Perspectives") || "[]"));
        const fieldsOf = (t) => {
            const byN = (a, b) => cmpText(effName(a), effName(b));
            return [
                ...[...(t.columns || []), ...newNodes("column", t.key)].filter((n) => !isDeleted(n)).sort(byN).map((n) => ["Column", n]),
                ...[...(t.measures || []), ...newNodes("measure", t.key)].filter((n) => !isDeleted(n)).sort(byN).map((n) => ["Measure", n]),
                ...(t.hierarchies || []).filter((n) => !isDeleted(n)).sort(byN).map((n) => ["Hierarchy", n]),
            ];
        };
        const withName = (node, pn, on) => {
            const set = new Set(listOf(node));
            if (on) set.add(pn); else set.delete(pn);
            return JSON.stringify([...set].sort());
        };
        // Stages field memberships; a table belongs while any of its fields do.
        function stageMembership(table, fieldNodes, on, pn) {
            const items = fieldNodes.map((n) => ({ node: n, prop: "Perspectives", value: withName(n, pn, on), label: "Perspectives" }));
            const toggled = new Set(fieldNodes.map((n) => n.key));
            const anyLeft = fieldsOf(table).some(([, n]) => (toggled.has(n.key) ? on : listOf(n).includes(pn)));
            const tableIn = fieldsOf(table).length ? anyLeft : on;
            items.push({ node: table, prop: "Perspectives", value: withName(table, pn, tableIn), label: "Perspectives" });
            stageMany(items);
        }
        function counts(pn) {
            let tables = 0, fields = 0;
            for (const t of savedTables()) {
                if (listOf(t).includes(pn)) tables += 1;
                for (const [, n] of fieldsOf(t)) if (listOf(n).includes(pn)) fields += 1;
            }
            return { tables, fields };
        }
        // Renaming a staged perspective renames it in every staged membership.
        function renameNew(p, name) {
            const c = createFor(p);
            const old = c.name;
            if (!name.trim() || old === name) return;
            pushUndo("rename:" + c.id);
            c.name = name;
            const swap = (raw) => JSON.stringify(JSON.parse(raw || "[]").map((x) => (x === old ? name : x)).sort());
            for (const e of Object.values(staging.edits)) {
                if (e.property !== "Perspectives") continue;
                e.newValue = swap(e.newValue);
                if (e.userValue !== undefined) e.userValue = swap(e.userValue);
            }
            for (const x of staging.creates) if (x.props.Perspectives) x.props.Perspectives = swap(x.props.Perspectives);
            afterChange({ keepDetail: true });
        }
        function removeNew(p) {
            const c = createFor(p);
            const pn = c.name;
            pushUndo("");
            staging.creates = staging.creates.filter((x) => x.id !== c.id);
            for (const [k, e] of Object.entries(staging.edits)) {
                if (e.property !== "Perspectives") continue;
                const scrub = (raw) => JSON.stringify(JSON.parse(raw || "[]").filter((x) => x !== pn));
                e.newValue = scrub(e.newValue);
                if (e.userValue !== undefined) e.userValue = scrub(e.userValue);
                if (same(e.newValue, e.oldValue)) delete staging.edits[k];
            }
            afterChange();
        }
        function drawSidebar() {
            const side = h("div", "slls-sx-fpm-side");
            const head = h("div", "slls-sx-fpm-side-head");
            const list = perspectives();
            head.appendChild(h("span", "slls-sx-pane-label", `Perspectives (${list.length})`));
            if (write()) {
                head.appendChild(button("slls-sx-iconbtn", "", "plus", () => {
                    addObject("perspective", null);
                    active = selectedKey;
                    search = "";
                    draw();
                }, "Create a new perspective"));
            }
            side.appendChild(head);
            const items = h("div", "slls-sx-fpm-list");
            items.dataset.scroll = "perspectives";
            if (!list.length) items.appendChild(h("div", "slls-sx-empty", "This model has no perspectives." + (write() ? " Use + to create one." : "")));
            for (const p of list) {
                const deleted = isDeleted(p);
                const b = h("button", "slls-sx-fpm-item slls-sx-sm-role" + (active === p.key ? " active" : "") + (p.isNew ? " new" : "") + (deleted ? " deleted" : ""));
                b.type = "button";
                const body = h("span", "slls-sx-sm-role-body");
                const top = h("span", "slls-sx-sm-role-top");
                top.appendChild(h("span", "name", effName(p)));
                if (p.isNew) top.appendChild(h("span", "slls-sx-pill new", "New"));
                if (deleted) top.appendChild(h("span", "slls-sx-pill danger", "Deleted"));
                const c = counts(memberName(p));
                body.append(top, h("span", "meta", `${plural(c.tables, "table")} \u00b7 ${plural(c.fields, "field")}`));
                b.append(iconEl("perspective", ""), body);
                b.addEventListener("click", () => { active = p.key; draw(); });
                if (write()) {
                    const del = h("span", "slls-sx-sm-del");
                    del.setAttribute("role", "button");
                    del.title = deleted ? "Restore perspective" : "Delete perspective";
                    del.setAttribute("aria-label", `${deleted ? "Restore" : "Delete"} perspective ${effName(p)}`);
                    del.innerHTML = deleted ? ICON.undo : ICON.trash;
                    del.addEventListener("click", (ev) => {
                        ev.stopPropagation();
                        if (deleted) { toggleOp(p, "delete"); return; }
                        confirmDialog(`Delete the perspective \u2018${effName(p)}\u2019? It is removed from the model when you Save.`, "Delete perspective",
                            () => { if (p.isNew) removeNew(p); else toggleOp(p, "delete"); });
                    });
                    b.appendChild(del);
                }
                items.appendChild(b);
            }
            side.appendChild(items);
            return side;
        }
        function drawMain() {
            const main = h("div", "slls-sx-fpm-main");
            const p = current();
            if (!p) {
                const empty = h("div", "slls-sx-center");
                empty.append(iconEl("perspective", ""), h("div", "", "Create or select a perspective to begin."));
                main.appendChild(empty);
                return main;
            }
            // Read live: renaming a staged perspective changes its member name.
            const pn = () => memberName(p);
            const editable = write() && !isDeleted(p);
            const bar = h("div", "slls-sx-fpm-toolbar");
            bar.appendChild(iconEl("perspective", "slls-sx-fpm-title-icon"));
            if (editable) {
                const name = h("input", "slls-sx-input slls-sx-fpm-name");
                name.value = effName(p);
                name.setAttribute("aria-label", "Perspective name");
                name.addEventListener("input", () => {
                    if (p.isNew) renameNew(p, name.value);
                    else setValue(p, "Name", name.value, { label: "Name", coalesce: true, keepDetail: true });
                    const el = m.body.querySelector(".slls-sx-fpm-item.active .name");
                    if (el) el.textContent = name.value;
                });
                bar.appendChild(name);
            } else bar.appendChild(h("span", "slls-sx-fpm-name-text", effName(p)));
            const sum = h("span", "slls-sx-tool-count");
            bar.appendChild(sum);
            main.appendChild(bar);
            const tools = h("div", "slls-sx-tool-toolbar");
            tools.appendChild(searchBox("Search tables and fields\u2026", search, (v) => { search = v; paint(); }));
            tools.append(button("slls-sx-iconbtn", "", "expand_rows", () => { savedTables().forEach((t) => openTables.add(t.key)); paint(); }, "Expand all"),
                button("slls-sx-iconbtn", "", "collapse_rows", () => { openTables.clear(); paint(); }, "Collapse all"));
            if (editable) {
                tools.append(button("slls-sx-btn small", "Select all", "", () => {
                    const items = [];
                    for (const t of savedTables()) {
                        for (const [, n] of fieldsOf(t)) items.push({ node: n, prop: "Perspectives", value: withName(n, pn(), true), label: "Perspectives" });
                        items.push({ node: t, prop: "Perspectives", value: withName(t, pn(), true), label: "Perspectives" });
                    }
                    stageMany(items);
                }), button("slls-sx-btn small", "Clear", "", () => {
                    const items = [];
                    for (const t of savedTables()) {
                        for (const [, n] of fieldsOf(t)) items.push({ node: n, prop: "Perspectives", value: withName(n, pn(), false), label: "Perspectives" });
                        items.push({ node: t, prop: "Perspectives", value: withName(t, pn(), false), label: "Perspectives" });
                    }
                    stageMany(items);
                }));
            }
            main.appendChild(tools);
            const scroll = h("div", "slls-sx-tool-scroll");
            scroll.dataset.scroll = "tree";
            const listEl = h("div", "slls-sx-pe-tree");
            scroll.appendChild(listEl);
            main.appendChild(scroll);
            const changed = (n) => listOf(n).includes(pn()) !== savedListOf(n).includes(pn()) && !p.isNew;
            function paint() {
                clear(listEl);
                const q = search.trim().toLowerCase();
                let tableCount = 0;
                for (const t of savedTables().sort(byLabel)) {
                    const fields = fieldsOf(t);
                    const inPersp = fields.filter(([, n]) => listOf(n).includes(pn()));
                    if (listOf(t).includes(pn())) tableCount += 1;
                    const tableHit = !q || effName(t).toLowerCase().includes(q);
                    const shown = tableHit ? fields : fields.filter(([, n]) => effName(n).toLowerCase().includes(q));
                    if (!tableHit && !shown.length) continue;
                    const isOpen = !!q || openTables.has(t.key);
                    const row = h("div", "slls-sx-pe-row table");
                    row.appendChild(caretBtn(isOpen, () => { if (openTables.has(t.key)) openTables.delete(t.key); else openTables.add(t.key); paint(); }));
                    const ids = fields.length ? fields.map(([, n]) => n.key) : [t.key];
                    const sel = new Set(fields.length ? inPersp.map(([, n]) => n.key) : (listOf(t).includes(pn()) ? [t.key] : []));
                    // triCheckbox has already applied the toggle to `sel`.
                    const cb = triCheckbox(ids, sel, `Toggle table ${effName(t)}`,
                        () => stageMembership(t, fields.map(([, n]) => n), ids.every((id) => sel.has(id)), pn()));
                    cb.disabled = !editable;
                    row.append(cb, iconEl(tableIcon(t), ""), h("span", "name", effName(t)));
                    row.appendChild(dirtyDot(changed(t) || fields.some(([, n]) => changed(n))));
                    const n3 = (kind) => [inPersp.filter(([k]) => k === kind).length, fields.filter(([k]) => k === kind).length];
                    const [c1, c2] = n3("Column"), [m1, m2] = n3("Measure"), [h1, h2] = n3("Hierarchy");
                    row.appendChild(h("span", "meta", `${c1}/${c2} cols \u00b7 ${m1}/${m2} measures \u00b7 ${h1}/${h2} hierarchies`));
                    listEl.appendChild(row);
                    if (!isOpen) continue;
                    for (const [kind, n] of shown) {
                        const r = h("label", "slls-sx-pe-row field" + (isHidden(n) ? " muted" : ""));
                        const fcb = h("input", "slls-sx-tool-check");
                        fcb.type = "checkbox";
                        fcb.checked = listOf(n).includes(pn());
                        fcb.disabled = !editable;
                        fcb.setAttribute("aria-label", `Toggle ${effName(n)}`);
                        fcb.addEventListener("change", () => stageMembership(t, [n], fcb.checked, pn()));
                        const icon = kind === "Column" ? (n.column_type === "Calculated" ? "calculated_column" : "column") : kind.toLowerCase();
                        r.append(fcb, iconEl(icon, ""), h("span", "name", effName(n)));
                        if (isHidden(n)) { const e = iconEl("eye_off", "slls-sx-tool-hidden"); e.title = "Hidden"; r.appendChild(e); }
                        r.appendChild(dirtyDot(changed(n)));
                        listEl.appendChild(r);
                    }
                }
                if (!listEl.childNodes.length) listEl.appendChild(h("div", "slls-sx-empty", "No tables match your search."));
                sum.textContent = `${plural(tableCount, "table")} in \u201c${effName(p)}\u201d`;
            }
            paint();
            return main;
        }
        function draw() {
            const list = perspectives();
            if (active === null || !list.some((p) => p.key === active)) active = list.length ? list[0].key : null;
            toolSubtitle(m, "Choose the tables, columns, measures and hierarchies in each perspective."
                + (mode === "write" ? " Changes are staged and applied when you Save." : ""));
            toolActions(m);
            keepScroll(m.body, () => {
                clear(m.body);
                const layout = h("div", "slls-sx-fpm");
                layout.append(drawSidebar(), drawMain());
                m.body.appendChild(layout);
            });
        }
        registerTool(m, draw, () => toolActions(m));
        draw();
    }

    // ------------------------------------------------------------------
    // Query scale-out manager
    // ------------------------------------------------------------------
    const QSO_DOCS_URL = "https://learn.microsoft.com/fabric/enterprise/powerbi/service-premium-scale-out";
    function fmtTime(v) {
        if (!v) return "\u2014";
        const d = new Date(v);
        return Number.isNaN(d.getTime()) ? String(v) : d.toLocaleString();
    }
    function toolNotice(ok, text, onDismiss) {
        const n = h("div", "slls-sx-tool-notice " + (ok ? "ok" : "error"));
        n.append(iconEl(ok ? "check_circle" : "alert", ""), h("span", "", text));
        if (onDismiss) n.appendChild(button("slls-sx-iconbtn", "", "close", onDismiss, "Dismiss"));
        return n;
    }
    function openQueryScaleOutManager() {
        const m = toolWindow("Query scale-out manager", "server");
        let state = null, loading = false, busy = false, error = null, notice = null;
        let autoSync = true, maxReplicas = "-1";
        const seed = (s) => {
            autoSync = s.settings.autoSync !== false;
            const cur = s.settings.maxReplicas == null ? 0 : s.settings.maxReplicas;
            maxReplicas = String(cur === 0 ? -1 : cur);
        };
        const enabled = () => !!state && state.settings.maxReplicas != null && state.settings.maxReplicas !== 0;
        function run(payload, message) {
            if (busy || loading) return;
            if (payload.op === "get") loading = true; else busy = true;
            notice = null;
            draw();
            request("qso", payload, (res) => {
                loading = false; busy = false;
                if (res.ok) {
                    state = res.data;
                    seed(state);
                    error = null;
                    if (message) notice = { ok: true, text: message };
                } else if (!state) error = res.error || "The query scale-out settings could not be loaded.";
                else notice = { ok: false, text: res.error || "The change failed." };
                if (m.overlay.isConnected) draw();
            });
        }
        function row(label, help, control) {
            const r = h("div", "slls-sx-qso-row");
            const t = h("div", "slls-sx-qso-label");
            t.append(h("span", "title", label), h("span", "help", help));
            r.append(t, control);
            return r;
        }
        function draw() {
            toolSubtitle(m, "Load-balance read queries across read-only replicas. Changes apply immediately.");
            clear(m.actions);
            const reload = button("slls-sx-btn small", "Reload", "refresh", () => run({ op: "get" }), "Reload the settings and sync status");
            reload.disabled = loading || busy;
            if (loading) reload.querySelector("svg").classList.add("slls-sx-spin");
            m.actions.appendChild(reload);
            keepScroll(m.body, () => {
                clear(m.body);
                const scroll = h("div", "slls-sx-tool-scroll");
                scroll.dataset.scroll = "main";
                const wrap = h("div", "slls-sx-tool-wrap");
                scroll.appendChild(wrap);
                m.body.appendChild(scroll);
                if (!state) {
                    if (error) {
                        const err = h("div", "slls-sx-dlm-error");
                        err.append(iconEl("alert", ""), h("div", "", error), button("slls-sx-btn small", "Try again", "refresh", () => run({ op: "get" })));
                        wrap.appendChild(err);
                    } else wrap.appendChild(loadingBox("Loading query scale-out settings\u2026"));
                    return;
                }
                if (notice) wrap.appendChild(toolNotice(notice.ok, notice.text, () => { notice = null; draw(); }));
                const s = state.settings;
                const card = h("div", "slls-sx-tool-card");
                const head = h("div", "slls-sx-qso-head");
                head.append(h("div", "slls-sx-section-title", "Settings"),
                    h("span", "slls-sx-pill" + (enabled() ? " new" : ""), enabled() ? "Scale-out enabled" : "Scale-out disabled"));
                card.appendChild(head);
                const seg = h("div", "slls-sx-seg");
                for (const fmt of ["Small", "Large"]) {
                    const b = h("button", s.storageMode === fmt ? "on" : "", fmt);
                    b.type = "button";
                    b.disabled = busy || s.storageMode === fmt;
                    b.addEventListener("click", () => run({ op: "storage", storageFormat: fmt }, `Storage format set to ${fmt}.`));
                    seg.appendChild(b);
                }
                card.appendChild(row("Storage format", "Scale-out requires the Large storage format.", seg));
                const sw = switchEl(autoSync, "Auto-sync read-only replicas", () => { autoSync = !autoSync; draw(); });
                sw.disabled = busy;
                card.appendChild(row("Auto-sync read-only replicas", "Automatically sync replicas after each model change.", sw));
                const max = h("input", "slls-sx-input slls-sx-qso-max");
                max.type = "number";
                max.min = "-1";
                max.value = maxReplicas;
                max.disabled = busy;
                max.setAttribute("aria-label", "Max read-only replicas");
                card.appendChild(row("Max read-only replicas", "-1 = as many as the capacity allows \u00b7 0 = disable scale-out.", max));
                const actions = h("div", "slls-sx-qso-actions");
                const label = () => {
                    const n = parseInt(max.value, 10);
                    if (enabled() && n === 0) return "Apply settings (disables scale-out)";
                    if (!enabled() && Number.isFinite(n) && n !== 0) return "Apply settings & enable scale-out";
                    return "Apply settings";
                };
                const apply = button("slls-sx-btn primary", label(), "save", () => {
                    const n = parseInt(max.value, 10);
                    const value = Number.isFinite(n) ? n : -1;
                    run({ op: "apply", autoSync, maxReplicas: value },
                        value === 0 ? "Query scale-out disabled." : `Query scale-out settings applied (max ${value === -1 ? "unlimited" : value} replica${value === 1 ? "" : "s"}).`);
                });
                apply.disabled = busy;
                if (busy) apply.querySelector("svg").classList.add("slls-sx-spin");
                const hint = h("span", "slls-sx-qso-hint", "Applying will upgrade the storage format to Large.");
                const refreshHint = () => {
                    apply.lastChild.textContent = label();
                    hint.style.display = s.storageMode === "Small" && parseInt(max.value, 10) !== 0 ? "" : "none";
                };
                max.addEventListener("input", () => { maxReplicas = max.value; refreshHint(); });
                const disable = button("slls-sx-btn", "Disable scale-out", "", () => run({ op: "disable" }, "Query scale-out disabled."));
                disable.disabled = busy || !enabled();
                actions.append(apply, disable, hint);
                refreshHint();
                card.appendChild(actions);
                wrap.appendChild(card);

                const st = state.status;
                const sc = h("div", "slls-sx-tool-card");
                const sh = h("div", "slls-sx-qso-head");
                sh.append(h("div", "slls-sx-section-title", "Sync status"), h("span", "slls-sx-pill", st.scaleOutStatus || "Unknown"));
                const sync = button("slls-sx-btn small", "Trigger sync", "refresh", () => run({ op: "sync" }, "Query scale-out sync triggered."),
                    enabled() ? "Trigger a sync of read-only replicas" : "Enable scale-out to sync replicas");
                sync.disabled = busy || !enabled();
                sh.appendChild(sync);
                sc.appendChild(sh);
                if (st.scaleOutStatus === "Enabled") {
                    const grid = h("div", "slls-sx-qso-stats");
                    const num = (v) => (v == null ? "\u2014" : Number(v).toLocaleString());
                    for (const [k, v] of [["Sync start", fmtTime(st.syncStartTime)], ["Sync end", fmtTime(st.syncEndTime)],
                        ["Trigger reason", st.triggerReason || "\u2014"], ["Commit version", num(st.commitVersion)],
                        ["Target sync version", num(st.targetSyncVersion)], ["Min active read version", num(st.minActiveReadVersion)]]) {
                        const cell = h("div", "stat");
                        cell.append(h("span", "k", k), h("span", "v", v));
                        grid.appendChild(cell);
                    }
                    sc.appendChild(grid);
                    sc.appendChild(h("div", "slls-sx-tool-label", `Read-only replicas (${st.replicas.length})`));
                    if (!st.replicas.length) sc.appendChild(h("div", "slls-sx-kv-empty", "No read-only replicas yet."));
                    else {
                        const table = h("table", "slls-sx-qso-table");
                        const tr = h("tr", "");
                        for (const c of ["Replica ID", "Type", "Version", "Timestamp"]) tr.appendChild(h("th", "", c));
                        const thead = h("thead", "");
                        thead.appendChild(tr);
                        const tbody = h("tbody", "");
                        for (const r of st.replicas) {
                            const rr = h("tr", "");
                            rr.append(h("td", "slls-sx-mono", r.id || "\u2014"), h("td", "", r.type || "\u2014"), h("td", "", r.version || "\u2014"), h("td", "", fmtTime(r.timestamp)));
                            tbody.appendChild(rr);
                        }
                        table.append(thead, tbody);
                        sc.appendChild(table);
                    }
                } else {
                    sc.appendChild(h("p", "slls-sx-dlm-text", /storagemodenotsupported/i.test(st.scaleOutStatus)
                        ? "Query scale-out is only supported when the semantic model is in the Large storage format. Switch the storage format to Large above, then enable scale-out."
                        : "Query scale-out is disabled for this semantic model. Enable it above to sync read-only replicas."));
                }
                wrap.appendChild(sc);
                const foot = h("p", "slls-sx-dlm-note");
                foot.appendChild(document.createTextNode("Query scale-out load-balances read queries across read-only replicas. "
                    + "It requires a Premium / Fabric capacity and the Large storage format. "));
                const a = h("a", "slls-sx-link slls-sx-fpm-link", "Learn more");
                a.href = QSO_DOCS_URL; a.target = "_blank"; a.rel = "noreferrer";
                a.appendChild(iconEl("external_link", ""));
                foot.appendChild(a);
                wrap.appendChild(foot);
            });
        }
        registerTool(m, draw);
        draw();
        run({ op: "get" });
    }
    function loadingBox(text) {
        const c = h("div", "slls-sx-center");
        c.style.padding = "48px 0";
        const sp = iconEl("refresh", "");
        sp.querySelector("svg").classList.add("slls-sx-spin");
        c.append(sp, h("div", "", text));
        return c;
    }

    // ------------------------------------------------------------------
    // Warm Direct Lake cache
    // ------------------------------------------------------------------
    let warmColumns = null;
    function openWarmCache() {
        const m = toolWindow("Warm Direct Lake cache", "flame");
        let loading = false, warming = false, error = null, notice = null;
        let search = "", residentFilter = "all", sortKey = "temperature", sortDir = "desc";
        const expandedTables = new Set();
        const selected = new Set();
        const savedPersp = () => (data.perspectives || []).map((p) => p.name).sort(cmpText);
        let perspective = savedPersp()[0] || "";
        let addDeps = true;
        const widths = new Map();
        const keyOfCol = (c) => `${c.table}\u0000${c.column}`;
        const directLake = () => !!data.is_direct_lake;
        function scan() {
            loading = true; error = null;
            draw();
            request("warm_scan", {}, (res) => {
                loading = false;
                if (res.ok) warmColumns = res.data.columns || [];
                else error = res.error || "Column residency could not be read.";
                if (m.overlay.isConnected) draw();
            });
        }
        function warm(payload, describe) {
            if (warming) return;
            warming = true; notice = null;
            draw();
            request("warm", payload, (res) => {
                warming = false;
                if (res.ok) {
                    warmColumns = res.data.columns || warmColumns;
                    notice = { ok: true, text: describe(res.data.warmed) };
                    if (!payload.perspective) selected.clear();
                } else notice = { ok: false, text: res.error || "Warming failed." };
                if (m.overlay.isConnected) draw();
            });
        }
        function tables() {
            const q = search.trim().toLowerCase();
            const by = new Map();
            for (const c of warmColumns || []) {
                if (residentFilter === "yes" && !c.resident) continue;
                if (residentFilter === "no" && c.resident) continue;
                if (q && !`${c.table} ${c.column}`.toLowerCase().includes(q)) continue;
                if (!by.has(c.table)) by.set(c.table, []);
                by.get(c.table).push(c);
            }
            const dir = sortDir === "asc" ? 1 : -1;
            const cmpCol = (a, b) => (sortKey === "temperature" ? (a.temperature - b.temperature)
                : sortKey === "resident" ? (a.resident - b.resident)
                    : sortKey === "lastAccessed" ? String(a.lastAccessed).localeCompare(String(b.lastAccessed))
                        : cmpText(a.column, b.column)) * dir;
            const out = [];
            for (const [table, cols] of by) {
                cols.sort(cmpCol);
                out.push({ table, cols, resident: cols.filter((c) => c.resident).length,
                    temperature: cols.reduce((s, c) => s + c.temperature, 0),
                    lastAccessed: cols.reduce((mx, c) => (c.lastAccessed > mx ? c.lastAccessed : mx), "") });
            }
            out.sort((a, b) => (sortKey === "temperature" ? (a.temperature - b.temperature)
                : sortKey === "resident" ? (a.resident / a.cols.length - b.resident / b.cols.length)
                    : sortKey === "lastAccessed" ? a.lastAccessed.localeCompare(b.lastAccessed)
                        : cmpText(a.table, b.table)) * dir);
            return out;
        }
        const fmtTemp = (n) => (!n ? "\u2014" : n.toLocaleString(undefined, { maximumFractionDigits: 4 }));
        let tbody = null, countEl = null, warmBtn = null, clearBtn = null;
        function refreshWarmBtn() {
            if (clearBtn) clearBtn.style.display = selected.size ? "" : "none";
            if (!warmBtn) return;
            warmBtn.lastChild.textContent = warming ? "Warming\u2026" : `Warm ${plural(selected.size, "column")}`;
            warmBtn.disabled = !selected.size || warming || !directLake();
        }
        function paintRows() {
            clear(tbody);
            const list = tables();
            const shown = list.reduce((n, t) => n + t.cols.length, 0);
            const total = (warmColumns || []).length;
            countEl.textContent = `${shown.toLocaleString()}${shown !== total ? ` of ${total.toLocaleString()}` : ""} column${shown === 1 ? "" : "s"}`;
            if (!list.length) {
                const tr = h("tr", "");
                const td = h("td", "slls-sx-empty", total ? "No columns match the current filters." : "No columns found.");
                td.colSpan = 4;
                tr.appendChild(td);
                tbody.appendChild(tr);
                return;
            }
            const filtering = !!search.trim() || residentFilter !== "all";
            for (const t of list) {
                const open = filtering || expandedTables.has(t.table);
                const tr = h("tr", "table");
                const td = h("td", "");
                const cell = h("div", "slls-sx-dm-obj");
                cell.appendChild(caretBtn(open, () => { if (expandedTables.has(t.table)) expandedTables.delete(t.table); else expandedTables.add(t.table); paintRows(); }));
                cell.appendChild(triCheckbox(t.cols.map(keyOfCol), selected, `Select every column of ${t.table}`, () => { syncTriCheckboxes(m.body); refreshWarmBtn(); }));
                const node = tableByName.get(t.table);
                cell.append(iconEl(node ? tableIcon(node) : "table", ""), h("span", "name", tableDisplayName(t.table)), h("span", "meta", String(t.cols.length)));
                td.appendChild(cell);
                tr.append(td, h("td", "", `${t.resident} / ${t.cols.length}`), h("td", "num", fmtTemp(t.temperature)), h("td", "", fmtTime(t.lastAccessed)));
                tbody.appendChild(tr);
                if (!open) continue;
                for (const c of t.cols) {
                    const cr = h("tr", "");
                    const ctd = h("td", "");
                    const cc = h("div", "slls-sx-dm-obj");
                    cc.style.paddingLeft = "26px";
                    cc.appendChild(triCheckbox([keyOfCol(c)], selected, `Select ${c.column}`, () => { syncTriCheckboxes(m.body); refreshWarmBtn(); }));
                    cc.append(iconEl("column", ""), h("span", "name", columnDisplayName(t.table, c.column)));
                    ctd.appendChild(cc);
                    const res = h("span", "slls-sx-pill" + (c.resident ? " new" : ""), c.resident ? "Yes" : "No");
                    const rtd = h("td", "");
                    rtd.appendChild(res);
                    const ltd = h("td", "", c.lastAccessed ? fmtTime(c.lastAccessed) : "\u2014");
                    if (c.lastAccessed) ltd.title = c.lastAccessed;
                    cr.append(ctd, rtd, h("td", "num", fmtTemp(c.temperature)), ltd);
                    tbody.appendChild(cr);
                }
            }
        }
        function draw() {
            toolSubtitle(m, "Page Direct Lake columns into memory. Residency comes from the same statistics as Vertipaq Analyzer.");
            clear(m.actions);
            const reload = button("slls-sx-btn small", "Reload", "refresh", scan, "Rescan column residency");
            reload.disabled = loading || warming;
            if (loading) reload.querySelector("svg").classList.add("slls-sx-spin");
            m.actions.appendChild(reload);
            keepScroll(m.body, () => {
                clear(m.body);
                tbody = null; warmBtn = null; clearBtn = null;
                if (!directLake()) {
                    m.body.appendChild(toolNotice(false, "This semantic model has no Direct Lake partitions, so its columns cannot be warmed. Residency is shown for reference."));
                }
                if (!warmColumns) {
                    if (error) {
                        const err = h("div", "slls-sx-dlm-error");
                        err.append(iconEl("alert", ""), h("div", "", error), button("slls-sx-btn small", "Try again", "refresh", scan));
                        m.body.appendChild(err);
                    } else m.body.appendChild(loadingBox("Scanning column residency\u2026"));
                    return;
                }
                const bar = h("div", "slls-sx-tool-toolbar");
                bar.appendChild(searchBox("Filter tables & columns\u2026", search, (v) => { search = v; paintRows(); }));
                bar.appendChild(field("Resident", selectEl([["all", "All"], ["yes", "Yes"], ["no", "No"]], residentFilter, (v) => { residentFilter = v; paintRows(); }, "Resident")));
                bar.append(button("slls-sx-iconbtn", "", "expand_rows", () => { (warmColumns || []).forEach((c) => expandedTables.add(c.table)); paintRows(); }, "Expand all tables"),
                    button("slls-sx-iconbtn", "", "collapse_rows", () => { expandedTables.clear(); paintRows(); }, "Collapse all tables"));
                const right = h("div", "slls-sx-warm-right");
                clearBtn = button("slls-sx-btn small", "Clear selection", "", () => { selected.clear(); syncTriCheckboxes(m.body); refreshWarmBtn(); });
                right.appendChild(clearBtn);
                warmBtn = button("slls-sx-btn small primary", "", "flame", () => {
                    const cols = (warmColumns || []).filter((c) => selected.has(keyOfCol(c))).map((c) => ({ table: c.table, column: c.column }));
                    const nTables = new Set(cols.map((c) => c.table)).size;
                    warm({ columns: cols }, () => `Warmed ${plural(cols.length, "column")} across ${plural(nTables, "table")}.`);
                }, "Run simple DAX queries to page the selected columns into memory");
                warmBtn.appendChild(document.createTextNode(""));
                if (warming) warmBtn.querySelector("svg").classList.add("slls-sx-spin");
                refreshWarmBtn();
                countEl = h("span", "slls-sx-tool-count");
                right.append(warmBtn, countEl);
                bar.appendChild(right);
                m.body.appendChild(bar);
                const persps = savedPersp();
                if (persps.length) {
                    const pb = h("div", "slls-sx-tool-toolbar slls-sx-warm-persp");
                    if (!persps.includes(perspective)) perspective = persps[0];
                    pb.appendChild(field("Warm from perspective", selectEl(persps, perspective, (v) => { perspective = v; }, "Perspective")));
                    const lab = h("label", "slls-sx-warm-deps");
                    const cb = h("input", "slls-sx-tool-check");
                    cb.type = "checkbox";
                    cb.checked = addDeps;
                    cb.addEventListener("change", () => { addDeps = cb.checked; draw(); });
                    lab.append(cb, document.createTextNode("Include dependencies"));
                    pb.appendChild(lab);
                    const wp = button("slls-sx-btn small", "Warm perspective", "flame", () => warm({ perspective, addDependencies: addDeps },
                        (n) => `Warmed ${plural(n, "column")} from the \u2018${perspective}\u2019 perspective${addDeps ? " (with dependencies)" : ""}.`),
                    "Put the columns in this perspective (plus their dependencies) into memory");
                    wp.disabled = warming || !directLake();
                    pb.appendChild(wp);
                    pb.appendChild(h("span", "slls-sx-dlm-note", "Puts every column in the perspective"
                        + (addDeps ? " and its dependencies (measure columns, hierarchy levels, relationships)" : "") + " into memory."));
                    m.body.appendChild(pb);
                }
                if (notice) m.body.appendChild(toolNotice(notice.ok, notice.text, () => { notice = null; draw(); }));
                const scroll = h("div", "slls-sx-tool-scroll");
                scroll.dataset.scroll = "rows";
                const table = h("table", "slls-sx-tm-table slls-sx-warm-table");
                const thead = h("thead", "");
                const hr = h("tr", "");
                for (const [key, label, w, title] of [["name", "Table / Column", 360, "The model table and its columns."],
                    ["resident", "Is Resident", 130, "Whether the column is currently held in memory. Direct Lake pages column data in on demand."],
                    ["temperature", "Temperature", 140, "A decimal indicating the frequency and recency of queries against the column (higher = hotter)."],
                    ["lastAccessed", "Last Accessed", 220, "The time the column was last queried, or empty if it has not been accessed."]]) {
                    const th = h("th", key === "temperature" ? "num" : "");
                    th.dataset.columnWidth = String(w);
                    th.title = title;
                    const b = h("button", "slls-sx-warm-sort" + (sortKey === key ? " on" : ""), label);
                    b.type = "button";
                    if (sortKey === key) b.appendChild(h("span", "dir", sortDir === "asc" ? "\u25b2" : "\u25bc"));
                    b.addEventListener("click", () => {
                        if (sortKey === key) sortDir = sortDir === "asc" ? "desc" : "asc";
                        else { sortKey = key; sortDir = key === "name" ? "asc" : "desc"; }
                        draw();
                    });
                    th.appendChild(b);
                    hr.appendChild(th);
                }
                thead.appendChild(hr);
                tbody = h("tbody", "");
                table.append(thead, tbody);
                scroll.appendChild(table);
                m.body.appendChild(scroll);
                sllsInstallColumnResizers(table, { widths, key: "warm", minWidth: 64 });
                paintRows();
            });
        }
        registerTool(m, draw);
        draw();
        if (!warmColumns) scan();
    }

    // ------------------------------------------------------------------
    // Create .vpax file
    // ------------------------------------------------------------------
    const VPAX_INFO_URL = "https://www.sqlbi.com/tv/inside-a-vpax-file-produced-by-vertipaq-analyzer/";
    function openCreateVpax() {
        const m = modal("Create .vpax file", "file_box");
        m.box.classList.add("slls-sx-vpax");
        let busy = false, error = null, result = null;
        let wsId = "", lhId = "";
        let fileName = model.get("dataset_name") || "";
        let overwrite = false, readStats = false, dlMode = "ResidentOnly";
        let workspaces = null, lakehouses = null;
        const wsSel = createSearchSelect({ placeholder: "Select a workspace\u2026", searchPlaceholder: "Filter workspaces\u2026", ariaLabel: "Save to workspace",
            emptyLabel: "Loading workspaces\u2026", onChange: (o) => { wsId = o.value; lhId = ""; lakehouses = null; loadLakehouses(); paint(); } });
        const lhSel = createSearchSelect({ placeholder: "Select a lakehouse\u2026", searchPlaceholder: "Filter lakehouses\u2026", ariaLabel: "Lakehouse",
            emptyLabel: "Select a workspace first", onChange: (o) => { lhId = o.value; paint(); } });
        function loadLakehouses() {
            if (!wsId) return;
            const target = wsId;
            request("vpax_lakehouses", { workspace_id: target }, (res) => {
                if (target !== wsId) return;
                lakehouses = res.ok ? res.data.items || [] : [];
                if (!res.ok) error = res.error || "Failed to load lakehouses.";
                paint();
            });
        }
        const name = () => fileName.trim().replace(/\.vpax$/i, "").trim();
        const ready = () => !!wsId && !!lhId && !!name() && !busy;
        function create() {
            if (!ready()) return;
            busy = true; error = null;
            paint();
            request("vpax_create", { lakehouseWorkspaceId: wsId, lakehouseId: lhId, fileName: name(), overwrite,
                readStatsFromData: readStats, directLakeStatsMode: dlMode }, (res) => {
                busy = false;
                if (res.ok) result = res.data; else error = res.error || "Failed to create the .vpax file.";
                if (m.overlay.isConnected) paint();
            });
        }
        let fields = null;
        function paint() {
            clear(m.body);
            const intro = h("p", "slls-sx-dlm-text");
            intro.append(document.createTextNode("Runs VertiPaq Analyzer over "), h("strong", "", model.get("dataset_name") || "the model"),
                document.createTextNode(" and saves a .vpax file to the chosen lakehouse\u2019s Files section. "));
            const a = h("a", "slls-sx-link slls-sx-fpm-link", "Learn more");
            a.href = VPAX_INFO_URL; a.target = "_blank"; a.rel = "noreferrer"; a.title = "Learn what's inside a .vpax file (SQLBI)";
            a.appendChild(iconEl("external_link", ""));
            intro.appendChild(a);
            m.body.appendChild(intro);
            const foot = m.box.querySelector(".slls-sx-modal-foot") || h("div", "slls-sx-modal-foot");
            clear(foot);
            if (result) {
                const lh = (lakehouses || []).find((x) => x.id === lhId);
                m.body.appendChild(toolNotice(true, `Saved ${result.filePath}${lh ? ` to the ${lh.name} lakehouse` : ""}.`));
                foot.appendChild(button("slls-sx-btn primary", "Done", "", () => m.close()));
                m.box.appendChild(foot);
                return;
            }
            if (!fields) fields = h("div", "slls-sx-vpax-fields");
            clear(fields);
            wsSel.setOptions((workspaces || []).map((w) => ({ value: w.id, label: w.name })), wsId);
            wsSel.setEmptyLabel(workspaces ? "No workspaces" : "Loading workspaces\u2026");
            wsSel.setDisabled(busy || !workspaces);
            lhSel.setOptions((lakehouses || []).map((l) => ({ value: l.id, label: l.name })), lhId);
            lhSel.setEmptyLabel(!wsId ? "Select a workspace first" : lakehouses ? "No lakehouses in this workspace" : "Loading lakehouses\u2026");
            lhSel.setDisabled(busy || !wsId || !lakehouses || !lakehouses.length);
            const fw = h("div", "slls-sx-tool-field");
            fw.append(h("span", "", "Save to workspace"), wsSel.el);
            const fl = h("div", "slls-sx-tool-field");
            fl.append(h("span", "", "Lakehouse"), lhSel.el);
            const fn = h("div", "slls-sx-tool-field");
            const fnRow = h("div", "slls-sx-vpax-name");
            const input = h("input", "slls-sx-input");
            input.value = fileName;
            input.disabled = busy;
            input.setAttribute("aria-label", "File name");
            input.addEventListener("input", () => { fileName = input.value; go.disabled = !ready(); });
            fnRow.append(input, h("span", "ext", ".vpax"));
            fn.append(h("span", "", "File name"), fnRow);
            fields.append(fw, fl, fn);
            const opts = h("div", "slls-sx-vpax-opts");
            const ow = switchEl(overwrite, "Overwrite", () => { overwrite = !overwrite; paint(); }, "Overwrite an existing file");
            const rs = switchEl(readStats, "Read statistics from data", () => { readStats = !readStats; paint(); }, "Read statistics from data");
            ow.disabled = rs.disabled = busy;
            opts.append(ow, rs);
            if (readStats) {
                const dl = selectEl([["ResidentOnly", "Resident columns only"], ["Full", "All columns"]], dlMode, (v) => { dlMode = v; }, "Direct Lake statistics");
                dl.disabled = busy;
                opts.appendChild(field("Direct Lake statistics", dl));
            }
            fields.appendChild(opts);
            m.body.appendChild(fields);
            if (error) {
                const err = h("div", "slls-sx-tool-error");
                err.append(iconEl("alert", ""), h("span", "", error));
                m.body.appendChild(err);
            }
            const cancel = button("slls-sx-btn", "Cancel", "", () => m.close());
            cancel.disabled = busy;
            const go = button("slls-sx-btn primary", busy ? "Creating\u2026" : "Create .vpax", busy ? "refresh" : "file_box", create);
            if (busy) go.querySelector("svg").classList.add("slls-sx-spin");
            go.disabled = !ready();
            foot.append(cancel, go);
            m.box.appendChild(foot);
        }
        paint();
        request("vpax_workspaces", {}, (res) => {
            workspaces = res.ok ? res.data.items || [] : [];
            if (!res.ok) error = res.error || "Failed to load workspaces.";
            if (m.overlay.isConnected) paint();
        });
    }
    function openHistory() {
        const m = modal("Change history", "history");
        const draw = () => {
            clear(m.body);
            clear(m.actions);
            const items = pendingItems();
            m.body.appendChild(h("div", "slls-sx-section-title", `Pending changes (${items.length})`));
            if (!items.length) m.body.appendChild(h("div", "slls-sx-kv-empty", "No pending changes."));
            for (const it of items) {
                const row = h("div", "slls-sx-change");
                const bodyEl = h("div", "slls-sx-change-body");
                bodyEl.appendChild(h("div", "slls-sx-change-title", it.title));
                if (it.detail) bodyEl.appendChild(h("div", "slls-sx-change-detail", it.detail));
                row.appendChild(bodyEl);
                if (!it.cascade) row.appendChild(button("slls-sx-btn small", "Revert", "undo", () => { revertChange(it); draw(); }));
                m.body.appendChild(row);
            }
            if (items.length) {
                const d = button("slls-sx-btn small danger", "Discard all", "trash", () => { discardAll(); draw(); });
                m.actions.appendChild(d);
            }
            m.body.appendChild(h("div", "slls-sx-section-title", "Saved in this session"));
            if (!history.length) m.body.appendChild(h("div", "slls-sx-kv-empty", "Nothing saved yet."));
            for (const batch of history) {
                const row = h("div", "slls-sx-change");
                const bodyEl = h("div", "slls-sx-change-body");
                bodyEl.appendChild(h("div", "slls-sx-change-title", `${batch.savedAt.toLocaleString()} \u2014 ${plural(batch.changes.length, "change")}`));
                bodyEl.appendChild(h("div", "slls-sx-change-detail", batch.changes.map((c) => "\u2022 " + c.title).join("\n")));
                row.appendChild(bodyEl);
                m.body.appendChild(row);
            }
        };
        draw();
    }
    function openErrors() {
        const errors = data.errors || [];
        const m = modal(`Model errors (${errors.length})`, "alert");
        if (!errors.length) { m.body.appendChild(h("div", "slls-sx-empty", "No objects are in an error state.")); return; }
        for (const e of errors) {
            const row = h("div", "slls-sx-change");
            const node = getNode(e.key);
            row.appendChild(iconEl(node ? nodeIcon(node) : "alert"));
            const bodyEl = h("div", "slls-sx-change-body");
            const title = h("a", "slls-sx-link slls-sx-change-title", node ? objectLabel(node) : e.name);
            title.addEventListener("click", () => { m.close(); reveal(e.key); });
            bodyEl.appendChild(title);
            bodyEl.appendChild(h("div", "slls-sx-change-detail", `${TYPE_LABEL[e.type] || e.type} \u00b7 ${e.state}${e.message ? "\n" + e.message : ""}`));
            row.appendChild(bodyEl);
            m.body.appendChild(row);
        }
    }

    // ------------------------------------------------------------------
    // Body + status
    // ------------------------------------------------------------------
    function renderBody() {
        clear(body);
        body.style.display = pickerOpen ? "none" : "";
        if (pickerOpen) return;
        if (kind() === "view") { if (viewOpen()) renderViewBody(); return; }
        const busy = model.get("busy") || "";
        if (busy === "connect" || (!loaded() && model.get("dataset_chosen") === true)) {
            loadingInto(body, "Reading model metadata\u2026");
            return;
        }
        if (!loaded()) return;
        body.append(treePane, dragHandle, detailPane);
        dragHandle.style.display = paneCollapsed ? "none" : "";
        renderTreePane();
        renderDetail();
    }
    function renderStatus() {
        const st = model.get("status") || {};
        clear(statusBar);
        statusBar.className = "slls-sx-status";
        if (!st.message) return;
        statusBar.classList.add("show", st.kind || "info");
        statusBar.appendChild(h("span", "", st.message));
        statusBar.appendChild(button("slls-sx-iconbtn", "", "close", () => { model.set("status", {}); model.save_changes(); }, "Dismiss"));
        if (statusTimer) clearTimeout(statusTimer);
        if (st.kind === "success") {
            statusTimer = setTimeout(() => { model.set("status", {}); model.save_changes(); }, 5000);
        }
    }
    function renderAll() {
        closeContextMenu();
        renderHeader();
        renderPicker();
        renderBody();
        renderStatus();
        redrawTools();
    }

    function onModelData() {
        data = inflate(model.get("model_data"));
        rebuildIndex();
        if (pendingSelect) {
            const p = pendingSelect;
            pendingSelect = null;
            const k = p.type === "model" ? modelKey()
                : p.type === "table" ? keyOf("table", "", "", p.name)
                : keyOf(p.type, p.table, p.hierarchy, p.name);
            if (index.has(k)) selectedKey = k;
        }
        if (!getNode(selectedKey)) selectedKey = modelKey();
    }
    model.on("change:model_version", () => { onModelData(); renderAll(); });
    model.on("change:view_version", () => { onViewData(); renderAll(); });
    model.on("change:busy", () => {
        renderHeader();
        if (pickerOpen) { renderPicker(); return; }
        // The connect reply closes the picker while busy is still "connect", so
        // the body is left on its loading screen; once busy clears it must be
        // rebuilt rather than only refreshing a tree pane that isn't shown.
        if (model.get("busy") === "connect" || !loaded() || !treePane.isConnected) renderBody();
        else renderTreePane();
    });
    model.on("change:status", renderStatus);
    model.on("change:available_workspaces", () => { if (pickerOpen) renderPicker(); });
    model.on("change:available_datasets", () => { if (pickerOpen) renderPicker(); });
    model.on("change:picker_loading", () => { if (pickerOpen) renderPicker(); });
    model.on("change:dataset_name", renderHeader);

    onModelData();
    onViewData();
    selectedKey = modelKey();
    renderAll();
}
export default { render };
"""

from sempy_labs._ui_components import (  # noqa: E402
    DARK_THEME_VARS as _UI_DARK_VARS,
    ICONS as _UI_ICONS,
    LIGHT_THEME_VARS as _UI_LIGHT_VARS,
    SEARCH_SELECT_CSS as _UI_SEARCH_SELECT_CSS,
    SEARCH_SELECT_JS as _UI_SEARCH_SELECT_JS,
    SYNTAX_HIGHLIGHT_VARS as _UI_SYNTAX_VARS,
    TABLE_COLUMN_RESIZE_JS as _UI_TABLE_COLUMN_RESIZE_JS,
    fullscreen_css as _ui_fullscreen_css,
    fullscreen_setup_js as _ui_fullscreen_setup_js,
    render_attribution_html as _ui_render_attribution_html,
    scoped_attribution_css as _ui_scoped_attribution_css,
    scoped_button_press_css as _ui_scoped_button_press_css,
    scoped_header_css as _ui_scoped_header_css,
    scoped_table_column_resize_css as _ui_scoped_table_column_resize_css,
)

_EXPLORER_ICON_NAMES = (
    "table", "calculated_table", "calculation_group", "field_parameter", "date_table",
    "column", "calculated_column", "measure", "hierarchy", "level", "calculation_item",
    "partition", "relationship", "perspective", "shield_check", "globe", "code", "zap",
    "database", "folder", "caret_right", "chevron_down", "search", "swap", "refresh",
    "expand_rows", "collapse_rows", "panel_collapse", "panel_expand", "list_tree",
    "sliders", "sun", "moon", "fullscreen", "fullscreen_exit", "pencil", "eye", "eye_off",
    "save", "history", "alert", "close", "trash", "plus", "undo", "redo", "file",
    "workflow", "wand", "expand_diagonal", "user", "telescope", "zoom_in", "zoom_out",
    "more", "arrow_up", "arrow_down", "external_link", "info", "package", "calendar",
    "download", "check_circle", "arrow_left", "network", "key_round", "focus", "lightbulb",
    "snowflake", "languages", "unlink", "arrow_left_right", "cable", "layers", "waves", "git_branch",
    "link", "reset", "user", "sparkles", "flame", "server", "file_box", "chevron_right",
    "warehouse", "upload",
)  # fmt: skip

_WIDGET_CSS = (
    _WIDGET_CSS.replace("__SLLS_LIGHT_VARS__", _UI_LIGHT_VARS)
    .replace("__SLLS_DARK_VARS__", _UI_DARK_VARS)
    .replace("__SLLS_SYNTAX_VARS__", _UI_SYNTAX_VARS)
    + "\n"
    + _UI_SEARCH_SELECT_CSS
    + _ui_scoped_header_css(".slls-sx")
    + _ui_scoped_button_press_css(".slls-sx")
    + _ui_scoped_attribution_css(".slls-sx")
    + "\n"
    + _ui_scoped_table_column_resize_css(".slls-sx")
    + _ui_fullscreen_css(".slls-sx", "slls-sx-fs", bg_var="var(--ui-bg-solid)")
)

_WIDGET_JS = (
    _ui_fullscreen_setup_js()
    + "\n"
    + _UI_SEARCH_SELECT_JS
    + "\n"
    + _UI_TABLE_COLUMN_RESIZE_JS
    + "\n"
    + _WIDGET_JS.replace(
        "__SLLS_EXPLORER_ICONS__",
        json.dumps({name: _UI_ICONS[name] for name in _EXPLORER_ICON_NAMES}),
    ).replace("__SLLS_ATTRIBUTION__", _ui_render_attribution_html())
)


# ---------------------------------------------------------------------------
# Object keys
# ---------------------------------------------------------------------------
# Every object is addressed by a key built from its type and its *saved* names,
# so pending renames never change the identity of the object being edited. The
# separator cannot appear in a TOM object name.
_KEY_SEP = "\u001f"

_TRANSLATABLE_TYPES = ("table", "column", "measure", "hierarchy", "level")
_PERSPECTIVE_TYPES = ("table", "column", "measure", "hierarchy")


def _explorer_key(
    obj_type: str, table: str = "", hierarchy: str = "", name: str = ""
) -> str:
    return _KEY_SEP.join([obj_type, table or "", hierarchy or "", name or ""])


def _object_key(obj) -> Optional[str]:
    """The explorer key of a TOM object, or None for unsupported types."""

    obj_type = str(obj.ObjectType)
    if obj_type == "Model":
        return _explorer_key("model")
    if obj_type == "Table":
        return _explorer_key("table", name=obj.Name)
    if obj_type == "Column":
        return _explorer_key("column", obj.Table.Name, name=obj.Name)
    if obj_type == "Measure":
        return _explorer_key("measure", obj.Table.Name, name=obj.Name)
    if obj_type == "Hierarchy":
        return _explorer_key("hierarchy", obj.Table.Name, name=obj.Name)
    if obj_type == "Level":
        return _explorer_key(
            "level", obj.Hierarchy.Table.Name, obj.Hierarchy.Name, obj.Name
        )
    return None


# ---------------------------------------------------------------------------
# Property extraction
# ---------------------------------------------------------------------------
# Each spec is (property, label, group, editable[, kind]). ``property`` may be a
# dotted path (e.g. ``CalculationGroup.Precedence``). The kind is inferred from
# the value (bool / number / enum / text) unless given explicitly. Properties
# the object does not expose are skipped.
_G_GENERAL = "General"
_G_FORMAT = "Formatting"
_G_BEHAVIOR = "Behavior"
_G_SOURCE = "Source"
_G_STATE = "State"
_G_LINEAGE = "Lineage"
_G_TIMELINE = "Timeline"

_LINEAGE_SPECS = (
    ("LineageTag", "Lineage tag", _G_LINEAGE, False),
    ("SourceLineageTag", "Source lineage tag", _G_LINEAGE, False),
)
_TIMELINE_SPECS = (
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
    ("StructureModifiedTime", "Structure modified", _G_TIMELINE, False),
    ("RefreshedTime", "Refreshed", _G_TIMELINE, False),
)
_STATE_SPECS = (
    ("State", "State", _G_STATE, False),
    ("ErrorMessage", "Error message", _G_STATE, False),
)

_MODEL_SPECS = (
    ("Name", "Name", _G_GENERAL, False),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("Culture", "Culture", _G_GENERAL, False),
    ("Collation", "Collation", _G_GENERAL, False),
    ("DefaultMode", "Default mode", _G_BEHAVIOR, True),
    ("DiscourageImplicitMeasures", "Discourage implicit measures", _G_BEHAVIOR, True),
    ("ValueFilterBehavior", "Value filter behavior", _G_BEHAVIOR, True),
    ("DirectLakeBehavior", "Direct Lake behavior", _G_BEHAVIOR, True),
    ("MaxParallelismPerQuery", "Max parallelism / query", _G_BEHAVIOR, True),
    ("DataSourceDefaultMaxConnections", "Max connections", _G_BEHAVIOR, True),
    ("SourceQueryCulture", "Source query culture", _G_BEHAVIOR, True),
    (
        "DefaultPowerBIDataSourceVersion",
        "Power BI data source version",
        _G_SOURCE,
        False,
    ),
    ("StorageLocation", "Storage location", _G_SOURCE, False),
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
    ("StructureModifiedTime", "Structure modified", _G_TIMELINE, False),
)

_DATABASE_SPECS = (
    ("Database.ID", "ID", "Database", False),
    ("Database.Name", "Name", "Database", False),
    ("Database.CompatibilityLevel", "Compatibility level", "Database", False),
    ("Database.CompatibilityMode", "Compatibility mode", "Database", False),
    ("Database.EstimatedSize", "Estimated size", "Database", False, "bytes"),
    ("Database.CreatedTimestamp", "Created", "Database", False),
    ("Database.LastUpdate", "Last update", "Database", False),
    ("Database.LastProcessed", "Last processed", "Database", False),
    ("Database.LastSchemaUpdate", "Last schema update", "Database", False),
)

_TABLE_SPECS = (
    ("Name", "Name", _G_GENERAL, True),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("IsHidden", "Hidden", _G_GENERAL, True),
    ("DataCategory", "Data category", _G_GENERAL, True),
    ("CalculationGroup.Precedence", "Precedence", _G_GENERAL, True),
    ("IsPrivate", "Private", _G_BEHAVIOR, True),
    ("ExcludeFromModelRefresh", "Exclude from refresh", _G_BEHAVIOR, True),
    ("ShowAsVariationsOnly", "Variations only", _G_BEHAVIOR, True),
    (
        "ExcludeFromAutomaticAggregations",
        "Exclude from auto aggregations",
        _G_BEHAVIOR,
        True,
    ),
    ("AlternateSourcePrecedence", "Alternate source precedence", _G_BEHAVIOR, True),
    ("SystemManaged", "System managed", _G_BEHAVIOR, False),
    *_LINEAGE_SPECS,
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
    ("StructureModifiedTime", "Structure modified", _G_TIMELINE, False),
)

_COLUMN_SPECS = (
    ("Name", "Name", _G_GENERAL, True),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("Type", "Column type", _G_GENERAL, False),
    ("DataType", "Data type", _G_GENERAL, True),
    ("DataCategory", "Data category", _G_GENERAL, True),
    ("IsHidden", "Hidden", _G_GENERAL, True),
    ("DisplayFolder", "Display folder", _G_FORMAT, True),
    ("FormatString", "Format string", _G_FORMAT, True),
    ("SummarizeBy", "Summarize by", _G_FORMAT, True),
    ("SortByColumn", "Sort by column", _G_FORMAT, True, "column_ref"),
    ("Alignment", "Alignment", _G_FORMAT, True),
    ("SourceColumn", "Source column", _G_SOURCE, True),
    ("SourceProviderType", "Source provider type", _G_SOURCE, False),
    ("IsKey", "Key", _G_BEHAVIOR, True),
    ("IsNullable", "Nullable", _G_BEHAVIOR, True),
    ("IsUnique", "Unique", _G_BEHAVIOR, True),
    ("IsAvailableInMDX", "Available in MDX", _G_BEHAVIOR, True),
    ("IsDefaultLabel", "Default label", _G_BEHAVIOR, True),
    ("IsDefaultImage", "Default image", _G_BEHAVIOR, True),
    ("KeepUniqueRows", "Keep unique rows", _G_BEHAVIOR, True),
    ("EncodingHint", "Encoding hint", _G_BEHAVIOR, True),
    ("IsDataTypeInferred", "Data type inferred", _G_BEHAVIOR, False),
    ("DisplayOrdinal", "Display ordinal", _G_BEHAVIOR, False),
    ("TableDetailPosition", "Table detail position", _G_BEHAVIOR, False),
    *_STATE_SPECS,
    *_LINEAGE_SPECS,
    *_TIMELINE_SPECS,
)

_MEASURE_SPECS = (
    ("Name", "Name", _G_GENERAL, True),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("IsHidden", "Hidden", _G_GENERAL, True),
    ("DataType", "Data type", _G_GENERAL, False),
    ("DataCategory", "Data category", _G_GENERAL, True),
    ("IsSimpleMeasure", "Simple measure", _G_GENERAL, False),
    ("DisplayFolder", "Display folder", _G_FORMAT, True),
    ("FormatString", "Format string", _G_FORMAT, True),
    ("KPI.Description", "KPI description", "KPI", False),
    ("KPI.TargetExpression", "Target expression", "KPI", False),
    ("KPI.StatusExpression", "Status expression", "KPI", False),
    ("KPI.StatusGraphic", "Status graphic", "KPI", False),
    ("KPI.TrendExpression", "Trend expression", "KPI", False),
    ("KPI.TrendGraphic", "Trend graphic", "KPI", False),
    *_STATE_SPECS,
    *_LINEAGE_SPECS,
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
    ("StructureModifiedTime", "Structure modified", _G_TIMELINE, False),
)

_HIERARCHY_SPECS = (
    ("Name", "Name", _G_GENERAL, True),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("IsHidden", "Hidden", _G_GENERAL, True),
    ("DisplayFolder", "Display folder", _G_FORMAT, True),
    ("HideMembers", "Hide members", _G_BEHAVIOR, True),
    ("State", "State", _G_STATE, False),
    *_LINEAGE_SPECS,
    *_TIMELINE_SPECS,
)

_LEVEL_SPECS = (
    ("Name", "Name", _G_GENERAL, True),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("Ordinal", "Ordinal", _G_GENERAL, False),
    ("Column", "Column", _G_GENERAL, False),
    *_LINEAGE_SPECS,
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
)

_CALCULATION_ITEM_SPECS = (
    ("Name", "Name", _G_GENERAL, True),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("Ordinal", "Ordinal", _G_GENERAL, True),
    *_STATE_SPECS,
    *_LINEAGE_SPECS,
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
)

_PARTITION_SPECS = (
    ("Name", "Name", _G_GENERAL, True),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("Mode", "Mode", _G_GENERAL, True),
    ("SourceType", "Source type", _G_SOURCE, False),
    ("Source.EntityName", "Entity name", _G_SOURCE, True),
    ("Source.SchemaName", "Schema name", _G_SOURCE, True),
    ("Source.ExpressionSource", "Expression source", _G_SOURCE, False),
    ("QueryGroup", "Query group", _G_SOURCE, False),
    ("DataView", "Data view", _G_SOURCE, False),
    *_STATE_SPECS,
    *_LINEAGE_SPECS,
    *_TIMELINE_SPECS,
)

_RELATIONSHIP_SPECS = (
    ("Name", "Name", _G_GENERAL, False),
    ("FromTable", "From table", _G_GENERAL, False),
    ("FromColumn", "From column", _G_GENERAL, False),
    ("FromCardinality", "From cardinality", _G_GENERAL, True),
    ("ToTable", "To table", _G_GENERAL, False),
    ("ToColumn", "To column", _G_GENERAL, False),
    ("ToCardinality", "To cardinality", _G_GENERAL, True),
    ("IsActive", "Active", _G_BEHAVIOR, True),
    ("CrossFilteringBehavior", "Cross filtering", _G_BEHAVIOR, True),
    ("SecurityFilteringBehavior", "Security filtering", _G_BEHAVIOR, True),
    (
        "RelyOnReferentialIntegrity",
        "Rely on referential integrity",
        _G_BEHAVIOR,
        True,
    ),
    ("JoinOnDateBehavior", "Join on date behavior", _G_BEHAVIOR, True),
    ("State", "State", _G_STATE, False),
    *_LINEAGE_SPECS,
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
)

_EXPRESSION_SPECS = (
    ("Name", "Name", _G_GENERAL, True),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("Kind", "Kind", _G_GENERAL, False),
    *_LINEAGE_SPECS,
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
)

_FUNCTION_SPECS = (
    ("Name", "Name", _G_GENERAL, True),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("IsHidden", "Hidden", _G_GENERAL, True),
    *_STATE_SPECS,
    *_LINEAGE_SPECS,
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
    ("StructureModifiedTime", "Structure modified", _G_TIMELINE, False),
)

_PERSPECTIVE_SPECS = (
    ("Name", "Name", _G_GENERAL, True),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
)

_ROLE_SPECS = (
    ("Name", "Name", _G_GENERAL, True),
    ("Description", "Description", _G_GENERAL, True, "longtext"),
    ("ModelPermission", "Model permission", _G_GENERAL, True),
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
)

_CULTURE_SPECS = (
    ("Name", "Name", _G_GENERAL, False),
    ("ModifiedTime", "Modified", _G_TIMELINE, False),
    ("StructureModifiedTime", "Structure modified", _G_TIMELINE, False),
)


def _get_path(obj, path: str):
    """Reads a (dotted) attribute.

    Raises AttributeError when the object does not expose it, including when an
    intermediate object is absent (e.g. ``CalculationGroup`` on a regular
    table), so the property is skipped rather than shown as blank.
    """

    current = obj
    for part in path.split("."):
        if current is None:
            raise AttributeError(path)
        current = getattr(current, part)
    return current


def _is_enum(value) -> bool:
    try:
        import System

        return isinstance(value, System.Enum)
    except Exception:
        return False


def _to_py(value):
    """Converts a .NET value to a JSON-friendly Python value."""

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if _is_enum(value):
        return str(value)
    try:
        import System

        if isinstance(value, System.DateTime):
            if value.Year <= 1700:
                return None
            return value.ToString("yyyy-MM-dd HH:mm:ss")
    except Exception:
        pass
    name = getattr(value, "Name", None)
    if isinstance(name, str):
        return name
    return str(value)


def _props(obj, obj_type: str, specs, ctx: dict) -> Tuple[dict, list]:
    """Extracts an object's property values for the detail pane.

    To keep the synced widget state small, property metadata (label, group,
    kind, enum type) is registered once per object type in ``ctx["specs"]``
    and enum option lists once per enum in ``ctx["enums"]``. Per object only
    the non-empty values are returned, plus the editable properties the object
    does not support (so they are not offered for editing).
    """

    registry = ctx["specs"].setdefault(obj_type, {})
    values: dict = {}
    absent: list = []
    for spec in specs:
        prop, label, group, editable = spec[:4]
        entry = registry.setdefault(
            prop,
            [
                prop,
                label,
                group,
                bool(editable),
                spec[4] if len(spec) > 4 else None,
                None,
            ],
        )
        try:
            raw = _get_path(obj, prop)
        except Exception:
            if editable:
                absent.append(prop)
            continue
        value = _to_py(raw)
        if entry[4] is None and value is not None:
            if _is_enum(raw):
                enum_type = raw.GetType()
                entry[4], entry[5] = "enum", str(enum_type.Name)
                if entry[5] not in ctx["enums"]:
                    import System

                    ctx["enums"][entry[5]] = [
                        str(n) for n in System.Enum.GetNames(enum_type)
                    ]
            elif isinstance(value, bool):
                entry[4] = "bool"
            elif isinstance(value, (int, float)):
                entry[4] = "number"
            elif value != "":
                entry[4] = "text"
        if value is None or value == "" or value is False:
            continue
        values[prop] = value
    return values, absent


def _spec_payload(registry: Dict[str, dict]) -> Dict[str, list]:
    """The per-type property specs as sent to the frontend."""

    return {
        obj_type: [
            entry[:4] + [entry[4] or "text", entry[5]] for entry in specs.values()
        ]
        for obj_type, specs in registry.items()
    }


def _annotations(obj) -> Dict[str, str]:
    try:
        return {str(a.Name): str(a.Value or "") for a in obj.Annotations}
    except Exception:
        return {}


def _extended_properties(obj) -> Dict[str, str]:
    try:
        return {str(p.Name): str(_to_py(p.Value) or "") for p in obj.ExtendedProperties}
    except Exception:
        return {}


def _collect_translations(model) -> Dict[str, Dict[str, Dict[str, str]]]:
    """Maps object key -> culture -> {caption, description, displayFolder}."""

    names = {
        "Caption": "caption",
        "Description": "description",
        "DisplayFolder": "displayFolder",
    }
    result: Dict[str, Dict[str, Dict[str, str]]] = {}
    for culture in model.Cultures:
        for translation in culture.ObjectTranslations:
            try:
                key = _object_key(translation.Object)
            except Exception:
                key = None
            prop = names.get(str(translation.Property))
            if key is None or prop is None:
                continue
            entry = result.setdefault(key, {}).setdefault(str(culture.Name), {})
            entry[prop] = str(translation.Value or "")
    return result


def _collect_perspective_membership(model) -> Tuple[Dict[str, List[str]], Dict]:
    """Maps object key -> perspective names, plus per-perspective counts."""

    membership: Dict[str, List[str]] = {}
    counts: Dict[str, Dict[str, int]] = {}
    for perspective in model.Perspectives:
        p_name = str(perspective.Name)
        c = {"tables": 0, "columns": 0, "measures": 0, "hierarchies": 0}
        for pt in perspective.PerspectiveTables:
            table = str(pt.Table.Name)
            c["tables"] += 1
            membership.setdefault(_explorer_key("table", name=table), []).append(p_name)
            # A table which includes all of its fields lists none of them.
            if bool(getattr(pt, "IncludeAll", False)):
                members = {
                    "column": [
                        col.Name
                        for col in pt.Table.Columns
                        if str(col.Type) != "RowNumber"
                    ],
                    "measure": [m.Name for m in pt.Table.Measures],
                    "hierarchy": [h.Name for h in pt.Table.Hierarchies],
                }
            else:
                members = {
                    "column": [pc.Column.Name for pc in pt.PerspectiveColumns],
                    "measure": [pm.Measure.Name for pm in pt.PerspectiveMeasures],
                    "hierarchy": [
                        ph.Hierarchy.Name for ph in pt.PerspectiveHierarchies
                    ],
                }
            for kind, count_key in (
                ("column", "columns"),
                ("measure", "measures"),
                ("hierarchy", "hierarchies"),
            ):
                c[count_key] += len(members[kind])
                for name in members[kind]:
                    membership.setdefault(
                        _explorer_key(kind, table, name=str(name)), []
                    ).append(p_name)
        counts[p_name] = c
    return membership, counts


def _record_error(errors: list, node: dict, obj) -> None:
    state = str(getattr(obj, "State", "") or "")
    message = str(getattr(obj, "ErrorMessage", "") or "")
    if (state and state not in ("Ready", "None")) or message:
        errors.append(
            {
                "key": _explorer_key(
                    node["type"],
                    node.get("table", ""),
                    node.get("hierarchy", ""),
                    node["name"],
                ),
                "type": node["type"],
                "table": node.get("table", ""),
                "name": node["name"],
                "state": state,
                "message": message,
            }
        )


def _base_node(
    obj_type: str,
    obj,
    specs,
    ctx: dict,
    table: str = "",
    hierarchy: str = "",
    name: Optional[str] = None,
) -> dict:
    # Only non-empty fields are sent; the frontend fills in the defaults and
    # derives the key from the type and names.
    name = str(obj.Name) if name is None else name
    key = _explorer_key(obj_type, table, hierarchy, name)
    values, absent = _props(obj, obj_type, specs, ctx)
    node = {"type": obj_type, "name": name, "v": values}
    if table:
        node["table"] = table
    if hierarchy:
        node["hierarchy"] = hierarchy
    if absent:
        node["x"] = absent
    for field, value in (
        ("annotations", _annotations(obj)),
        ("extended_properties", _extended_properties(obj)),
        ("translations", ctx["translations"].get(key)),
        ("perspectives", sorted(ctx["membership"].get(key, []))),
    ):
        if value:
            node[field] = value
    if getattr(obj, "IsHidden", None) is True:
        node["hidden"] = True
    return node


def _set_expressions(node: dict, **expressions) -> None:
    """Stores an object's non-empty DAX / M expressions (by property name)."""

    values = {prop: str(value) for prop, value in expressions.items() if value}
    if values:
        node["e"] = values


def _table_kind(tom, table) -> str:
    from sempy_labs.semantic_model._perspective_editor import _perspective_table_kind

    try:
        return _perspective_table_kind(tom, table)
    except Exception:
        return "calculation_group" if table.CalculationGroup is not None else "table"


def _partition_node(table_name: str, partition, ctx: dict) -> dict:
    node = _base_node("partition", partition, _PARTITION_SPECS, ctx, table_name)
    source_type = str(partition.SourceType)
    node["source_type"] = source_type
    node["mode"] = str(partition.Mode)
    if source_type == "Query":
        try:
            node["data_source"] = str(partition.Source.DataSource.Name)
        except Exception:
            pass
    if source_type in ("M", "Calculated"):
        _set_expressions(node, Expression=partition.Source.Expression)
    _record_error(ctx["errors"], node, partition)
    return node


def _table_node(tom, table, ctx: dict) -> dict:
    t_name = str(table.Name)
    node = _base_node("table", table, _TABLE_SPECS, ctx)
    node["kind"] = _table_kind(tom, table)
    columns = []
    for column in table.Columns:
        c_type = str(column.Type)
        if c_type == "RowNumber":
            continue
        c_node = _base_node("column", column, _COLUMN_SPECS, ctx, t_name)
        c_node["column_type"] = c_type
        if c_type == "Calculated":
            _set_expressions(c_node, Expression=column.Expression)
        _record_error(ctx["errors"], c_node, column)
        columns.append(c_node)
    node["columns"] = columns

    measures = []
    for measure in table.Measures:
        m_node = _base_node("measure", measure, _MEASURE_SPECS, ctx, t_name)
        _set_expressions(
            m_node,
            Expression=measure.Expression,
            FormatStringExpression=getattr(
                measure.FormatStringDefinition, "Expression", ""
            ),
            DetailRowsExpression=getattr(
                measure.DetailRowsDefinition, "Expression", ""
            ),
        )
        _record_error(ctx["errors"], m_node, measure)
        measures.append(m_node)
    node["measures"] = measures

    hierarchies = []
    for hierarchy in table.Hierarchies:
        h_node = _base_node("hierarchy", hierarchy, _HIERARCHY_SPECS, ctx, t_name)
        levels = sorted(hierarchy.Levels, key=lambda lv: lv.Ordinal)
        h_node["levels"] = [
            dict(
                _base_node(
                    "level", level, _LEVEL_SPECS, ctx, t_name, str(hierarchy.Name)
                ),
                column=str(level.Column.Name) if level.Column is not None else "",
            )
            for level in levels
        ]
        hierarchies.append(h_node)
    node["hierarchies"] = hierarchies

    items = []
    if table.CalculationGroup is not None:
        for item in table.CalculationGroup.CalculationItems:
            i_node = _base_node(
                "calculationItem", item, _CALCULATION_ITEM_SPECS, ctx, t_name
            )
            _set_expressions(
                i_node,
                Expression=item.Expression,
                FormatStringExpression=getattr(
                    item.FormatStringDefinition, "Expression", ""
                ),
            )
            _record_error(ctx["errors"], i_node, item)
            items.append(i_node)
    node["calculation_items"] = items

    partitions = [_partition_node(t_name, p, ctx) for p in table.Partitions]
    node["partitions"] = partitions
    modes = sorted({p["mode"] for p in partitions})
    node["mode"] = modes[0] if len(modes) == 1 else ("Hybrid" if modes else "")
    # A calculated table's DAX lives on (and is edited through) its partition.
    calc_partition = next(
        (p for p in partitions if p["source_type"] == "Calculated"), None
    )
    if calc_partition is not None and node["kind"] != "calculation_group":
        node["calc_partition"] = calc_partition["name"]
    return node


def _relationship_node(rel, ctx: dict) -> dict:
    node = _base_node("relationship", rel, _RELATIONSHIP_SPECS, ctx)
    node["from_table"] = str(rel.FromTable.Name)
    node["from_column"] = str(rel.FromColumn.Name)
    node["to_table"] = str(rel.ToTable.Name)
    node["to_column"] = str(rel.ToColumn.Name)
    return node


def _role_node(role, ctx: dict) -> dict:
    node = _base_node("role", role, _ROLE_SPECS, ctx)
    node["members"] = [str(m.MemberName) for m in role.Members]
    permissions = []
    for tp in role.TablePermissions:
        columns = []
        try:
            for cp in tp.ColumnPermissions:
                columns.append(
                    {
                        "column": str(cp.Column.Name),
                        "permission": str(cp.MetadataPermission),
                    }
                )
        except Exception:
            pass
        permissions.append(
            {
                "table": str(tp.Table.Name),
                "filter": str(tp.FilterExpression or ""),
                "permission": str(getattr(tp, "MetadataPermission", "Default")),
                "columns": columns,
            }
        )
    node["table_permissions"] = permissions
    return node


def _model_payload(tom) -> dict:
    """Builds the full explorer payload for the connected semantic model."""

    model = tom.model
    membership, perspective_counts = _collect_perspective_membership(model)
    ctx = {
        "enums": {},
        "specs": {},
        "errors": [],
        "translations": _collect_translations(model),
        "membership": membership,
    }

    model_node = _base_node(
        "model", model, _MODEL_SPECS + _DATABASE_SPECS, ctx, name="Model"
    )

    tables = [_table_node(tom, t, ctx) for t in model.Tables]
    relationships = [_relationship_node(r, ctx) for r in model.Relationships]

    expressions = []
    for e in model.Expressions:
        node = _base_node("expression", e, _EXPRESSION_SPECS, ctx)
        node["lang"] = "dax" if str(e.Kind) == "DAX" else "m"
        _set_expressions(node, Expression=e.Expression)
        expressions.append(node)

    functions = []
    try:
        for f in model.Functions:
            node = _base_node("function", f, _FUNCTION_SPECS, ctx)
            _set_expressions(node, Expression=f.Expression)
            _record_error(ctx["errors"], node, f)
            functions.append(node)
    except Exception:
        pass

    perspectives = []
    for p in model.Perspectives:
        node = _base_node("perspective", p, _PERSPECTIVE_SPECS, ctx)
        node["counts"] = perspective_counts.get(str(p.Name), {})
        perspectives.append(node)

    roles = [_role_node(r, ctx) for r in model.Roles]

    cultures = []
    for c in model.Cultures:
        node = _base_node("culture", c, _CULTURE_SPECS, ctx)
        node["translated_count"] = int(c.ObjectTranslations.Count)
        try:
            node["has_linguistic_metadata"] = c.LinguisticMetadata is not None
        except Exception:
            node["has_linguistic_metadata"] = False
        cultures.append(node)

    try:
        compatibility_level = int(model.Database.CompatibilityLevel)
    except Exception:
        compatibility_level = 0

    return {
        "model": model_node,
        "tables": tables,
        "relationships": relationships,
        "expressions": expressions,
        "functions": functions,
        "perspectives": perspectives,
        "roles": roles,
        "cultures": cultures,
        "errors": ctx["errors"],
        "enums": ctx["enums"],
        "specs": _spec_payload(ctx["specs"]),
        "compatibility_level": compatibility_level,
        "is_direct_lake": any(
            any(p["mode"] == "DirectLake" for p in t["partitions"]) for t in tables
        ),
    }


def _collect_explorer_state(
    dataset: str | UUID, workspace: Optional[str | UUID]
) -> dict:
    from sempy_labs.tom import connect_semantic_model

    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:
        payload = _model_payload(tom)
        return {
            "dataset_name": str(getattr(tom, "_dataset_name", "") or dataset),
            "dataset_id": str(getattr(tom, "_dataset_id", "") or dataset),
            "workspace_name": str(getattr(tom, "_workspace_name", "") or ""),
            "workspace_id": str(getattr(tom, "_workspace_id", "") or workspace or ""),
            "payload": payload,
        }


# ---------------------------------------------------------------------------
# Applying staged changes
# ---------------------------------------------------------------------------
def _resolve_object(model, target: dict):
    """Finds a TOM object from an explorer target (saved names)."""

    obj_type = target.get("type")
    table = target.get("table") or ""
    hierarchy = target.get("hierarchy") or ""
    name = target.get("name") or ""
    try:
        if obj_type == "model":
            return model
        if obj_type == "table":
            return model.Tables[name]
        if obj_type == "column":
            return model.Tables[table].Columns[name]
        if obj_type == "measure":
            return model.Tables[table].Measures[name]
        if obj_type == "hierarchy":
            return model.Tables[table].Hierarchies[name]
        if obj_type == "level":
            return model.Tables[table].Hierarchies[hierarchy].Levels[name]
        if obj_type == "calculationItem":
            return model.Tables[table].CalculationGroup.CalculationItems[name]
        if obj_type == "partition":
            return model.Tables[table].Partitions[name]
        if obj_type == "relationship":
            return model.Relationships[name]
        if obj_type == "expression":
            return model.Expressions[name]
        if obj_type == "function":
            return model.Functions[name]
        if obj_type == "perspective":
            return model.Perspectives[name]
        if obj_type == "role":
            return model.Roles[name]
        if obj_type == "culture":
            return model.Cultures[name]
    except Exception:
        pass
    label = f"'{table}'[{name}]" if table and obj_type != "table" else f"'{name}'"
    raise ValueError(f"The {obj_type} {label} was not found in the semantic model.")


def _convert_value(prop_type, value):
    """Converts a JSON value to the .NET type of the property being set."""

    import System

    if value is None:
        return None
    underlying = System.Nullable.GetUnderlyingType(prop_type) or prop_type
    if underlying.IsEnum:
        return System.Enum.Parse(underlying, str(value), True)
    full_name = str(underlying.FullName)
    if full_name == "System.Boolean":
        if isinstance(value, str):
            return value.strip().lower() == "true"
        return bool(value)
    if full_name in ("System.Int32", "System.Int64", "System.Int16"):
        return int(value)
    if full_name in ("System.Double", "System.Single", "System.Decimal"):
        return float(value)
    if full_name == "System.String":
        return str(value)
    raise ValueError(f"Unsupported property type '{full_name}'.")


def _json_value(value, default):
    if value is None or value == "":
        return default
    if isinstance(value, str):
        return json.loads(value)
    return value


def _set_property(obj, path: str, value) -> None:
    """Sets a (dotted) property on a TOM object using reflection for typing."""

    parts = path.split(".")
    target = obj
    for part in parts[:-1]:
        target = getattr(target, part)
        if target is None:
            raise ValueError(f"'{path}' cannot be set on this object.")
    attr = parts[-1]
    prop_info = target.GetType().GetProperty(attr)
    if prop_info is None or not prop_info.CanWrite:
        raise ValueError(f"The '{path}' property is not writable.")
    setattr(target, attr, _convert_value(prop_info.PropertyType, value))


def _perspective_member_types():
    import Microsoft.AnalysisServices.Tabular as TOM

    return {
        "Column": ("PerspectiveColumns", TOM.PerspectiveColumn, "Column", "Columns"),
        "Measure": (
            "PerspectiveMeasures",
            TOM.PerspectiveMeasure,
            "Measure",
            "Measures",
        ),
        "Hierarchy": (
            "PerspectiveHierarchies",
            TOM.PerspectiveHierarchy,
            "Hierarchy",
            "Hierarchies",
        ),
    }


def _materialize_include_all(perspective_table, table) -> None:
    """Turns an IncludeAll perspective table into explicit members."""

    perspective_table.IncludeAll = False
    for (
        members_attr,
        member_cls,
        ref_attr,
        source_attr,
    ) in _perspective_member_types().values():
        members = getattr(perspective_table, members_attr)
        for source in getattr(table, source_attr):
            if source_attr == "Columns" and str(source.Type) == "RowNumber":
                continue
            if not members.ContainsName(source.Name):
                member = member_cls()
                setattr(member, ref_attr, source)
                members.Add(member)


def _set_perspective_membership(model, obj, wanted: set) -> None:
    """Adds / removes an object so it belongs to exactly ``wanted`` perspectives.

    Idempotent (members already present are left alone) and aware of
    perspective tables which include all of their fields.
    """

    import Microsoft.AnalysisServices.Tabular as TOM

    obj_type = str(obj.ObjectType)
    for perspective in model.Perspectives:
        want = str(perspective.Name) in wanted
        tables = perspective.PerspectiveTables
        if obj_type == "Table":
            has = tables.ContainsName(obj.Name)
            if want and not has:
                pt = TOM.PerspectiveTable()
                pt.Table = obj
                tables.Add(pt)
            elif not want and has:
                tables.Remove(tables[obj.Name])
            continue

        if obj_type not in _perspective_member_types():
            raise ValueError(f"A {obj_type.lower()} cannot belong to a perspective.")
        members_attr, member_cls, ref_attr, _ = _perspective_member_types()[obj_type]
        table = obj.Table
        pt = tables[table.Name] if tables.ContainsName(table.Name) else None
        if pt is not None and bool(getattr(pt, "IncludeAll", False)):
            if want:
                continue
            _materialize_include_all(pt, table)
        members = getattr(pt, members_attr) if pt is not None else None
        has = members is not None and members.ContainsName(obj.Name)
        if want and not has:
            if pt is None:
                pt = TOM.PerspectiveTable()
                pt.Table = table
                tables.Add(pt)
                members = getattr(pt, members_attr)
            member = member_cls()
            setattr(member, ref_attr, obj)
            members.Add(member)
        elif not want and has:
            members.Remove(members[obj.Name])


def _apply_edit(tom, obj, prop: str, value) -> None:
    import Microsoft.AnalysisServices.Tabular as TOM

    model = tom.model
    obj_type = str(obj.ObjectType)

    if prop == "Annotations":
        obj.Annotations.Clear()
        for k, v in _json_value(value, {}).items():
            ann = TOM.Annotation()
            ann.Name = str(k)
            ann.Value = str(v)
            obj.Annotations.Add(ann)
        return

    if prop == "Translations":
        props = {
            "caption": TOM.TranslatedProperty.Caption,
            "description": TOM.TranslatedProperty.Description,
            "displayFolder": TOM.TranslatedProperty.DisplayFolder,
        }
        for culture_name, values in _json_value(value, {}).items():
            if not model.Cultures.ContainsName(culture_name):
                continue
            translations = model.Cultures[culture_name].ObjectTranslations
            for field, tp in props.items():
                if field not in values:
                    continue
                text = values.get(field) or ""
                if text:
                    translations.SetTranslation(obj, tp, text)
                else:
                    existing = translations[obj, tp]
                    if existing is not None:
                        translations.Remove(existing)
        return

    if prop == "Perspectives":
        _set_perspective_membership(model, obj, set(_json_value(value, [])))
        return

    if prop == "Members" and obj_type == "Role":
        import System

        wanted = [str(m).strip() for m in _json_value(value, []) if str(m).strip()]
        current = {str(m.MemberName): m for m in obj.Members}
        for member_name, member in current.items():
            if member_name not in wanted:
                obj.Members.Remove(member)
        for member_name in wanted:
            if member_name not in current:
                member = TOM.ExternalModelRoleMember()
                member.IdentityProvider = "AzureAD"
                member.MemberName = member_name
                member.MemberType = System.Enum.Parse(TOM.RoleMemberType, "User")
                obj.Members.Add(member)
        return

    if prop == "RlsFilters" and obj_type == "Role":
        for table_name, filter_expression in _json_value(value, {}).items():
            if not model.Tables.ContainsName(table_name):
                continue
            if obj.TablePermissions.ContainsName(table_name):
                tp = obj.TablePermissions[table_name]
            elif filter_expression:
                tp = TOM.TablePermission()
                tp.Table = model.Tables[table_name]
                obj.TablePermissions.Add(tp)
            else:
                continue
            tp.FilterExpression = filter_expression or None
        return

    if prop == "Ols" and obj_type == "Role":
        _set_object_level_security(model, obj, _json_value(value, []))
        return

    if prop in ("FromColumn", "ToColumn") and obj_type == "Relationship":
        table = obj.FromTable if prop == "FromColumn" else obj.ToTable
        if not table.Columns.ContainsName(str(value)):
            raise ValueError(f"The '{table.Name}'[{value}] column was not found.")
        setattr(obj, prop, table.Columns[str(value)])
        return

    if prop == "FormatStringExpression":
        if value:
            definition = TOM.FormatStringDefinition()
            definition.Expression = str(value)
            obj.FormatStringDefinition = definition
        else:
            obj.FormatStringDefinition = None
        return

    if prop == "DetailRowsExpression":
        if value:
            definition = TOM.DetailRowsDefinition()
            definition.Expression = str(value)
            obj.DetailRowsDefinition = definition
        else:
            obj.DetailRowsDefinition = None
        return

    if prop == "Expression" and obj_type == "Partition":
        obj.Source.Expression = str(value or "")
        if str(obj.SourceType) == "Calculated":
            _reconcile_field_parameter_group_column(obj.Table, obj.Source.Expression)
        return

    if prop == "SortByColumn" and obj_type == "Column":
        obj.SortByColumn = obj.Table.Columns[value] if value else None
        return

    _set_property(obj, prop, value)


def _set_object_level_security(model, role, entries: list) -> None:
    """Sets a role's table / column metadata permissions (OLS).

    ``entries`` lists every non-default permission as ``{table, column,
    permission}`` (no column = the table itself); anything not listed is reset
    to ``Default``.
    """

    import System
    import Microsoft.AnalysisServices.Tabular as TOM

    def _permission(name):
        return System.Enum.Parse(TOM.MetadataPermission, str(name or "Default"))

    wanted: Dict[str, Dict[str, str]] = {}
    for entry in entries:
        table = str(entry.get("table") or "")
        if not table or not model.Tables.ContainsName(table):
            continue
        perm = str(entry.get("permission") or "Default")
        wanted.setdefault(table, {})[str(entry.get("column") or "")] = perm

    for table in model.Tables:
        name = str(table.Name)
        perms = wanted.get(name, {})
        has_tp = role.TablePermissions.ContainsName(name)
        if not perms and not has_tp:
            continue
        if has_tp:
            tp = role.TablePermissions[name]
        else:
            tp = TOM.TablePermission()
            tp.Table = table
            role.TablePermissions.Add(tp)
        tp.MetadataPermission = _permission(perms.get("", "Default"))
        columns = {c: p for c, p in perms.items() if c}
        for cp in list(tp.ColumnPermissions):
            column = str(cp.Column.Name)
            if column in columns:
                cp.MetadataPermission = _permission(columns.pop(column))
            else:
                tp.ColumnPermissions.Remove(cp)
        for column, perm in columns.items():
            if not table.Columns.ContainsName(column):
                continue
            cp = TOM.ColumnPermission()
            cp.Column = table.Columns[column]
            cp.MetadataPermission = _permission(perm)
            tp.ColumnPermissions.Add(cp)
        if (
            not perms
            and not tp.FilterExpression
            and tp.ColumnPermissions.Count == 0
            and str(tp.MetadataPermission) == "Default"
        ):
            role.TablePermissions.Remove(tp)


def _new_relationship(model, props: dict):
    """Creates a single-column relationship from a staged spec."""

    import uuid
    import System
    import Microsoft.AnalysisServices.Tabular as TOM

    def _column(table_key: str, column_key: str):
        table_name = str(props.get(table_key) or "")
        column_name = str(props.get(column_key) or "")
        if not model.Tables.ContainsName(table_name):
            raise ValueError(f"The '{table_name}' table was not found.")
        table = model.Tables[table_name]
        if not table.Columns.ContainsName(column_name):
            raise ValueError(f"The '{table_name}'[{column_name}] column was not found.")
        return table.Columns[column_name]

    rel = TOM.SingleColumnRelationship()
    rel.Name = str(uuid.uuid4())
    rel.FromColumn = _column("fromTable", "fromColumn")
    rel.ToColumn = _column("toTable", "toColumn")
    for prop, enum, default in (
        ("FromCardinality", TOM.RelationshipEndCardinality, "Many"),
        ("ToCardinality", TOM.RelationshipEndCardinality, "One"),
        ("CrossFilteringBehavior", TOM.CrossFilteringBehavior, "OneDirection"),
    ):
        setattr(rel, prop, System.Enum.Parse(enum, str(props.get(prop) or default)))
    rel.IsActive = props.get("IsActive") is not False
    model.Relationships.Add(rel)
    return rel


def _unique_name(base: str, exists) -> str:
    if not exists(base):
        return base
    i = 1
    while exists(f"{base}_{i}"):
        i += 1
    return f"{base}_{i}"


def _new_lineage_tag(obj) -> None:
    import uuid

    try:
        obj.LineageTag = str(uuid.uuid4())
    except Exception:
        pass


# Starting definition of a new named expression (matches the frontend).
_NEW_EXPRESSION = 'let\n    Source = ""\nin\n    Source'


# Marks a field parameter's "Fields" column, as Power BI does.
_FIELD_PARAMETER_METADATA = '{"version":3,"kind":2}'


def _is_field_parameter_table(table) -> bool:
    return any(
        str(prop.Name) == "ParameterMetadata"
        for column in table.Columns
        for prop in column.ExtendedProperties
    )


def _field_parameter_has_group(expression: str) -> bool:
    """True when the rows carry the optional 4th ("hierarchy" / group) value,
    e.g. ``( "Label", NAMEOF ( [Sales] ), 0, "Group" )``."""

    return bool(
        re.search(
            r'NAMEOF\s*\([^)]*\)\s*,\s*-?\d+\s*,\s*"', expression or "", re.IGNORECASE
        )
    )


def _reconcile_field_parameter_group_column(table, expression: str) -> None:
    """Adds / removes a field parameter's ``[Value4]`` group column so it
    matches whether the rows carry a group value. No-op for other tables."""

    import Microsoft.AnalysisServices.Tabular as TOM

    if not _is_field_parameter_table(table):
        return
    group = next(
        (
            c
            for c in table.Columns
            if str(c.Type) == "CalculatedTableColumn"
            and str(c.SourceColumn).lower() == "[value4]"
        ),
        None,
    )
    if _field_parameter_has_group(expression):
        if group is None:
            column = TOM.CalculatedTableColumn()
            column.Name = _unique_name(
                f"{table.Name} Group", table.Columns.ContainsName
            )
            column.SourceColumn = "[Value4]"
            column.DataType = TOM.DataType.String
            _new_lineage_tag(column)
            table.Columns.Add(column)
    elif group is not None:
        table.Columns.Remove(group)


def _new_field_parameter(model, name: str, expression: str):
    """Builds a field parameter: a calculated table listing (label, NAMEOF,
    order[, group]) rows, with a value column grouped by a hidden fields column
    (which carries the ParameterMetadata marker), both sorted by a hidden
    order column."""

    import Microsoft.AnalysisServices.Tabular as TOM

    table = TOM.Table()
    table.Name = name
    _new_lineage_tag(table)
    partition = TOM.Partition()
    partition.Name = name
    partition.Mode = TOM.ModeType.Import
    source = TOM.CalculatedPartitionSource()
    source.Expression = expression
    partition.Source = source
    table.Partitions.Add(partition)

    def _column(column_name: str, source_column: str, data_type, hidden: bool):
        column = TOM.CalculatedTableColumn()
        column.Name = column_name
        column.SourceColumn = source_column
        column.DataType = data_type
        column.IsHidden = hidden
        _new_lineage_tag(column)
        table.Columns.Add(column)
        return column

    value = _column(name, "[Value1]", TOM.DataType.String, False)
    fields = _column(f"{name} Fields", "[Value2]", TOM.DataType.String, True)
    order = _column(f"{name} Order", "[Value3]", TOM.DataType.Int64, True)
    marker = TOM.JsonExtendedProperty()
    marker.Name = "ParameterMetadata"
    marker.Value = _FIELD_PARAMETER_METADATA
    fields.ExtendedProperties.Add(marker)
    value.SortByColumn = order
    fields.SortByColumn = order
    value.RelatedColumnDetails = TOM.RelatedColumnDetails()
    group_by = TOM.GroupByColumn()
    group_by.GroupingColumn = fields
    value.RelatedColumnDetails.GroupByColumns.Add(group_by)
    _reconcile_field_parameter_group_column(table, expression)
    model.Tables.Add(table)
    return table


def _new_calculation_group(model, name: str):
    import Microsoft.AnalysisServices.Tabular as TOM

    precedence = max(
        [
            t.CalculationGroup.Precedence
            for t in model.Tables
            if t.CalculationGroup is not None
        ]
        or [-1]
    )
    table = TOM.Table()
    table.Name = name
    _new_lineage_tag(table)
    table.CalculationGroup = TOM.CalculationGroup()
    table.CalculationGroup.Precedence = precedence + 1
    name_column = TOM.DataColumn()
    name_column.Name = "Name"
    name_column.SourceColumn = "Name"
    name_column.DataType = TOM.DataType.String
    _new_lineage_tag(name_column)
    ordinal_column = TOM.DataColumn()
    ordinal_column.Name = "Ordinal"
    ordinal_column.SourceColumn = "Ordinal"
    ordinal_column.DataType = TOM.DataType.Int64
    ordinal_column.IsHidden = True
    _new_lineage_tag(ordinal_column)
    table.Columns.Add(name_column)
    table.Columns.Add(ordinal_column)
    name_column.SortByColumn = ordinal_column
    partition = TOM.Partition()
    partition.Name = name
    partition.Source = TOM.CalculationGroupSource()
    table.Partitions.Add(partition)
    model.DiscourageImplicitMeasures = True
    model.Tables.Add(table)
    return table


def _create_object(tom, spec: dict):
    """Creates a staged new object, applies its staged properties, returns it."""

    import Microsoft.AnalysisServices.Tabular as TOM

    model = tom.model
    obj_type = spec.get("type")
    name = str(spec.get("name") or "").strip()
    table_name = spec.get("table") or ""
    props = dict(spec.get("props") or {})
    if not name:
        raise ValueError(f"A new {obj_type} needs a name.")
    if obj_type == "daxlibPackage":
        _install_daxlib_package(model, name, props)
        return None
    if obj_type == "relationship":
        return _new_relationship(model, props)

    def _table():
        if not model.Tables.ContainsName(table_name):
            raise ValueError(f"The '{table_name}' table was not found.")
        return model.Tables[table_name]

    if obj_type == "measure":
        obj = TOM.Measure()
        obj.Name = name
        obj.Expression = ""
        _new_lineage_tag(obj)
        _table().Measures.Add(obj)
    elif obj_type == "column":
        obj = TOM.CalculatedColumn()
        obj.Name = name
        obj.Expression = ""
        obj.DataType = TOM.DataType.String
        _new_lineage_tag(obj)
        _table().Columns.Add(obj)
    elif obj_type == "calculationItem":
        table = _table()
        if table.CalculationGroup is None:
            raise ValueError(f"The '{table_name}' table is not a calculation group.")
        obj = TOM.CalculationItem()
        obj.Name = name
        obj.Expression = "SELECTEDMEASURE()"
        table.CalculationGroup.CalculationItems.Add(obj)
    elif obj_type == "table":
        obj = _new_calculation_group(model, name)
    elif obj_type == "fieldParameter":
        if model.Tables.ContainsName(name):
            raise ValueError(f"A table named '{name}' already exists.")
        obj = _new_field_parameter(model, name, str(props.pop("Expression", "") or ""))
    elif obj_type == "role":
        obj = TOM.ModelRole()
        obj.Name = name
        obj.ModelPermission = TOM.ModelPermission.Read
        model.Roles.Add(obj)
    elif obj_type == "perspective":
        obj = TOM.Perspective()
        obj.Name = name
        model.Perspectives.Add(obj)
    elif obj_type == "expression":
        obj = TOM.NamedExpression()
        obj.Name = name
        obj.Kind = TOM.ExpressionKind.M
        obj.Expression = _NEW_EXPRESSION
        _new_lineage_tag(obj)
        model.Expressions.Add(obj)
    else:
        raise ValueError(f"Creating a '{obj_type}' is not supported.")

    for prop, value in props.items():
        _apply_edit(tom, obj, prop, value)
    return obj


def _measure_name_taken(model, table, name: str) -> bool:
    """Measure names are model-wide and may not clash with the table's columns."""

    return any(t.Measures.ContainsName(name) for t in model.Tables) or (
        table.Columns.ContainsName(name)
    )


def _restamp_lineage_tags(table) -> None:
    for column in table.Columns:
        _new_lineage_tag(column)
    for measure in table.Measures:
        _new_lineage_tag(measure)
    for hierarchy in table.Hierarchies:
        _new_lineage_tag(hierarchy)
        for level in hierarchy.Levels:
            _new_lineage_tag(level)
    for partition in table.Partitions:
        _new_lineage_tag(partition)


def _duplicate_object(model, obj) -> None:
    obj_type = str(obj.ObjectType)
    if obj_type == "Measure":
        clone = obj.Clone()
        clone.Name = _unique_name(
            f"{obj.Name} copy", lambda n: _measure_name_taken(model, obj.Table, n)
        )
        _new_lineage_tag(clone)
        obj.Table.Measures.Add(clone)
        return
    if obj_type == "Column":
        clone = obj.Clone()
        clone.Name = _unique_name(
            f"{obj.Name} copy",
            lambda n: obj.Table.Columns.ContainsName(n)
            or obj.Table.Measures.ContainsName(n),
        )
        _new_lineage_tag(clone)
        obj.Table.Columns.Add(clone)
        return
    if obj_type == "Table":
        clone = obj.Clone()
        clone.Name = _unique_name(f"{obj.Name} copy", model.Tables.ContainsName)
        _new_lineage_tag(clone)
        _restamp_lineage_tags(clone)
        # Cloned measures keep their names, which must be unique model-wide.
        taken = set()
        for measure in clone.Measures:
            measure.Name = _unique_name(
                f"{measure.Name} copy",
                lambda n: n in taken or _measure_name_taken(model, clone, n),
            )
            taken.add(measure.Name)
        if clone.CalculationGroup is not None:
            clone.CalculationGroup.Precedence = 1 + max(
                t.CalculationGroup.Precedence
                for t in model.Tables
                if t.CalculationGroup is not None
            )
        model.Tables.Add(clone)
        return
    collections = {
        "Hierarchy": lambda: obj.Table.Hierarchies,
        "CalculationItem": lambda: obj.CalculationGroup.CalculationItems,
        "Role": lambda: model.Roles,
        "Perspective": lambda: model.Perspectives,
        "Expression": lambda: model.Expressions,
    }
    if obj_type not in collections:
        raise ValueError(f"A {obj_type.lower()} cannot be duplicated.")
    collection = collections[obj_type]()
    clone = obj.Clone()
    clone.Name = _unique_name(f"{obj.Name} copy", collection.ContainsName)
    _new_lineage_tag(clone)
    if obj_type == "Hierarchy":
        for level in clone.Levels:
            _new_lineage_tag(level)
    collection.Add(clone)


def _is_attached(model, obj) -> bool:
    """False once the object (or an ancestor) was removed from the model."""

    current = obj
    for _ in range(8):
        if current == model or str(getattr(current, "ObjectType", "")) == "Model":
            return True
        parent = getattr(current, "Parent", model)
        if parent is None:
            return False
        current = parent
    return True


def _detach_metadata(model, obj) -> None:
    """Removes an object's translations and perspective memberships."""

    import Microsoft.AnalysisServices.Tabular as TOM

    for culture in model.Cultures:
        translations = culture.ObjectTranslations
        for prop in ("Caption", "Description", "DisplayFolder"):
            try:
                existing = translations[obj, getattr(TOM.TranslatedProperty, prop)]
            except Exception:
                existing = None
            if existing is not None:
                translations.Remove(existing)
    obj_type = str(obj.ObjectType)
    member_types = _perspective_member_types()
    for perspective in model.Perspectives:
        tables = perspective.PerspectiveTables
        if obj_type == "Table":
            if tables.ContainsName(obj.Name):
                tables.Remove(tables[obj.Name])
        elif obj_type in member_types and tables.ContainsName(obj.Table.Name):
            members = getattr(tables[obj.Table.Name], member_types[obj_type][0])
            if members.ContainsName(obj.Name):
                members.Remove(members[obj.Name])


def _remove_level(model, hierarchy, level) -> None:
    _detach_metadata(model, level)
    hierarchy.Levels.Remove(level)
    for ordinal, remaining in enumerate(
        sorted(hierarchy.Levels, key=lambda lv: lv.Ordinal)
    ):
        remaining.Ordinal = ordinal


def _detach_references(model, obj) -> None:
    """Clears everything which references an object about to be deleted.

    TOM rejects SaveChanges when a deleted object is still referenced by a
    translation, perspective, relationship, sort-by column, hierarchy level or
    role permission, so those references are removed first.
    """

    obj_type = str(obj.ObjectType)
    if obj_type == "Table":
        for child in [*obj.Columns, *obj.Measures, *obj.Hierarchies]:
            _detach_metadata(model, child)
        for hierarchy in obj.Hierarchies:
            for level in hierarchy.Levels:
                _detach_metadata(model, level)
        _detach_metadata(model, obj)
        for rel in list(model.Relationships):
            if obj.Name in (rel.FromTable.Name, rel.ToTable.Name):
                model.Relationships.Remove(rel)
        for role in model.Roles:
            if role.TablePermissions.ContainsName(obj.Name):
                role.TablePermissions.Remove(role.TablePermissions[obj.Name])
    elif obj_type == "Column":
        table = obj.Table
        _detach_metadata(model, obj)
        for rel in list(model.Relationships):
            ends = (
                (rel.FromTable.Name, rel.FromColumn.Name),
                (rel.ToTable.Name, rel.ToColumn.Name),
            )
            if (table.Name, obj.Name) in ends:
                model.Relationships.Remove(rel)
        for column in table.Columns:
            sort_by = column.SortByColumn
            if sort_by is not None and sort_by.Name == obj.Name:
                column.SortByColumn = None
        for hierarchy in list(table.Hierarchies):
            for level in list(hierarchy.Levels):
                if level.Column is not None and level.Column.Name == obj.Name:
                    _remove_level(model, hierarchy, level)
            if hierarchy.Levels.Count == 0:
                _detach_metadata(model, hierarchy)
                table.Hierarchies.Remove(hierarchy)
        for role in model.Roles:
            if role.TablePermissions.ContainsName(table.Name):
                permissions = role.TablePermissions[table.Name].ColumnPermissions
                if permissions.ContainsName(obj.Name):
                    permissions.Remove(permissions[obj.Name])
    elif obj_type == "Hierarchy":
        for level in obj.Levels:
            _detach_metadata(model, level)
        _detach_metadata(model, obj)
    elif obj_type in ("Measure", "Level"):
        _detach_metadata(model, obj)


def _delete_object(model, obj) -> None:
    # A child of an object removed earlier in the batch is already gone.
    if not _is_attached(model, obj):
        return
    obj_type = str(obj.ObjectType)
    if obj_type == "Level":
        _remove_level(model, obj.Hierarchy, obj)
        return
    _detach_references(model, obj)
    removers = {
        "Table": lambda: model.Tables.Remove(obj),
        "Column": lambda: obj.Table.Columns.Remove(obj),
        "Measure": lambda: obj.Table.Measures.Remove(obj),
        "Hierarchy": lambda: obj.Table.Hierarchies.Remove(obj),
        "Level": lambda: obj.Hierarchy.Levels.Remove(obj),
        "Partition": lambda: obj.Table.Partitions.Remove(obj),
        "CalculationItem": lambda: obj.CalculationGroup.CalculationItems.Remove(obj),
        "Relationship": lambda: model.Relationships.Remove(obj),
        "Role": lambda: model.Roles.Remove(obj),
        "Perspective": lambda: model.Perspectives.Remove(obj),
        "Expression": lambda: model.Expressions.Remove(obj),
        "Function": lambda: model.Functions.Remove(obj),
        "Culture": lambda: model.Cultures.Remove(obj),
    }
    obj_type = str(obj.ObjectType)
    if obj_type not in removers:
        raise ValueError(f"A {obj_type.lower()} cannot be deleted.")
    removers[obj_type]()


def _apply_changes(tom, creates: list, edits: list, ops: list) -> int:
    """Applies staged changes in one transaction; returns the change count.

    Every edit / op target is resolved by its *saved* names before anything is
    changed, so renames staged in the same batch never break later lookups.
    On failure the local changes are discarded so nothing is half-saved.
    """

    model = tom.model
    try:
        for spec in creates:
            _create_object(tom, spec)
        resolved_edits = [(_resolve_object(model, e["target"]), e) for e in edits]
        resolved_ops = [(_resolve_object(model, o["target"]), o) for o in ops]
        # Renames go last so name-based values (sort-by column, RLS tables,
        # perspectives) still resolve against the saved names.
        resolved_edits.sort(key=lambda pair: pair[1]["property"] == "Name")
        for obj, edit in resolved_edits:
            try:
                _apply_edit(tom, obj, edit["property"], edit.get("value"))
            except Exception as e:
                label = edit.get("label") or str(obj.Name)
                raise ValueError(
                    f"Could not set '{edit['property']}' on {label}: {e}"
                ) from e
        for obj, op in resolved_ops:
            if op.get("op") == "duplicate":
                _duplicate_object(model, obj)
        for obj, op in resolved_ops:
            if op.get("op") == "delete":
                _delete_object(model, obj)
    except Exception:
        try:
            model.UndoLocalChanges()
        except Exception:
            pass
        # Closing a read-write connection saves; flag it read-only so the
        # discarded batch is never written.
        tom._readonly = True
        raise
    return len(creates) + len(edits) + len(ops)


# ---------------------------------------------------------------------------
# TMDL, dependencies and DAX formatting
# ---------------------------------------------------------------------------
def _object_tmdl(model, target: dict) -> Tuple[str, str]:
    import Microsoft.AnalysisServices.Tabular as TOM

    obj = _resolve_object(model, target)
    serializer = getattr(TOM, "TmdlSerializer", None)
    if serializer is not None:
        return str(serializer.SerializeObject(obj)), "tmdl"
    return str(TOM.JsonSerializer.SerializeObject(obj)), "json"


# DISCOVER_CALC_DEPENDENCY object types mapped to explorer object types.
_DEPENDENCY_TYPES = {
    "MEASURE": "measure",
    "COLUMN": "column",
    "CALC_COLUMN": "column",
    "TABLE": "table",
    "CALC_TABLE": "table",
    "CALCULATION_ITEM": "calculationItem",
    "HIERARCHY": "hierarchy",
    "PARTITION": "partition",
    "M_EXPRESSION": "expression",
    "FUNCTION": "function",
    "USER_DEFINED_FUNCTION": "function",
    "RELATIONSHIP": "relationship",
    "ACTIVE_RELATIONSHIP": "relationship",
}


def _dependency_node(raw_type: str, table: str, name: str) -> dict:
    raw_type = str(raw_type or "").upper()
    obj_type = _DEPENDENCY_TYPES.get(raw_type, "")
    table = str(table or "")
    name = str(name or "")
    if obj_type == "table":
        name = table or name
        table = ""
        key = _explorer_key("table", name=name)
    elif obj_type in ("relationship", "expression", "function"):
        table = ""
        key = _explorer_key(obj_type, name=name)
    elif obj_type:
        key = _explorer_key(obj_type, table, name=name)
    else:
        key = ""
    label = obj_type or raw_type.replace("_", " ").title()
    return {"key": key, "type": label, "table": table, "name": name}


def _dependency_graph(rows: List[dict]) -> dict:
    """Builds the model's dependency graph as deduplicated nodes plus edges.

    Each edge is ``[source, referenced]`` (indexes into ``nodes``). The whole
    graph is sent once so the frontend can root, re-root and reverse it
    without another round trip.
    """

    index: Dict[str, int] = {}
    nodes: List[dict] = []
    edges = set()

    def _node_id(raw_type, table, name) -> int:
        node = _dependency_node(raw_type, table, name)
        ident = node["key"] or _KEY_SEP.join(
            [node["type"], node["table"], node["name"]]
        )
        if ident not in index:
            index[ident] = len(nodes)
            nodes.append(node)
        return index[ident]

    for row in rows:
        source = _node_id(row["OBJECT_TYPE"], row["TABLE"], row["OBJECT"])
        ref = _node_id(
            row["REFERENCED_OBJECT_TYPE"],
            row["REFERENCED_TABLE"],
            row["REFERENCED_OBJECT"],
        )
        if source != ref:
            edges.add((source, ref))
    return {"nodes": nodes, "edges": [list(edge) for edge in sorted(edges)]}


def _model_dependencies(dataset_id: str, workspace_id: str) -> dict:
    import sempy.fabric as fabric

    df = fabric.evaluate_dax(
        dataset=dataset_id,
        workspace=workspace_id,
        dax_string="""
        SELECT [OBJECT_TYPE], [TABLE], [OBJECT], [REFERENCED_OBJECT_TYPE],
            [REFERENCED_TABLE], [REFERENCED_OBJECT]
        FROM $SYSTEM.DISCOVER_CALC_DEPENDENCY
        """,
    )
    df.columns = [str(c).strip("[]") for c in df.columns]
    return _dependency_graph(df.fillna("").to_dict("records"))


def _format_dax_expression(expression: str) -> str:
    from sempy_labs._daxformatter import _format_dax

    formatted = _format_dax([expression])
    if not formatted:
        raise ValueError("DAX Formatter returned no result.")
    return formatted[0]


# ---------------------------------------------------------------------------
# Tool helpers: AI descriptions, translation, relationship inference
# ---------------------------------------------------------------------------
# Compatibility level of semantic models created from the explorer (above the
# 1702 that DAX user-defined functions need).
_NEW_MODEL_COMPATIBILITY_LEVEL = 1706


def _create_blank_model(workspace_id: str, name: str) -> str:
    """Creates an empty semantic model in a workspace; returns its ID."""

    from sempy_labs._generate_semantic_model import create_blank_semantic_model
    from sempy_labs._ui_components import list_picker_datasets

    name = str(name or "").strip()
    if not workspace_id or not name:
        raise ValueError(
            "Select a workspace and enter a name for the new semantic model."
        )
    # create_blank_semantic_model embeds the name in a quoted TMSL string.
    if "'" in name or '"' in name:
        raise ValueError("The semantic model name cannot contain quotes.")
    taken = {str(d.get("name", "")).lower() for d in list_picker_datasets(workspace_id)}
    if name.lower() in taken:
        raise ValueError(
            f"A semantic model named '{name}' already exists in this workspace. "
            "Choose another name."
        )
    with _QuietTool():
        dataset_id = create_blank_semantic_model(
            dataset=name,
            compatibility_level=_NEW_MODEL_COMPATIBILITY_LEVEL,
            workspace=workspace_id,
            overwrite=False,
        )
    return str(dataset_id)


# ---------------------------------------------------------------------------
# Semantic views
# ---------------------------------------------------------------------------
# A semantic view is a folder of YAML definitions in a lakehouse,
# Context/Semantic Views/<view>/ (subfolders and shortcut content included).
# Publishing compiles it to TMDL under SystemContext/Semantic Views/<view>/ and
# creates or updates a semantic model with the view's name. Both folders are
# read and written through the OneLake DFS API.
_ONELAKE_DFS_URL = "https://onelake.dfs.fabric.microsoft.com"
_VIEWS_FOLDER = "Context/Semantic Views"
_VIEWS_OUTPUT_FOLDER = "SystemContext/Semantic Views"
_VIEW_YAML_VERSION = "0.2.0.dev0"
_VIEW_MAX_FILES = 20
_VIEW_MAX_YAML_BYTES = 16 * 1024 * 1024
_VIEW_NAME_INVALID = re.compile(r'[\\/:*?"<>|#%\x00-\x1f]')
_VIEW_DIALECTS = (
    "DAX",
    "ANSI_SQL",
    "SNOWFLAKE",
    "MDX",
    "TABLEAU",
    "DATABRICKS",
    "MAQL",
    "BIGQUERY",
)
# Definition keys which Fabric accepts but omits from the generated model.
_VIEW_IGNORED_KEYS = {
    "document": ("ai_context", "custom_extensions"),
    "dataset": (
        "primary_key",
        "unique_keys",
        "fields",
        "ai_context",
        "custom_extensions",
    ),
    "relationship": ("ai_context", "custom_extensions"),
    "metric": ("datatype", "ai_context", "custom_extensions"),
}


def _onelake_request(
    method: str,
    workspace_id: str,
    path: str = "",
    params: Optional[dict] = None,
    data: Optional[bytes] = None,
    headers: Optional[dict] = None,
):
    """Sends a request to the OneLake DFS endpoint of a workspace."""

    import notebookutils
    import requests
    from urllib.parse import quote

    token = notebookutils.credentials.getToken("storage")
    url = f"{_ONELAKE_DFS_URL}/{workspace_id}"
    if path:
        url += "/" + quote(path)
    return requests.request(
        method,
        url,
        params=params,
        data=data,
        headers={"Authorization": f"Bearer {token}", **(headers or {})},
        timeout=120,
    )


def _onelake_raise(response, what: str):
    message = ""
    try:
        message = str((response.json().get("error") or {}).get("message") or "")
    except Exception:
        pass
    detail = f": {message.splitlines()[0]}" if message else ""
    raise ValueError(f"Could not {what} (HTTP {response.status_code}){detail}")


def _onelake_list(
    workspace_id: str, item_id: str, directory: str, recursive: bool = False
) -> Optional[List[dict]]:
    """Lists a folder of an item (None if the folder doesn't exist).

    Paths are returned relative to the item's root, e.g.
    ``Context/Semantic Views/sales``.
    """

    params = {
        "resource": "filesystem",
        "recursive": "true" if recursive else "false",
        "directory": f"{item_id}/{directory}",
    }
    out = []
    while True:
        response = _onelake_request("GET", workspace_id, params=params)
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            _onelake_raise(response, f"list the '{directory}' folder")
        for p in response.json().get("paths", []):
            name = str(p.get("name") or "")
            out.append(
                {
                    # Names are relative to the workspace: '<item>/<path>'.
                    "path": name.split("/", 1)[1] if "/" in name else "",
                    "is_dir": str(p.get("isDirectory", "")).lower() == "true",
                    "size": int(p.get("contentLength") or 0),
                    "modified": str(p.get("lastModified") or ""),
                    "etag": str(p.get("etag") or ""),
                }
            )
        continuation = response.headers.get("x-ms-continuation")
        if not continuation:
            return out
        params["continuation"] = continuation


def _onelake_read(workspace_id: str, item_id: str, path: str) -> str:
    response = _onelake_request("GET", workspace_id, f"{item_id}/{path}")
    if response.status_code != 200:
        _onelake_raise(response, f"read '{path}'")
    # Files written on Windows use CRLF; the explorer works with LF.
    text = response.content.decode("utf-8-sig", errors="replace")
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _onelake_create_directory(workspace_id: str, item_id: str, path: str) -> None:
    # If-None-Match fails the request (409) instead of touching an existing
    # folder; missing parent folders are created implicitly.
    response = _onelake_request(
        "PUT",
        workspace_id,
        f"{item_id}/{path}",
        params={"resource": "directory"},
        headers={"If-None-Match": "*"},
    )
    if response.status_code == 409:
        raise ValueError(f"The '{path}' folder already exists.")
    if response.status_code not in (200, 201):
        _onelake_raise(response, f"create the '{path}' folder")


def _onelake_create_file(
    workspace_id: str, item_id: str, path: str, content: bytes
) -> None:
    full_path = f"{item_id}/{path}"
    response = _onelake_request(
        "PUT",
        workspace_id,
        full_path,
        params={"resource": "file"},
        headers={"If-None-Match": "*"},
    )
    if response.status_code == 409:
        raise ValueError(f"The '{path}' file already exists.")
    if response.status_code not in (200, 201):
        _onelake_raise(response, f"create '{path}'")
    if content:
        response = _onelake_request(
            "PATCH",
            workspace_id,
            full_path,
            params={"action": "append", "position": "0"},
            data=content,
            headers={"Content-Type": "application/octet-stream"},
        )
        if response.status_code not in (200, 202):
            _onelake_raise(response, f"write '{path}'")
    response = _onelake_request(
        "PATCH",
        workspace_id,
        full_path,
        params={"action": "flush", "position": str(len(content))},
    )
    if response.status_code != 200:
        _onelake_raise(response, f"write '{path}'")


def _relative_path(path: str, folder: str) -> str:
    """``path`` relative to ``folder`` ('' if it isn't inside it)."""

    prefix = folder + "/"
    return path.replace(prefix, "", 1) if path.startswith(prefix) else ""


def _list_semantic_views(workspace_id: str, lakehouse_id: str) -> List[dict]:
    """The views of a lakehouse: the folders directly under Context/Semantic Views."""

    paths = _onelake_list(workspace_id, lakehouse_id, _VIEWS_FOLDER)
    views = []
    for p in paths or []:
        name = _relative_path(p["path"], _VIEWS_FOLDER)
        if p["is_dir"] and name and "/" not in name:
            views.append({"name": name, "modified": p["modified"]})
    return sorted(views, key=lambda v: v["name"].lower())


def _view_starter_yaml(name: str) -> str:
    # A JSON string is a valid YAML double-quoted scalar.
    quoted = json.dumps(name, ensure_ascii=False)
    return f"""# Semantic view definition.
# Every .yaml file in this folder (including subfolders) is merged into this
# view when it is published. The folder name is the view name and the name of
# the generated semantic model.
# Reference: https://learn.microsoft.com/fabric/fundamentals/semantic-views-yaml-reference
version: {_VIEW_YAML_VERSION}
name: {quoted}
# description: What this view is about.

# Map lakehouse tables to datasets (tables in the semantic model).
# datasets:
#   - name: sales
#     source: dbo.sales
#     description: One row per sales transaction line.
#   - name: customers
#     source: dbo.customers

# Relationships go from the many side to the one side, on a single column.
# relationships:
#   - name: sales_to_customers
#     from: sales
#     to: customers
#     from_columns:
#       - customer_id
#     to_columns:
#       - customer_id

# Metrics become measures in a 'Metrics' table.
# metrics:
#   - name: Total Net Sales
#     description: Sales value after discounts.
#     expression:
#       dialects:
#         - dialect: DAX
#           expression: "SUM('sales'[net_amount])"
"""


def _create_semantic_view(workspace_id: str, lakehouse_id: str, name: str) -> str:
    """Creates a view folder with a starter definition file; returns the name."""

    name = str(name or "").strip()
    if not workspace_id or not lakehouse_id or not name:
        raise ValueError(
            "Select a workspace and a lakehouse and enter a name for the new "
            "semantic view."
        )
    if _VIEW_NAME_INVALID.search(name) or name.endswith("."):
        raise ValueError(
            'A semantic view name cannot contain \\ / : * ? " < > | # % or end '
            "with a period."
        )
    taken = {
        v["name"].lower() for v in _list_semantic_views(workspace_id, lakehouse_id)
    }
    if name.lower() in taken:
        raise ValueError(
            f"A semantic view named '{name}' already exists in this lakehouse. "
            "Choose another name."
        )
    folder = f"{_VIEWS_FOLDER}/{name}"
    _onelake_create_directory(workspace_id, lakehouse_id, folder)
    _onelake_create_file(
        workspace_id,
        lakehouse_id,
        f"{folder}/{name}_semantic_view.yaml",
        _view_starter_yaml(name).encode("utf-8"),
    )
    return name


def _text(value) -> str:
    return "" if value is None else str(value)


def _merge_view_documents(documents: List[Tuple[str, object]]) -> dict:
    """Merges parsed view YAML files (``(path, document)``) into one definition.

    Every object remembers the file it is defined in. Problems Fabric reports
    when publishing (missing properties, unsupported or ignored constructs,
    conflicting descriptions) are collected in ``issues``.
    """

    datasets, relationships, metrics, issues = [], [], [], []
    descriptions: Dict[str, List[str]] = {}

    def issue(level, message, file):
        issues.append({"level": level, "message": message, "file": file})

    def ignored(obj, kind, label, file):
        keys = [k for k in _VIEW_IGNORED_KEYS[kind] if k in obj]
        if keys:
            issue(
                "warning",
                f"{label}: {', '.join(keys)} {'is' if len(keys) == 1 else 'are'} "
                "not applied to the generated model.",
                file,
            )
        return keys

    def entries(doc, key, file):
        value = doc.get(key)
        if value is None:
            return []
        if not isinstance(value, list):
            issue("error", f"'{key}' must be a list.", file)
            return []
        if key == "datasets" and not value:
            issue("error", "'datasets' must not be empty when it is declared.", file)
        # The position in the file's list addresses the object when saving.
        return [(i, v) for i, v in enumerate(value) if isinstance(v, dict)]

    for path, doc in documents:
        if doc is None:
            continue
        if not isinstance(doc, dict):
            issue("error", "The file is not a YAML mapping.", path)
            continue
        version = _text(doc.get("version"))
        if not version:
            issue("error", "The required 'version' property is missing.", path)
        elif version != _VIEW_YAML_VERSION:
            issue(
                "error",
                f"Version '{version}' is not supported (use {_VIEW_YAML_VERSION}).",
                path,
            )
        if not _text(doc.get("name")):
            issue("error", "The required 'name' property is missing.", path)
        if doc.get("description") is not None:
            descriptions.setdefault(_text(doc.get("description")), []).append(path)
        ignored(doc, "document", "Document", path)
        for idx, ds in entries(doc, "datasets", path):
            name = _text(ds.get("name"))
            if not name or not _text(ds.get("source")):
                issue("error", f"Dataset '{name}' needs a name and a source.", path)
            datasets.append(
                {
                    "name": name,
                    "source": _text(ds.get("source")),
                    "description": _text(ds.get("description")),
                    "file": path,
                    "index": idx,
                    "ignored": ignored(ds, "dataset", f"Dataset '{name}'", path),
                }
            )
        for idx, rel in entries(doc, "relationships", path):
            name = _text(rel.get("name"))
            from_columns = [_text(c) for c in rel.get("from_columns") or []]
            to_columns = [_text(c) for c in rel.get("to_columns") or []]
            if not (name and rel.get("from") and rel.get("to")):
                issue(
                    "error",
                    f"Relationship '{name}' needs a name, 'from' and 'to'.",
                    path,
                )
            if len(from_columns) != 1 or len(to_columns) != 1:
                issue(
                    "error",
                    f"Relationship '{name}' must have exactly one column in "
                    "'from_columns' and in 'to_columns'.",
                    path,
                )
            relationships.append(
                {
                    "name": name,
                    "from": _text(rel.get("from")),
                    "to": _text(rel.get("to")),
                    "from_columns": from_columns,
                    "to_columns": to_columns,
                    "file": path,
                    "index": idx,
                    "ignored": ignored(
                        rel, "relationship", f"Relationship '{name}'", path
                    ),
                }
            )
        for idx, met in entries(doc, "metrics", path):
            name = _text(met.get("name"))
            expression = met.get("expression")
            dialects = []
            if isinstance(expression, dict):
                for d in expression.get("dialects") or []:
                    if isinstance(d, dict):
                        dialects.append(
                            {
                                "dialect": _text(d.get("dialect")).upper(),
                                "expression": _text(d.get("expression")),
                            }
                        )
            dax = next((d["expression"] for d in dialects if d["dialect"] == "DAX"), "")
            if not dialects:
                issue(
                    "error",
                    f"Metric '{name}' needs an expression with at least one dialect.",
                    path,
                )
            elif not dax.strip():
                issue(
                    "warning",
                    f"Metric '{name}' has no DAX expression, so it is omitted "
                    "from the generated model.",
                    path,
                )
            other = [d["dialect"] for d in dialects if d["dialect"] != "DAX"]
            unknown = [d for d in other if d not in _VIEW_DIALECTS]
            if unknown:
                issue(
                    "warning",
                    f"Metric '{name}': unknown dialect {', '.join(unknown)}.",
                    path,
                )
            metrics.append(
                {
                    "name": name,
                    "description": _text(met.get("description")),
                    "dax": dax,
                    "dialects": dialects,
                    "datatype": _text(met.get("datatype")),
                    "file": path,
                    "index": idx,
                    "ignored": ignored(met, "metric", f"Metric '{name}'", path),
                }
            )

    if len(descriptions) > 1:
        issue(
            "error",
            "The view description differs between files ("
            + ", ".join(sorted(p for paths in descriptions.values() for p in paths))
            + "); it must be identical wherever it is defined.",
            "",
        )
    for label, objects, folded in (
        ("dataset", datasets, False),
        ("relationship", relationships, False),
        ("metric", metrics, True),
    ):
        seen = set()
        for obj in objects:
            key = obj["name"].lower() if folded else obj["name"]
            if obj["name"] and key in seen:
                issue(
                    "error",
                    f"The {label} name '{obj['name']}' is used more than once.",
                    obj["file"],
                )
            seen.add(key)
    table_names = {d["name"].lower() for d in datasets}
    for met in metrics:
        if met["name"].lower() in table_names:
            issue(
                "error",
                f"Metric '{met['name']}' has the same name as a dataset.",
                met["file"],
            )
    return {
        "description": next(iter(descriptions), ""),
        "datasets": datasets,
        "relationships": relationships,
        "metrics": metrics,
        "issues": issues,
    }


# TMDL object keywords; any other word on a line of its own is a flag property
# (e.g. 'isHidden').
_TMDL_OBJECTS = {
    "model", "database", "table", "column", "measure", "hierarchy", "level",
    "partition", "relationship", "expression", "role", "perspective", "culture",
    "annotation", "extendedProperty", "calculationGroup", "calculationItem",
    "tablePermission", "columnPermission", "member", "dataSource", "function",
    "formatStringDefinition", "detailRowsDefinition", "kpi", "refreshPolicy",
    "queryGroup", "variation", "ref", "linguisticMetadata", "calendar",
    "perspectiveTable", "perspectiveColumn", "perspectiveMeasure",
    "perspectiveHierarchy", "translation", "source",
}  # fmt: skip


def _tmdl_indent(ws: str) -> int:
    return ws.count("\t") + ws.count(" ") // 4


def _tmdl_name(text: str) -> Tuple[str, str]:
    """Splits a TMDL name ('quoted' or plain) off the start of ``text``."""

    text = text.strip()
    m = re.match(r"'((?:[^']|'')*)'(.*)$", text, re.S)
    if m:
        return m.group(1).replace("''", "'"), m.group(2)
    m = re.match(r"([^\s.=]*)(.*)$", text, re.S)
    return m.group(1), m.group(2)


def _tmdl_column_ref(text: str) -> Tuple[str, str]:
    """'Table Name'.'Column Name' / table.column -> (table, column)."""

    table, rest = _tmdl_name(text)
    if rest.startswith("."):
        column, _ = _tmdl_name(rest[1:])
        return table, column
    return table, ""


def _parse_tmdl(text: str) -> List[dict]:
    """Parses TMDL into a tree of ``{kind, name, props, children, description,
    expression}`` objects (enough to read tables, columns, measures,
    partitions and relationships)."""

    roots: List[dict] = []
    stack: List[Tuple[int, dict]] = []
    description: List[str] = []
    lines = str(text or "").replace("\r\n", "\n").split("\n")
    i = 0

    def owner(indent):
        while stack and stack[-1][0] >= indent:
            stack.pop()
        return stack[-1][1] if stack else None

    while i < len(lines):
        line = lines[i]
        i += 1
        m = re.match(r"^([ \t]*)(.*)$", line)
        ws, rest = m.group(1), m.group(2).rstrip()
        if not rest:
            continue
        indent = _tmdl_indent(ws)
        if rest.startswith("///"):
            description.append(rest[3:].strip())
            continue
        word_match = re.match(r"^([A-Za-z][A-Za-z0-9]*)(.*)$", rest)
        if not word_match:
            continue
        word, tail = word_match.group(1), word_match.group(2)
        parent = owner(indent)
        if tail.startswith(":"):
            if parent is not None:
                parent["props"][word] = tail[1:].strip().strip('"')
            description = []
            continue
        if word not in _TMDL_OBJECTS:
            if parent is not None and not tail.strip():
                parent["props"][word] = "true"
            description = []
            continue
        node = {
            "kind": word,
            "name": "",
            "props": {},
            "children": [],
            "description": "\n".join(description),
            "expression": "",
        }
        description = []
        if word == "ref":
            # 'ref table X' only orders objects; it defines nothing.
            continue
        name, after = _tmdl_name(tail)
        node["name"] = name
        after = after.strip()
        if after.startswith("="):
            expr = after[1:].strip()
            body = []
            if expr == "```":
                while i < len(lines) and lines[i].strip() != "```":
                    body.append(lines[i])
                    i += 1
                i += 1
            else:
                if expr:
                    body.append(expr)
                # Continuation lines sit deeper than the object's properties.
                while i < len(lines):
                    nxt = lines[i]
                    nm = re.match(r"^([ \t]*)(.*)$", nxt)
                    if nm.group(2).strip() and _tmdl_indent(nm.group(1)) <= indent + 1:
                        break
                    body.append(nxt)
                    i += 1
            node["expression"] = _dedent_lines(body)
        if parent is not None:
            parent["children"].append(node)
        else:
            roots.append(node)
        stack.append((indent, node))
    return roots


def _dedent_lines(lines: List[str]) -> str:
    while lines and not lines[-1].strip():
        lines = lines[:-1]
    margin = min((len(ln) - len(ln.lstrip()) for ln in lines if ln.strip()), default=0)
    return "\n".join(ln[margin:] if ln.strip() else "" for ln in lines).strip("\n")


def _tmdl_model(texts: List[str]) -> dict:
    """Tables (with columns, measures and source), relationships and the
    model description from a set of TMDL files."""

    tables: Dict[str, dict] = {}
    relationships, description = [], ""

    def walk(nodes):
        nonlocal description
        for node in nodes:
            kind, props = node["kind"], node["props"]
            if kind == "model":
                description = node["description"] or props.get("description", "")
                # A single-file definition nests the tables and relationships
                # in the model.
                walk(node["children"])
            elif kind == "table":
                table = tables.setdefault(
                    node["name"],
                    {"name": node["name"], "columns": [], "measures": [], "source": ""},
                )
                table["description"] = node["description"]
                table["is_hidden"] = props.get("isHidden") == "true"
                for child in node["children"]:
                    cp = child["props"]
                    if child["kind"] == "column":
                        table["columns"].append(
                            {
                                "name": child["name"],
                                "data_type": cp.get("dataType", ""),
                                "description": child["description"],
                                "source_column": cp.get("sourceColumn", ""),
                                "format_string": cp.get("formatString", ""),
                                "is_hidden": cp.get("isHidden") == "true",
                            }
                        )
                    elif child["kind"] == "measure":
                        table["measures"].append(
                            {
                                "name": child["name"],
                                "expression": child["expression"],
                                "description": child["description"],
                                "format_string": cp.get("formatString", ""),
                            }
                        )
                    elif child["kind"] == "partition":
                        src = dict(cp)
                        for grand in child["children"]:
                            if grand["kind"] == "source":
                                src.update(grand["props"])
                        entity = src.get("entityName", "")
                        if entity:
                            schema = src.get("schemaName", "")
                            table["source"] = f"{schema}.{entity}" if schema else entity
            elif kind == "relationship":
                from_table, from_column = _tmdl_column_ref(props.get("fromColumn", ""))
                to_table, to_column = _tmdl_column_ref(props.get("toColumn", ""))
                relationships.append(
                    {
                        "name": node["name"],
                        "from": from_table,
                        "from_column": from_column,
                        "to": to_table,
                        "to_column": to_column,
                        "from_cardinality": props.get("fromCardinality", "many"),
                        "to_cardinality": props.get("toCardinality", "one"),
                        "cross_filter": props.get(
                            "crossFilteringBehavior", "oneDirection"
                        ),
                        "active": props.get("isActive", "true") != "false",
                    }
                )
            else:
                walk(node["children"])

    for text in texts:
        walk(_parse_tmdl(text))
    return {
        "tables": tables,
        "relationships": relationships,
        "description": description,
    }


def _lakehouse_table_columns(workspace_id: str, lakehouse_id: str, source: str) -> list:
    """Columns of a lakehouse table ('schema.table') from the OneLake table API."""

    from sempy_labs._helper_functions import _base_api

    schema, _, table = source.rpartition(".")
    response = _base_api(
        request=f"{workspace_id}/{lakehouse_id}/api/2.1/unity-catalog/tables/"
        f"{lakehouse_id}.{schema or 'dbo'}.{table}",
        client="onelake",
    )
    return [
        {
            "name": _text(c.get("name")),
            "data_type": _text(c.get("type_text") or c.get("type_name")).lower(),
            "description": _text(c.get("comment")),
            "source_column": _text(c.get("name")),
            "format_string": "",
            "is_hidden": False,
        }
        for c in sorted(
            response.json().get("columns") or [], key=lambda c: c.get("position", 0)
        )
    ]


def _semantic_view_payload(
    workspace_id: str, lakehouse_id: str, view_name: str
) -> dict:
    """Reads a semantic view: its YAML definition (Context) merged with the
    published TMDL output (SystemContext), plus the files behind both."""

    import yaml

    folder = f"{_VIEWS_FOLDER}/{view_name}"
    paths = _onelake_list(workspace_id, lakehouse_id, folder, recursive=True)
    if paths is None:
        raise ValueError(
            f"The '{view_name}' semantic view was not found in this lakehouse."
        )
    files, documents = [], []
    yaml_bytes = 0
    for p in sorted(paths, key=lambda p: p["path"].lower()):
        if p["is_dir"] or not p["path"].startswith(folder + "/"):
            continue
        rel = _relative_path(p["path"], folder)
        entry = {
            "path": rel,
            "area": "definition",
            "size": p["size"],
            "modified": p["modified"],
            "etag": p.get("etag", ""),
            "language": "yaml" if rel.endswith(".yaml") else "text",
            "content": "",
            "error": "",
            "ignored": not rel.endswith(".yaml"),
        }
        files.append(entry)
        if entry["ignored"]:
            continue
        yaml_bytes += p["size"]
        entry["content"] = _onelake_read(workspace_id, lakehouse_id, p["path"])
        try:
            documents.append((rel, yaml.safe_load(entry["content"])))
        except yaml.YAMLError as e:
            entry["error"] = " ".join(str(e).split())
    merged = _merge_view_documents(documents)
    issues = merged["issues"]
    for f in files:
        if f["error"]:
            issues.insert(
                0,
                {
                    "level": "error",
                    "message": f"Invalid YAML: {f['error']}",
                    "file": f["path"],
                },
            )
    if not merged["datasets"] and not merged["metrics"] and documents:
        issues.append(
            {
                "level": "warning",
                "message": "The view doesn't define any datasets or metrics yet; add "
                "them to its YAML files before publishing it.",
                "file": "",
            }
        )
    yaml_count = sum(1 for f in files if not f["ignored"])
    if not yaml_count:
        issues.insert(
            0, {"level": "error", "message": "The view has no .yaml files.", "file": ""}
        )
    if yaml_count > _VIEW_MAX_FILES:
        issues.append(
            {
                "level": "error",
                "message": f"The view has {yaml_count} .yaml files (the limit is {_VIEW_MAX_FILES}).",
                "file": "",
            }
        )
    if yaml_bytes > _VIEW_MAX_YAML_BYTES:
        issues.append(
            {
                "level": "error",
                "message": "The view's .yaml files exceed 16 MB in total.",
                "file": "",
            }
        )

    # Published output: the compiled TMDL of the generated semantic model.
    output_folder = f"{_VIEWS_OUTPUT_FOLDER}/{view_name}"
    tmdl_texts = []
    for p in sorted(
        _onelake_list(workspace_id, lakehouse_id, output_folder, recursive=True) or [],
        key=lambda p: p["path"].lower(),
    ):
        if p["is_dir"] or not p["path"].lower().endswith(".tmdl"):
            continue
        content = _onelake_read(workspace_id, lakehouse_id, p["path"])
        tmdl_texts.append(content)
        files.append(
            {
                "path": _relative_path(p["path"], output_folder),
                "area": "generated",
                "size": p["size"],
                "modified": p["modified"],
                "language": "tmdl",
                "content": content,
                "error": "",
                "ignored": False,
            }
        )
    published = bool(tmdl_texts)
    tmdl = _tmdl_model(tmdl_texts)
    tables = {name.lower(): t for name, t in tmdl["tables"].items()}

    def state(found: bool, differences: List[str]) -> dict:
        """Publish status of an object's saved definition: 'published' (as
        published), 'changed' (published, with saved changes which aren't
        published yet) or 'missing' (not in the generated model); '' if the
        view was never published."""

        if not published:
            return {"published": False, "publish_state": "", "publish_diff": []}
        if not found:
            return {"published": False, "publish_state": "missing", "publish_diff": []}
        return {
            "published": True,
            "publish_state": "changed" if differences else "published",
            "publish_diff": differences,
        }

    datasets = []
    for ds in merged["datasets"]:
        table = tables.get(ds["name"].lower())
        columns, columns_from = [], ""
        if table is not None:
            columns, columns_from = table["columns"], "published"
        elif ds["source"]:
            try:
                columns = _lakehouse_table_columns(
                    workspace_id, lakehouse_id, ds["source"]
                )
                columns_from = "lakehouse"
            except Exception:
                columns = []
        differences = []
        if table is not None:
            if table["source"] and _view_source(ds["source"]) != _view_source(
                table["source"]
            ):
                differences.append("source")
            if _same_text(ds["description"], table.get("description")) is False:
                differences.append("description")
        datasets.append(
            {
                **ds,
                "columns": columns,
                "columns_from": columns_from,
                "inferred": False,
                **state(table is not None, differences),
            }
        )
    # Tables Fabric inferred (e.g. referenced only by metrics) are in the
    # generated model without a dataset definition.
    declared = {d["name"].lower() for d in datasets}
    for key, table in tables.items():
        if key in declared or key == "metrics" and table["measures"]:
            continue
        datasets.append(
            {
                "name": table["name"],
                "source": table["source"],
                "description": table.get("description", ""),
                "file": "",
                "index": -1,
                "ignored": [],
                "columns": table["columns"],
                "columns_from": "published",
                "inferred": True,
                **state(True, []),
            }
        )

    # A relationship is matched to the generated model's relationship between
    # the same columns; if there is none, to one with its name or joining the
    # same columns the other way round (published, but changed since).
    def ends(t1, c1, t2, c2):
        return (t1.lower(), c1.lower(), t2.lower(), c2.lower())

    by_ends = {
        ends(r["from"], r["from_column"], r["to"], r["to_column"]): r
        for r in tmdl["relationships"]
    }
    by_name = {r["name"].lower(): r for r in tmdl["relationships"]}
    relationships, matched_rels = [], set()
    for rel in merged["relationships"]:
        from_column = rel["from_columns"][0] if rel["from_columns"] else ""
        to_column = rel["to_columns"][0] if rel["to_columns"] else ""
        match = by_ends.get(ends(rel["from"], from_column, rel["to"], to_column))
        differences = []
        if match is None:
            match = by_ends.get(
                ends(rel["to"], to_column, rel["from"], from_column)
            ) or by_name.get(rel["name"].lower())
            if match is not None:
                if (match["from"].lower(), match["to"].lower()) != (
                    rel["from"].lower(),
                    rel["to"].lower(),
                ):
                    differences.append("from / to")
                if (
                    match["from_column"].lower(),
                    match["to_column"].lower(),
                ) != (from_column.lower(), to_column.lower()):
                    differences.append("columns")
                differences = differences or ["definition"]
        if match is not None:
            matched_rels.add(id(match))
        relationships.append(
            {
                **rel,
                "from_column": from_column,
                "to_column": to_column,
                "cross_filter": match["cross_filter"] if match else "oneDirection",
                **state(match is not None, differences),
            }
        )

    measures = {}
    for table in tmdl["tables"].values():
        for measure in table["measures"]:
            measures[measure["name"].lower()] = measure
    metrics, matched_measures = [], set()
    for met in merged["metrics"]:
        match = measures.get(met["name"].lower())
        differences = []
        if match is not None:
            matched_measures.add(met["name"].lower())
            # Metrics without DAX are omitted when publishing.
            if not _same_dax(met["dax"], match["expression"]):
                differences.append("expression")
            if _same_text(met["description"], match["description"]) is False:
                differences.append("description")
        metrics.append(
            {
                **met,
                "format_string": match["format_string"] if match else "",
                **state(match is not None, differences),
            }
        )

    # Published objects whose definition has since been removed.
    if published:
        stale = [
            f"Metric '{m['name']}'"
            for key, m in measures.items()
            if key not in matched_measures
        ] + [
            f"Relationship {r['from']}[{r['from_column']}] \u2192 "
            f"{r['to']}[{r['to_column']}]"
            for r in tmdl["relationships"]
            if id(r) not in matched_rels
        ]
        for label in stale:
            issues.append(
                {
                    "level": "warning",
                    "message": f"{label} is in the published model but not in the "
                    "definition; publish the view to remove it.",
                    "file": "",
                }
            )

    view_differences = []
    if published and _same_text(merged["description"], tmdl["description"]) is False:
        view_differences.append("description")
    unpublished = len(view_differences) + sum(
        1
        for o in datasets + relationships + metrics
        if o.get("publish_state") in ("changed", "missing")
    )
    return {
        "view": {
            "name": view_name,
            "description": merged["description"],
            "folder": folder,
            "output_folder": output_folder,
            "published": published,
            "publish_diff": view_differences,
            # Saved definitions which differ from the last publish.
            "unpublished_count": unpublished,
        },
        "datasets": datasets,
        "relationships": relationships,
        "metrics": metrics,
        "files": files,
        "issues": issues,
    }


def _normalized_text(value) -> str:
    return " ".join(str(value or "").split())


def _same_text(a, b) -> Optional[bool]:
    """Compares descriptions ignoring whitespace; None when both are empty."""

    a, b = _normalized_text(a), _normalized_text(b)
    if not a and not b:
        return None
    return a == b


def _same_dax(a, b) -> bool:
    # Layout (line breaks, TMDL indentation, spaces next to brackets and
    # operators) doesn't change the DAX.
    def compact(text):
        return re.sub(r" ?([^\w\s]) ?", r"\1", _normalized_text(text))

    return compact(a) == compact(b)


def _view_source(source) -> str:
    """'schema.table' (dbo when unqualified), lower case, for comparing."""

    source = str(source or "").strip().lower()
    return source if "." in source else f"dbo.{source}"


# Keys of the YAML lists holding each kind of view object.
_VIEW_LISTS = ("datasets", "relationships", "metrics")


class _ViewYaml:
    """Loads and dumps view YAML files, keeping comments and formatting when
    ruamel.yaml is installed (PyYAML otherwise)."""

    def __init__(self):
        try:
            from ruamel.yaml import YAML
            from ruamel.yaml.scalarstring import LiteralScalarString

            self._yaml = YAML()
            self._yaml.preserve_quotes = True
            self._yaml.width = 4096
            self._yaml.indent(mapping=2, sequence=4, offset=2)
            self._literal = LiteralScalarString
            self.keeps_comments = True
        except ImportError:
            self._yaml = None
            self.keeps_comments = False

    def load(self, text: str):
        if self._yaml is not None:
            return self._yaml.load(text)
        import yaml

        return yaml.safe_load(text)

    def string(self, value: str):
        """A string value; multi-line text is written as a literal block."""

        value = str(value).replace("\r\n", "\n")
        if self._yaml is not None and "\n" in value:
            return self._literal(value)
        return value

    def dump(self, doc) -> str:
        if self._yaml is not None:
            import io

            out = io.StringIO()
            self._yaml.dump(doc, out)
            return out.getvalue()
        import yaml

        class _Dumper(yaml.SafeDumper):
            # Indent list items under their key, as Fabric's examples do.
            def increase_indent(self, flow=False, indentless=False):
                return super().increase_indent(flow, False)

        def _str(dumper, data):
            style = "|" if "\n" in data else None
            return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)

        _Dumper.add_representer(str, _str)
        return yaml.dump(
            doc,
            Dumper=_Dumper,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
            width=4096,
        )


def _set_text(obj, key: str, value, yml: _ViewYaml) -> None:
    """Sets a text property; an empty value removes it."""

    value = "" if value is None else str(value)
    if value.strip():
        obj[key] = yml.string(value)
    elif key in obj:
        del obj[key]


def _set_document_description(document, value: str, yml: _ViewYaml) -> None:
    """Sets the view description; a new one goes right after 'name'."""

    if "description" in document or not value.strip():
        _set_text(document, "description", value, yml)
        return
    keys = list(document.keys())
    position = keys.index("name") + 1 if "name" in keys else len(keys)
    if hasattr(document, "insert"):
        document.insert(position, "description", yml.string(value))
    else:
        items = list(document.items())
        items.insert(position, ("description", yml.string(value)))
        document.clear()
        document.update(items)


def _apply_view_values(kind: str, obj, values: dict, yml: _ViewYaml) -> None:
    """Applies edited explorer fields to a dataset / relationship / metric."""

    for field, value in values.items():
        if field in ("name", "source", "from", "to"):
            obj[field] = yml.string(value or "")
        elif field == "description":
            _set_text(obj, "description", value, yml)
        elif kind == "relationships" and field in ("from_column", "to_column"):
            obj[field.replace("_column", "_columns")] = [yml.string(value or "")]
        elif kind == "metrics" and field == "dax":
            expression = obj.get("expression")
            if not isinstance(expression, dict):
                expression = obj["expression"] = {}
            dialects = expression.get("dialects")
            if not isinstance(dialects, list):
                dialects = expression["dialects"] = []
            entry = next(
                (
                    d
                    for d in dialects
                    if isinstance(d, dict)
                    and str(d.get("dialect", "")).upper() == "DAX"
                ),
                None,
            )
            if entry is None:
                entry = {"dialect": "DAX"}
                dialects.insert(0, entry)
            entry["expression"] = yml.string(value or "")


def _new_view_object(kind: str, values: dict, yml: _ViewYaml) -> dict:
    obj: dict = {"name": yml.string(values.get("name") or "")}
    if kind == "datasets":
        obj["source"] = yml.string(values.get("source") or "")
        _set_text(obj, "description", values.get("description"), yml)
    elif kind == "relationships":
        obj["from"] = yml.string(values.get("from") or "")
        obj["to"] = yml.string(values.get("to") or "")
        obj["from_columns"] = [yml.string(values.get("from_column") or "")]
        obj["to_columns"] = [yml.string(values.get("to_column") or "")]
    else:
        _set_text(obj, "description", values.get("description"), yml)
        obj["expression"] = {
            "dialects": [
                {"dialect": "DAX", "expression": yml.string(values.get("dax") or "")}
            ]
        }
    return obj


def _save_semantic_view(
    workspace_id: str,
    lakehouse_id: str,
    view_name: str,
    changes: dict,
    etags: Optional[dict] = None,
) -> Tuple[int, bool]:
    """Writes staged explorer edits back to a view's YAML files.

    ``changes`` holds ``description`` (the view description, or None when
    unchanged), ``updates`` / ``deletes`` (``{kind, file, index, name}`` plus
    ``values`` for updates; objects are addressed by their position in their
    file's list and checked by name) and ``creates`` (``{kind, file,
    values}``). ``etags`` are the files' ETags when the view was read: a file
    changed since then is not overwritten. Returns the number of changes and
    whether YAML comments could be kept.
    """

    folder = f"{_VIEWS_FOLDER}/{view_name}"
    listing = _onelake_list(workspace_id, lakehouse_id, folder, recursive=True)
    if listing is None:
        raise ValueError(f"The '{view_name}' semantic view no longer exists.")
    current = {
        _relative_path(p["path"], folder): p
        for p in listing
        if not p["is_dir"] and p["path"].endswith(".yaml")
    }
    yaml_files = sorted(current, key=str.lower)
    if not yaml_files:
        raise ValueError("The semantic view has no .yaml file to save changes to.")
    yml = _ViewYaml()
    docs: Dict[str, object] = {}
    originals: Dict[str, str] = {}
    touched: set = set()

    def doc(path):
        if path not in current:
            raise ValueError(
                f"'{path}' is no longer in the semantic view. Reload the view."
            )
        if path not in docs:
            known = (etags or {}).get(path)
            if known and current[path].get("etag") and known != current[path]["etag"]:
                raise ValueError(
                    f"'{path}' was changed after the view was loaded. Reload the "
                    "view and make your changes again."
                )
            text = _onelake_read(workspace_id, lakehouse_id, f"{folder}/{path}")
            originals[path] = text
            loaded = yml.load(text) if text.strip() else None
            if loaded is None:
                loaded = yml.load(f"version: {_VIEW_YAML_VERSION}\nname: x\n")
                loaded["name"] = view_name
            if not isinstance(loaded, dict):
                raise ValueError(f"'{path}' is not a YAML mapping.")
            docs[path] = loaded
        return docs[path]

    def located(change):
        kind = change.get("kind")
        if kind not in _VIEW_LISTS:
            raise ValueError(f"Unknown object kind '{kind}'.")
        items = doc(change.get("file")).get(kind)
        index = int(change.get("index", -1))
        if not isinstance(items, list) or not 0 <= index < len(items):
            raise ValueError(
                f"'{change.get('name')}' was not found in {change.get('file')}."
            )
        obj = items[index]
        if not isinstance(obj, dict) or str(obj.get("name") or "") != str(
            change.get("name") or ""
        ):
            raise ValueError(
                f"'{change.get('name')}' has changed in {change.get('file')}. Reload "
                "the view and make your changes again."
            )
        return items, index, obj

    count = 0
    for change in changes.get("updates") or []:
        _, _, obj = located(change)
        _apply_view_values(change["kind"], obj, dict(change.get("values") or {}), yml)
        touched.add(change["file"])
        count += 1
    deletes = list(changes.get("deletes") or [])
    for change in sorted(deletes, key=lambda c: -int(c.get("index", -1))):
        items, index, _ = located(change)
        del items[index]
        if not items:
            # An empty list is invalid (e.g. 'datasets: []').
            del doc(change["file"])[change["kind"]]
        touched.add(change["file"])
        count += 1

    def target(kind):
        for path in yaml_files:
            if isinstance(doc(path).get(kind), list):
                return path
        for path in yaml_files:
            if isinstance(doc(path).get("datasets"), list):
                return path
        return yaml_files[0]

    for change in changes.get("creates") or []:
        kind = change.get("kind")
        if kind not in _VIEW_LISTS:
            raise ValueError(f"Unknown object kind '{kind}'.")
        path = change.get("file") if change.get("file") in current else target(kind)
        document = doc(path)
        if not isinstance(document.get(kind), list):
            document[kind] = []
        document[kind].append(
            _new_view_object(kind, dict(change.get("values") or {}), yml)
        )
        touched.add(path)
        count += 1

    if changes.get("description") is not None:
        value = str(changes["description"])
        defining = [p for p in yaml_files if "description" in doc(p)]
        for path in defining or ([target("datasets")] if value.strip() else []):
            _set_document_description(doc(path), value, yml)
            touched.add(path)
        count += 1

    comments_kept = True
    for path in sorted(touched):
        text = yml.dump(docs[path])
        if not yml.keeps_comments and re.search(r"(^|\s)#", originals[path]):
            comments_kept = False
        _onelake_write_file(
            workspace_id,
            lakehouse_id,
            f"{folder}/{path}",
            text.encode("utf-8"),
            current[path].get("etag", ""),
        )
    return count, comments_kept


def _publish_view(workspace_id: str, lakehouse_id: str, view_name: str) -> List[str]:
    """Publishes a view; returns the publish warnings as text."""

    from sempy_labs.semantic_model._semantic_view import publish_semantic_view

    with _QuietTool():
        result = publish_semantic_view(
            view_name, lakehouse=lakehouse_id, workspace=workspace_id
        )
    warnings = []
    for w in (result or {}).get("warnings") or []:
        if not isinstance(w, dict):
            warnings.append(str(w))
            continue
        where = ":".join(
            str(w[k]) for k in ("sourcePath", "line") if w.get(k) not in (None, "")
        )
        message = str(w.get("message") or w.get("code") or "")
        warnings.append(f"{message} ({where})" if where else message)
    return warnings


def _refresh_view_model(workspace_id: str, view_name: str) -> None:
    """Refreshes the semantic model generated from a view (same name and
    workspace as the view)."""

    from sempy_labs._refresh_semantic_model import refresh_semantic_model

    with _QuietTool():
        refresh_semantic_model(dataset=view_name, workspace=workspace_id)


def _onelake_write_file(
    workspace_id: str, item_id: str, path: str, content: bytes, etag: str = ""
) -> None:
    """Overwrites a file; with an ETag, only if it hasn't changed since."""

    full_path = f"{item_id}/{path}"
    response = _onelake_request(
        "PUT",
        workspace_id,
        full_path,
        params={"resource": "file"},
        headers={"If-Match": etag} if etag else None,
    )
    if response.status_code == 412:
        raise ValueError(
            f"'{path}' was changed by someone else. Reload the view and make your "
            "changes again."
        )
    if response.status_code not in (200, 201):
        _onelake_raise(response, f"write '{path}'")
    if content:
        response = _onelake_request(
            "PATCH",
            workspace_id,
            full_path,
            params={"action": "append", "position": "0"},
            data=content,
            headers={"Content-Type": "application/octet-stream"},
        )
        if response.status_code not in (200, 202):
            _onelake_raise(response, f"write '{path}'")
    response = _onelake_request(
        "PATCH",
        workspace_id,
        full_path,
        params={"action": "flush", "position": str(len(content))},
    )
    if response.status_code != 200:
        _onelake_raise(response, f"write '{path}'")


def _lakehouse_tables(workspace_id: str, lakehouse_id: str) -> List[str]:
    """The lakehouse's tables as 'schema.table' (for dataset sources)."""

    from sempy_labs._helper_functions import _base_api

    base = f"{workspace_id}/{lakehouse_id}/api/2.1/unity-catalog"
    try:
        schemas = _base_api(
            request=f"{base}/schemas?catalog_name={lakehouse_id}", client="onelake"
        ).json()
        names = []
        for schema in schemas.get("schemas") or []:
            schema_name = schema.get("name")
            tables = _base_api(
                request=f"{base}/tables?catalog_name={lakehouse_id}&schema_name={schema_name}",
                client="onelake",
            ).json()
            names += [
                f"{schema_name}.{t.get('name')}" for t in tables.get("tables") or []
            ]
        return sorted(names, key=str.lower)
    except Exception:
        # Lakehouses without schemas list their tables in 'dbo'.
        responses = _base_api(
            request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
            uses_pagination=True,
            client="fabric_sp",
        )
        return sorted(
            (f"dbo.{t.get('name')}" for r in responses for t in r.get("data") or []),
            key=str.lower,
        )


def _generate_measure_descriptions(workspace_id: str, measures: List[dict]) -> list:
    """Generates measure descriptions with the Fabric Copilot completions API.

    Each measure is sent on its own (``{table, name, expression}``); returns
    ``{table, name, description}`` for the measures that got a description.
    """

    from sempy_labs._helper_functions import _base_api

    results = []
    for m in measures:
        name = str(m.get("name") or "")
        if not name:
            continue
        payload = {
            "scenarioDefinition": {
                "generateModelItemDescriptions": {
                    "modelItems": [
                        {
                            "urn": name,
                            "type": 1,
                            "name": name,
                            "expression": str(m.get("expression") or ""),
                        }
                    ],
                },
            },
            "workspaceId": workspace_id,
            "artifactInfo": {"artifactType": "SemanticModel"},
        }
        response = _base_api(
            request="explore/v202304/nl2nl/completions",
            client="internal",
            method="post",
            payload=payload,
        )
        items = (response.json() or {}).get("modelItems") or []
        description = next(
            (str(i.get("description")) for i in items if i.get("description")), ""
        )
        if description.strip():
            results.append(
                {
                    "table": str(m.get("table") or ""),
                    "name": name,
                    "description": description.strip(),
                }
            )
    return results


_TRANSLATOR_URL = "https://api.cognitive.microsofttranslator.com/translate"


def _translate_texts(key: str, region: str, texts: List[str], languages: list):
    """Translates texts with Azure AI Translator.

    Returns ``{text: {language (lower-case): translation}}``.
    """

    import requests

    key = str(key or "").strip()
    if not key:
        raise ValueError("An Azure AI Translator key is required.")
    texts = list(dict.fromkeys(str(t) for t in texts if str(t or "").strip()))
    languages = list(dict.fromkeys(str(x) for x in languages if str(x or "").strip()))
    headers = {"Ocp-Apim-Subscription-Key": key, "Content-Type": "application/json"}
    if str(region or "").strip():
        headers["Ocp-Apim-Subscription-Region"] = str(region).strip()
    result: Dict[str, Dict[str, str]] = {}
    if not texts or not languages:
        return result
    # The service accepts up to 1,000 texts (50,000 characters) per request.
    batch: List[str] = []

    def _send(chunk: List[str]) -> None:
        response = requests.post(
            _TRANSLATOR_URL,
            params=[("api-version", "3.0")] + [("to", x) for x in languages],
            headers=headers,
            json=[{"Text": t} for t in chunk],
            timeout=60,
        )
        if not response.ok:
            raise ValueError(
                f"Translation failed ({response.status_code}): {response.text[:300]}"
            )
        for text, item in zip(chunk, response.json() or []):
            for tr in item.get("translations") or []:
                result.setdefault(text, {})[str(tr.get("to", "")).lower()] = str(
                    tr.get("text") or ""
                )

    size = 0
    for text in texts:
        if batch and (len(batch) >= 100 or size + len(text) > 9000):
            _send(batch)
            batch, size = [], 0
        batch.append(text)
        size += len(text)
    if batch:
        _send(batch)
    return result


def _dax_table(name: str) -> str:
    return "'" + str(name).replace("'", "''") + "'"


def _dax_column(table: str, column: str) -> str:
    return f"{_dax_table(table)}[{str(column).replace(']', ']]')}]"


def _relationship_inference(pair: dict, stats: Dict[Tuple[str, str], tuple]) -> dict:
    """Infers a relationship's direction and cardinality from column uniqueness."""

    a = (str(pair.get("from_table")), str(pair.get("from_column")))
    b = (str(pair.get("to_table")), str(pair.get("to_column")))
    if a not in stats or b not in stats:
        return {
            "from_table": a[0],
            "from_column": a[1],
            "to_table": b[0],
            "to_column": b[1],
            "error": "Could not measure column statistics for this pair.",
        }
    (a_rows, a_distinct), (b_rows, b_distinct) = stats[a], stats[b]
    a_unique = a_rows > 0 and a_rows == a_distinct
    b_unique = b_rows > 0 and b_rows == b_distinct
    if b_unique and not a_unique:
        sides, cards, label, confident = (a, b), ("Many", "One"), "Many-to-one", True
    elif a_unique and not b_unique:
        sides, cards, label, confident = (b, a), ("Many", "One"), "Many-to-one", True
    elif a_unique and b_unique:
        sides, cards, label, confident = (a, b), ("One", "One"), "One-to-one", True
    else:
        sides, cards, label, confident = (a, b), ("Many", "Many"), "Many-to-many", False
    f, t = sides
    return {
        "from_table": f[0],
        "from_column": f[1],
        "to_table": t[0],
        "to_column": t[1],
        "from_cardinality": cards[0],
        "to_cardinality": cards[1],
        "label": label,
        "confident": confident,
        "from_rows": stats[f][0],
        "from_distinct": stats[f][1],
        "from_unique": stats[f][0] > 0 and stats[f][0] == stats[f][1],
        "to_rows": stats[t][0],
        "to_distinct": stats[t][1],
        "to_unique": stats[t][0] > 0 and stats[t][0] == stats[t][1],
    }


def _infer_relationships(dataset_id: str, workspace_id: str, pairs: List[dict]):
    """Measures each pair's columns with one DAX query and infers cardinality."""

    import sempy.fabric as fabric

    combos: List[Tuple[str, str]] = []
    for p in pairs:
        for side in ("from", "to"):
            combo = (str(p.get(f"{side}_table")), str(p.get(f"{side}_column")))
            if combo[0] and combo[1] and combo not in combos:
                combos.append(combo)
    if not combos:
        return []
    rows = [
        f'ROW("Idx", {i}, "Rows", COUNTROWS({_dax_table(t)}), '
        f'"Distinct", DISTINCTCOUNT({_dax_column(t, c)}))'
        for i, (t, c) in enumerate(combos)
    ]
    query = "EVALUATE " + (
        rows[0] if len(rows) == 1 else "UNION(" + ", ".join(rows) + ")"
    )
    df = fabric.evaluate_dax(
        dataset=dataset_id, dax_string=query, workspace=workspace_id
    )
    stats: Dict[Tuple[str, str], tuple] = {}
    for values in df.itertuples(index=False):
        try:
            idx = int(values[0])
        except (TypeError, ValueError):
            continue
        if 0 <= idx < len(combos):
            num = [0 if v != v or v is None else int(v) for v in values[1:3]]
            stats[combos[idx]] = (num[0], num[1])
    return [_relationship_inference(p, stats) for p in pairs]


# ---------------------------------------------------------------------------
# Tool helpers: query scale-out, Direct Lake cache warming, .vpax files
# ---------------------------------------------------------------------------
class _NoProgress:
    """A silent stand-in for tqdm (widget work runs on the kernel thread)."""

    def __init__(self, iterable=None, *args, **kwargs):
        self._iterable = iterable if iterable is not None else []

    def __iter__(self):
        return iter(self._iterable)

    def set_description(self, *args, **kwargs):
        pass


class _QuietTool:
    """Silences prints and progress bars of the library functions a tool calls.

    The captured output stays available as ``text`` (some functions report
    failures by printing rather than raising).
    """

    def __enter__(self):
        import contextlib
        import io
        from unittest import mock
        from sempy_labs.directlake import _warm_cache

        self._stack = contextlib.ExitStack()
        self._buffer = io.StringIO()
        self._stack.enter_context(contextlib.redirect_stdout(self._buffer))
        self._stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
        self._stack.enter_context(mock.patch.object(_warm_cache, "tqdm", _NoProgress))
        return self

    def __exit__(self, *exc):
        self._stack.close()
        return False

    @property
    def text(self) -> str:
        return self._buffer.getvalue()


def _json_time(value) -> str:
    """A JSON-friendly timestamp ('' for empty / NaT)."""

    import pandas as pd

    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _json_int(value):
    import pandas as pd

    try:
        return None if pd.isna(value) else int(value)
    except (TypeError, ValueError):
        return None


def _qso_state(dataset_id: str, workspace_id: str) -> dict:
    """Query scale-out settings and sync status of a semantic model."""

    import pandas as pd
    from sempy_labs._query_scale_out import list_qso_settings, qso_sync_status

    with _QuietTool():
        settings_df = list_qso_settings(dataset=dataset_id, workspace=workspace_id)
        status_df, replicas_df = qso_sync_status(
            dataset=dataset_id, workspace=workspace_id
        )
    if settings_df.empty:
        raise ValueError("The query scale-out settings could not be read.")
    row = settings_df.iloc[0]
    auto_sync = row["QSO Auto Sync Enabled"]
    settings = {
        "storageMode": str(row["Storage Mode"]),
        "autoSync": (
            True if auto_sync is None or pd.isna(auto_sync) else bool(auto_sync)
        ),
        "maxReplicas": _json_int(row["QSO Max Read Only Replicas"]),
    }
    if status_df.empty:
        # qso_sync_status only returns rows while scale-out is enabled.
        state = (
            "StorageModeNotSupported"
            if settings["storageMode"] == "Small"
            else "Disabled"
        )
        status = {"scaleOutStatus": state, "replicas": []}
    else:
        s = status_df.iloc[0]
        status = {
            "scaleOutStatus": str(s["Scale Out Status"]),
            "syncStartTime": _json_time(s["Sync Start Time"]),
            "syncEndTime": _json_time(s["Sync End Time"]),
            "triggerReason": str(s["Trigger Reason"] or ""),
            "commitVersion": _json_int(s["Commit Version"]),
            "targetSyncVersion": _json_int(s["Target Sync Version"]),
            "minActiveReadVersion": _json_int(s["Min Active Read Version"]),
            "replicas": [
                {
                    "id": str(r["Replica ID"] or ""),
                    "type": str(r["Replica Type"] or ""),
                    "version": str(r["Replica Version"] or ""),
                    "timestamp": _json_time(r["Replica Timestamp"]),
                }
                for _, r in replicas_df.iterrows()
            ],
        }
    return {"settings": settings, "status": status}


def _qso_action(dataset_id: str, workspace_id: str, data: dict) -> dict:
    """Applies a query scale-out change, then returns the new state."""

    from sempy_labs._query_scale_out import (
        disable_qso,
        qso_sync,
        set_qso,
        set_semantic_model_storage_format,
    )

    op = data.get("op")
    with _QuietTool():
        if op == "apply":
            set_qso(
                dataset=dataset_id,
                auto_sync=bool(data.get("autoSync", True)),
                max_read_only_replicas=int(data.get("maxReplicas", -1)),
                workspace=workspace_id,
            )
        elif op == "disable":
            disable_qso(dataset=dataset_id, workspace=workspace_id)
        elif op == "storage":
            set_semantic_model_storage_format(
                dataset=dataset_id,
                storage_format=str(data.get("storageFormat") or ""),
                workspace=workspace_id,
            )
        elif op == "sync":
            qso_sync(dataset=dataset_id, workspace=workspace_id)
        elif op != "get":
            raise ValueError(f"Unknown query scale-out operation '{op}'.")
    return _qso_state(dataset_id, workspace_id)


def _column_residency(dataset_id: str, workspace_id: str) -> list:
    """Every column's Direct Lake residency (Is Resident / Temperature / Last Accessed)."""

    import sempy.fabric as fabric

    with _QuietTool():
        df = fabric.list_columns(
            dataset=dataset_id, workspace=workspace_id, extended=True
        )
    rows = []
    for _, r in df.iterrows():
        if str(r.get("Type", "")) == "RowNumber" or str(
            r.get("Column Name", "")
        ).startswith("RowNumber-"):
            continue
        temperature = r.get("Temperature")
        try:
            temperature = float(temperature) if temperature == temperature else 0.0
        except (TypeError, ValueError):
            temperature = 0.0
        rows.append(
            {
                "table": str(r.get("Table Name", "")),
                "column": str(r.get("Column Name", "")),
                "resident": str(r.get("Is Resident")).lower() in ("true", "yes", "1"),
                "temperature": temperature,
                "lastAccessed": _json_time(r.get("Last Accessed")),
            }
        )
    return rows


def _warm_cache(dataset_id: str, workspace_id: str, data: dict) -> dict:
    """Warms selected columns or a perspective's columns, then rescans residency."""

    import pandas as pd
    from sempy_labs.directlake._warm_cache import (
        _put_columns_into_memory,
        warm_direct_lake_cache_perspective,
    )

    warmed = 0
    with _QuietTool():
        if data.get("perspective"):
            df = warm_direct_lake_cache_perspective(
                dataset=dataset_id,
                perspective=str(data["perspective"]),
                add_dependencies=bool(data.get("addDependencies")),
                workspace=workspace_id,
            )
            warmed = 0 if df is None else len(df)
        else:
            columns = [
                {"Table Name": str(c["table"]), "Column Name": str(c["column"])}
                for c in data.get("columns") or []
                if c.get("table") and c.get("column")
            ]
            if not columns:
                raise ValueError("Select at least one column to warm.")
            _put_columns_into_memory(
                dataset=dataset_id,
                workspace=workspace_id,
                col_df=pd.DataFrame(columns),
                return_dataframe=False,
            )
            warmed = len(columns)
    return {"warmed": warmed, "columns": _column_residency(dataset_id, workspace_id)}


def _create_vpax_file(dataset_id: str, workspace_id: str, data: dict) -> dict:
    """Creates a .vpax file for the model in the chosen lakehouse's Files."""

    from sempy_labs._vpax import create_vpax

    lakehouse_id = str(data.get("lakehouseId") or "")
    lakehouse_workspace_id = str(data.get("lakehouseWorkspaceId") or "")
    file_name = re.sub(r"\.vpax$", "", str(data.get("fileName") or ""), flags=re.I)
    file_name = file_name.strip()
    if not lakehouse_id or not lakehouse_workspace_id or not file_name:
        raise ValueError("Select a workspace and lakehouse, and enter a file name.")
    with _QuietTool() as quiet:
        create_vpax(
            dataset=dataset_id,
            workspace=workspace_id,
            lakehouse=lakehouse_id,
            lakehouse_workspace=lakehouse_workspace_id,
            file_path=file_name,
            read_stats_from_data=bool(data.get("readStatsFromData")),
            direct_lake_stats_mode=str(
                data.get("directLakeStatsMode") or "ResidentOnly"
            ),
            overwrite=bool(data.get("overwrite")),
        )
    # create_vpax reports an existing file by printing instead of raising.
    if "already exists" in quiet.text:
        raise ValueError(
            f"Files/{file_name}.vpax already exists in that lakehouse. "
            "Turn on 'Overwrite' to replace it."
        )
    return {"filePath": f"Files/{file_name}.vpax"}


# ---------------------------------------------------------------------------
# DAXLib.org packages
# ---------------------------------------------------------------------------
_DAXLIB_QUERY_URL = "https://api.daxlib.org/v1/query"
_DAXLIB_GITHUB_CONTENTS = "https://api.github.com/repos/daxlib/daxlib/contents/packages"
_DAXLIB_GITHUB_RAW = "https://raw.githubusercontent.com/daxlib/daxlib/main/packages"
_DAXLIB_ID_ANNOTATION = "DAXLIB_PackageId"
_DAXLIB_VERSION_ANNOTATION = "DAXLIB_PackageVersion"
# DAX user-defined functions require this compatibility level.
_DAXLIB_MIN_COMPAT = 1702
_DAXLIB_CACHE_SECONDS = 600
# Do not toggle ``busy`` (which locks Save / reload) for tool window lookups.
_BACKGROUND_ACTIONS = (
    "daxlib_catalog",
    "daxlib_package",
    "generate_descriptions",
    "translate_texts",
    "infer_relationships",
    "qso",
    "warm_scan",
    "warm",
    "vpax_workspaces",
    "vpax_lakehouses",
    "vpax_create",
    "list_lakehouses",
    "list_views",
    "view_tables",
    "view_columns",
)
_daxlib_cache: Dict[object, Tuple[float, object]] = {}


def _daxlib_cached(key, loader):
    import time

    hit = _daxlib_cache.get(key)
    if hit and time.time() - hit[0] < _DAXLIB_CACHE_SECONDS:
        return hit[1]
    value = loader()
    _daxlib_cache[key] = (time.time(), value)
    return value


def _daxlib_get(url: str, accept: str = "application/json"):
    import requests

    return requests.get(
        url,
        headers={"User-Agent": "semantic-link-labs", "Accept": accept},
        timeout=30,
    )


def _daxlib_version_key(version: str):
    """Sort key for package versions: numeric parts, then release > prerelease."""

    core, _, pre = str(version).partition("-")
    parts = [int(p) if p.isdigit() else 0 for p in core.split(".")]
    parts += [0] * (4 - len(parts))
    pre_parts = tuple(
        (0, int(p), "") if p.isdigit() else (1, 0, p.lower())
        for p in pre.split(".")
        if pre
    )
    return (tuple(parts), 0 if pre else 1, pre_parts)


def _daxlib_catalog() -> List[dict]:
    """Every package published to DAXLib.org, most downloaded first."""

    def _load():
        packages: List[dict] = []
        seen = set()
        page = 100
        for skip in range(0, 5000, page):
            response = _daxlib_get(f"{_DAXLIB_QUERY_URL}?take={page}&skip={skip}")
            if not response.ok:
                raise ValueError(
                    "The DAXLib.org package list could not be loaded "
                    f"({response.status_code})."
                )
            items = (response.json() or {}).get("data") or []
            for item in items:
                package_id = str(item.get("id") or "")
                if not package_id or package_id.lower() in seen:
                    continue
                seen.add(package_id.lower())
                texts = {
                    field: [str(x) for x in (item.get(field) or []) if x]
                    for field in ("authors", "owners", "tags")
                }
                packages.append(
                    {
                        "id": package_id,
                        "version": str(item.get("version") or ""),
                        "downloads": int(item.get("downloads") or 0),
                        "description": str(item.get("description") or ""),
                        "published": str(item.get("published") or ""),
                        "hasIcon": item.get("hasIcon") is True,
                        "projectUrl": str(item.get("projectUrl") or ""),
                        **texts,
                    }
                )
            if len(items) < page:
                break
        packages.sort(key=lambda p: (-p["downloads"], p["id"].lower()))
        return packages

    return _daxlib_cached("catalog", _load)


def _daxlib_package_from_github(package_name: str, version: Optional[str]) -> dict:
    """Reads a package from the DAXLib GitHub repository (fallback source)."""

    lower = package_name.lower()
    shard = lower[0]
    response = _daxlib_get(
        f"{_DAXLIB_GITHUB_CONTENTS}/{shard}/{lower}", "application/vnd.github+json"
    )
    if response.status_code == 404:
        versions = []
    elif not response.ok:
        raise ValueError(
            f"The '{package_name}' package could not be looked up "
            f"({response.status_code})."
        )
    else:
        versions = [
            str(item.get("name"))
            for item in response.json() or []
            if item.get("type") == "dir" and item.get("name")
        ]
    versions.sort(key=_daxlib_version_key, reverse=True)
    if not versions:
        raise ValueError(f"No versions found for package '{package_name}'")
    selected = version or versions[0]
    if selected not in versions:
        raise ValueError(
            f"Version '{selected}' not found for package '{package_name}'. "
            f"Available versions: {', '.join(versions)}"
        )
    base = f"{_DAXLIB_GITHUB_RAW}/{shard}/{lower}/{selected}"
    tmdl = _daxlib_get(f"{base}/lib/functions.tmdl", "text/plain")
    if not tmdl.ok:
        raise ValueError(
            f"Could not read the functions of '{package_name}' version '{selected}'."
        )
    manifest = _daxlib_get(f"{base}/manifest.daxlib")
    return {
        "tmdl": tmdl.text,
        "version": selected,
        "versions": versions,
        "manifest": manifest.text if manifest.ok else None,
        "nuspec": None,
    }


def _daxlib_metadata(raw: dict, package_name: str) -> Tuple[str, str, str]:
    """The package id (canonical casing), description and authors."""

    import xml.etree.ElementTree as ET

    meta = {"id": "", "description": "", "authors": ""}
    try:
        manifest = json.loads(raw.get("manifest") or "{}")
        for field in meta:
            value = manifest.get(field)
            meta[field] = ", ".join(value) if isinstance(value, list) else value or ""
    except Exception:
        pass
    if raw.get("nuspec") and not all(meta.values()):
        try:
            for el in ET.fromstring(raw["nuspec"]).iter():
                field = str(el.tag).rsplit("}", 1)[-1]
                if field in meta and not meta[field] and el.text:
                    meta[field] = el.text.strip()
        except Exception:
            pass
    return (
        str(meta["id"] or package_name),
        str(meta["description"]),
        str(meta["authors"]),
    )


def _daxlib_definition(body: str) -> str:
    """Removes the TMDL indentation from a package function's definition."""

    import textwrap

    lines = body.strip("\n").rstrip().split("\n")
    if lines[0][:1] in (" ", "\t"):
        return textwrap.dedent("\n".join(lines))
    # The TMDL parser consumed the first line's indentation; the rest keep it.
    rest = textwrap.dedent("\n".join(lines[1:]))
    return lines[0] + ("\n" + rest if len(lines) > 1 else "")


def _daxlib_package(package_name: str, version: Optional[str] = None) -> dict:
    """A package version's functions, metadata and the available versions."""

    from sempy_labs.daxlib._functions import (
        _download_daxlib_package,
        extract_functions_list,
    )

    package_name = str(package_name or "").strip()
    if not package_name:
        raise ValueError("A package name is required.")
    version = str(version or "").strip() or None

    def _load():
        try:
            raw = _download_daxlib_package(package_name, version)
        except ValueError:
            raise
        except Exception:
            # The DAXLib .NET client is unavailable (e.g. outside Fabric).
            raw = _daxlib_package_from_github(package_name, version)
        package_id, description, authors = _daxlib_metadata(raw, package_name)
        functions = [
            {"name": f["name"], "definition": _daxlib_definition(f["definition"])}
            for f in extract_functions_list(raw["tmdl"])
        ]
        return {
            "packageId": package_id,
            "version": raw["version"],
            "versions": list(raw["versions"]),
            "description": description,
            "authors": authors,
            "functions": functions,
        }

    return _daxlib_cached(("package", package_name.lower(), version), _load)


def _find_annotation(obj, name: str):
    lower = name.lower()
    return next((a for a in obj.Annotations if str(a.Name).lower() == lower), None)


def _set_annotation(obj, name: str, value: str) -> None:
    import Microsoft.AnalysisServices.Tabular as TOM

    existing = _find_annotation(obj, name)
    if existing is not None:
        existing.Value = value
        return
    annotation = TOM.Annotation()
    annotation.Name = name
    annotation.Value = value
    obj.Annotations.Add(annotation)


def _daxlib_owner(function, package_id: str) -> bool:
    """Whether a model function was installed from the given package."""

    tagged = _find_annotation(function, _DAXLIB_ID_ANNOTATION)
    if tagged is not None:
        return str(tagged.Value).lower() == package_id.lower()
    # Installed without DAXLib annotations (e.g. by sempy_labs): by name prefix.
    return str(function.Name).lower().startswith(package_id.lower() + ".")


def _install_daxlib_package(model, package_id: str, props: dict) -> None:
    """Adds / updates a DAXLib package's functions in the model.

    Functions of the package which are not in this version are removed; the
    rest are upserted and tagged with the package id and version.
    """

    import Microsoft.AnalysisServices.Tabular as TOM

    version = str(props.get("version") or "")
    functions = [f for f in props.get("functions") or [] if f.get("name")]
    if not version or not functions:
        raise ValueError(f"The '{package_id}' package has no functions to add.")
    database = model.Database
    if int(database.CompatibilityLevel) < _DAXLIB_MIN_COMPAT:
        if not props.get("upgradeCompatibility"):
            raise ValueError(
                "DAX user-defined functions require compatibility level "
                f"{_DAXLIB_MIN_COMPAT} or higher."
            )
        database.CompatibilityLevel = _DAXLIB_MIN_COMPAT

    wanted = {str(f["name"]) for f in functions}
    for function in list(model.Functions):
        if str(function.Name) not in wanted and _daxlib_owner(function, package_id):
            model.Functions.Remove(function)
    for spec in functions:
        name = str(spec["name"])
        if model.Functions.ContainsName(name):
            function = model.Functions[name]
            function.Expression = str(spec.get("definition") or "")
        else:
            function = TOM.Function()
            function.Name = name
            function.Expression = str(spec.get("definition") or "")
            _new_lineage_tag(function)
            model.Functions.Add(function)
        _set_annotation(function, _DAXLIB_ID_ANNOTATION, package_id)
        _set_annotation(function, _DAXLIB_VERSION_ANNOTATION, version)
        # sempy_labs' add_package_to_semantic_model tags by the package name.
        legacy = _find_annotation(function, package_id)
        if legacy is not None:
            legacy.Value = version


@log
def semantic_explorer(
    dataset: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    dark_mode: bool = False,
):
    """
    Displays an interactive explorer for browsing and editing a semantic model.

    The explorer shows the whole model in one tree - tables, columns, measures,
    hierarchies and levels, calculation items, partitions, relationships,
    expressions, functions, perspectives, roles and cultures - with search, an
    object type filter, and "view as" perspective / culture modes. Selecting an
    object shows its properties, its DAX or Power Query (M) expressions (syntax
    highlighted), annotations, extended properties, translations and
    perspective membership, and lets you view its TMDL definition and its
    dependencies. Model errors (objects in an error state) are listed in one
    place.

    In read-write mode, objects can be renamed, hidden, re-described and
    otherwise edited, expressions can be edited and formatted with DAX
    Formatter, and objects can be added, duplicated and deleted. Renaming a
    table, column or measure automatically updates the DAX expressions which
    reference it. Changes are staged locally (with undo / redo and a change
    history) and are only written to the semantic model on Save.

    The semantic model picker can also create a new, empty semantic model
    (compatibility level 1706) in a workspace and open it in the explorer.

    The explorer also opens semantic views (preview): the YAML definitions in a
    lakehouse's Context/Semantic Views folder. A view's datasets, columns,
    relationships and metrics are shown in a read-only tree built from its YAML
    and, once it is published, the generated TMDL in SystemContext. The YAML
    and TMDL files can be viewed, and a new view can be created with a starter
    definition file. In edit mode, the view's description, datasets,
    relationships and metrics can be edited, added and deleted (renames update
    the references to them); changes are staged with undo / redo and written to
    the YAML files only on Save. Comments in the YAML files are kept when the
    ruamel.yaml package is installed.

    Parameters
    ----------
    dataset : str | uuid.UUID, default=None
        Name or ID of the semantic model. If None, the explorer opens on a start
        page to choose between a semantic model (open an existing one or create
        a new, empty one) and a semantic view (open an existing one or create a
        new one in a lakehouse).
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    dark_mode : bool, default=False
        If True, renders the explorer with a dark color theme. If False, renders
        with a light color theme.
    """

    try:
        import anywidget
        import traitlets
    except ImportError as e:
        raise ImportError(
            "The 'semantic_explorer' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from e

    from IPython.display import display
    from sempy_labs._helper_functions import (
        resolve_lakehouse_name_and_id,
        resolve_workspace_name_and_id,
    )
    from sempy_labs._ui_components import (
        list_picker_datasets,
        list_picker_lakehouses,
        list_picker_workspaces,
        run_widget_task,
    )
    from sempy_labs.tom import connect_semantic_model

    if dataset is not None:
        state = _collect_explorer_state(dataset, workspace)
        dataset_chosen = True
    else:
        workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
        state = {
            "dataset_name": "",
            "dataset_id": "",
            "workspace_name": str(workspace_name or ""),
            "workspace_id": str(workspace_id or ""),
            "payload": {},
        }
        dataset_chosen = False

    ctx = {
        "dataset_id": state["dataset_id"] or None,
        "workspace_id": state["workspace_id"] or None,
        "view": None,
    }

    class SemanticExplorerWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        model_data = traitlets.Dict().tag(sync=True)
        model_version = traitlets.Int(0).tag(sync=True)
        result = traitlets.Dict().tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        busy = traitlets.Unicode("").tag(sync=True)
        dataset_name = traitlets.Unicode("").tag(sync=True)
        workspace_name = traitlets.Unicode("").tag(sync=True)
        dataset_chosen = traitlets.Bool(False).tag(sync=True)
        active_workspace_id = traitlets.Unicode("").tag(sync=True)
        active_dataset_id = traitlets.Unicode("").tag(sync=True)
        available_workspaces = traitlets.List().tag(sync=True)
        available_datasets = traitlets.List().tag(sync=True)
        picker_loading = traitlets.Bool(False).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)
        # "" (nothing open yet), "model" or "view".
        explorer_kind = traitlets.Unicode("").tag(sync=True)
        view_data = traitlets.Dict().tag(sync=True)
        view_version = traitlets.Int(0).tag(sync=True)

    widget = SemanticExplorerWidget(
        explorer_kind="model" if dataset_chosen else "",
        model_data=state["payload"],
        model_version=1 if dataset_chosen else 0,
        result={},
        status={},
        pending_action={},
        run=0,
        busy="",
        dataset_name=state["dataset_name"],
        workspace_name=state["workspace_name"],
        dataset_chosen=dataset_chosen,
        active_workspace_id=state["workspace_id"],
        active_dataset_id=state["dataset_id"],
        available_workspaces=(
            [{"id": state["workspace_id"], "name": state["workspace_name"]}]
            if state["workspace_id"]
            else []
        ),
        available_datasets=(
            [{"id": state["dataset_id"], "name": state["dataset_name"]}]
            if dataset_chosen
            else []
        ),
        picker_loading=False,
        dark_mode=bool(dark_mode),
    )

    def _load(dataset_id: str, workspace_id: str) -> None:
        new_state = _collect_explorer_state(dataset_id, workspace_id)
        ctx["dataset_id"] = new_state["dataset_id"]
        ctx["workspace_id"] = new_state["workspace_id"]
        widget.dataset_name = new_state["dataset_name"]
        widget.workspace_name = new_state["workspace_name"]
        widget.active_dataset_id = new_state["dataset_id"]
        widget.active_workspace_id = new_state["workspace_id"]
        widget.model_data = new_state["payload"]
        widget.dataset_chosen = True
        widget.explorer_kind = "model"
        widget.model_version += 1

    def _load_view(workspace_id: str, lakehouse_id: str, view_name: str) -> None:
        payload = _semantic_view_payload(workspace_id, lakehouse_id, view_name)
        workspace_name, _ = resolve_workspace_name_and_id(workspace_id)
        lakehouse_name, _ = resolve_lakehouse_name_and_id(lakehouse_id, workspace_id)
        payload["view"].update(
            {
                "workspace_id": workspace_id,
                "workspace_name": str(workspace_name),
                "lakehouse_id": lakehouse_id,
                "lakehouse_name": str(lakehouse_name),
            }
        )
        ctx["view"] = (workspace_id, lakehouse_id, view_name)
        widget.view_data = payload
        widget.explorer_kind = "view"
        widget.view_version += 1

    def _reply(data: dict, ok: bool, payload: Optional[dict] = None, error: str = ""):
        widget.result = {
            "request_id": data.get("request_id", 0),
            "action": data.get("action", ""),
            "ok": ok,
            "data": payload or {},
            "error": error,
        }

    def _handle(data: dict) -> None:
        action = data.get("action")
        try:
            if action == "list_workspaces":
                widget.available_workspaces = list_picker_workspaces(
                    ctx["workspace_id"], widget.workspace_name
                )
                target = str(data.get("workspace_id") or "")
                if target:
                    widget.available_datasets = list_picker_datasets(target)
                return
            if action == "list_datasets":
                target = str(data.get("workspace_id") or "")
                widget.available_datasets = (
                    list_picker_datasets(target) if target else []
                )
                return
            if action == "connect":
                workspace_id = str(data.get("workspace_id") or "")
                dataset_id = str(data.get("dataset_id") or "")
                if not workspace_id or not dataset_id:
                    raise ValueError("Select a workspace and a semantic model.")
                _load(dataset_id, workspace_id)
                widget.status = {}
                _reply(data, True)
                return
            if action == "create_model":
                workspace_id = str(data.get("workspace_id") or "")
                name = str(data.get("name") or "").strip()
                dataset_id = _create_blank_model(workspace_id, name)
                widget.available_datasets = list_picker_datasets(workspace_id)
                _load(dataset_id, workspace_id)
                widget.status = {
                    "message": f"Created the '{name}' semantic model.",
                    "kind": "success",
                }
                _reply(data, True, {"dataset_id": dataset_id})
                return
            if action == "list_lakehouses":
                target = str(data.get("workspace_id") or "")
                _reply(data, True, {"items": list_picker_lakehouses(target)})
                return
            if action == "list_views":
                views = _list_semantic_views(
                    str(data.get("workspace_id") or ""),
                    str(data.get("lakehouse_id") or ""),
                )
                _reply(data, True, {"items": views})
                return
            if action == "open_view":
                workspace_id = str(data.get("workspace_id") or "")
                lakehouse_id = str(data.get("lakehouse_id") or "")
                view_name = str(data.get("view") or "")
                if not workspace_id or not lakehouse_id or not view_name:
                    raise ValueError(
                        "Select a workspace, a lakehouse and a semantic view."
                    )
                _load_view(workspace_id, lakehouse_id, view_name)
                widget.status = {}
                _reply(data, True)
                return
            if action == "create_view":
                workspace_id = str(data.get("workspace_id") or "")
                lakehouse_id = str(data.get("lakehouse_id") or "")
                name = _create_semantic_view(
                    workspace_id, lakehouse_id, str(data.get("name") or "")
                )
                _load_view(workspace_id, lakehouse_id, name)
                widget.status = {
                    "message": f"Created the '{name}' semantic view.",
                    "kind": "success",
                }
                _reply(data, True, {"view": name})
                return
            if action == "save_view":
                if not ctx["view"]:
                    raise ValueError("Select a semantic view first.")
                changes = dict(data.get("changes") or {})
                count, comments_kept = 0, True
                if any(changes.get(k) for k in ("updates", "deletes", "creates")) or (
                    changes.get("description") is not None
                ):
                    count, comments_kept = _save_semantic_view(
                        *ctx["view"], changes, data.get("etags")
                    )
                parts = []
                if count:
                    parts.append(
                        f"Saved {count} change{'s' if count != 1 else ''} to the "
                        "semantic view's YAML files."
                    )
                if not comments_kept:
                    parts.append(
                        "Comments in the changed files were not kept (install "
                        "ruamel.yaml to keep them)."
                    )
                kind = "success"
                published = False
                if data.get("publish"):
                    if count:
                        _load_view(*ctx["view"])
                    widget.status = {
                        "message": " ".join(parts + ["Publishing\u2026"]),
                        "kind": "info",
                    }
                    try:
                        warnings = _publish_view(*ctx["view"])
                        published = True
                        parts.append("Published the semantic view.")
                        if warnings:
                            kind = "info"
                            parts.append(
                                f"{len(warnings)} warning"
                                f"{'s' if len(warnings) != 1 else ''}: "
                                + "; ".join(warnings[:5])
                            )
                    except Exception as e:
                        kind = "error"
                        parts.append(f"Publishing failed: {e}")
                    if published:
                        workspace_id, _, view_name = ctx["view"]
                        widget.status = {
                            "message": " ".join(
                                parts
                                + [f"Refreshing the '{view_name}' semantic model\u2026"]
                            ),
                            "kind": "info",
                        }
                        try:
                            _refresh_view_model(workspace_id, view_name)
                            parts.append(f"Refreshed the '{view_name}' semantic model.")
                        except Exception as e:
                            kind = "error"
                            parts.append(
                                f"Refreshing the '{view_name}' semantic model failed: {e}"
                            )
                _load_view(*ctx["view"])
                widget.status = {"message": " ".join(parts) or "Saved.", "kind": kind}
                _reply(data, True, {"count": count, "published": published})
                return
            if action == "view_tables":
                if not ctx["view"]:
                    raise ValueError("Select a semantic view first.")
                workspace_id, lakehouse_id, _ = ctx["view"]
                tables = _lakehouse_tables(workspace_id, lakehouse_id)
                _reply(data, True, {"items": tables})
                return
            if action == "view_columns":
                if not ctx["view"]:
                    raise ValueError("Select a semantic view first.")
                workspace_id, lakehouse_id, _ = ctx["view"]
                columns = _lakehouse_table_columns(
                    workspace_id, lakehouse_id, str(data.get("source") or "")
                )
                _reply(data, True, {"columns": columns})
                return
            if action == "format_dax":
                formatted = _format_dax_expression(str(data.get("expression") or ""))
                _reply(data, True, {"formatted": formatted})
                return
            if action == "reload_view":
                if not ctx["view"]:
                    raise ValueError("Select a semantic view first.")
                _load_view(*ctx["view"])
                widget.status = {
                    "message": "Semantic view reloaded.",
                    "kind": "success",
                }
                _reply(data, True)
                return
            if action == "daxlib_catalog":
                _reply(data, True, {"packages": _daxlib_catalog()})
                return
            if action == "daxlib_package":
                _reply(
                    data,
                    True,
                    _daxlib_package(data.get("package"), data.get("version")),
                )
                return
            if action == "translate_texts":
                translated = _translate_texts(
                    data.get("key"),
                    data.get("region"),
                    list(data.get("texts") or []),
                    list(data.get("languages") or []),
                )
                _reply(data, True, {"translations": translated})
                return

            if not ctx["dataset_id"]:
                raise ValueError("Select a semantic model first.")

            if action == "reload":
                _load(ctx["dataset_id"], ctx["workspace_id"])
                widget.status = {
                    "message": "Model metadata reloaded.",
                    "kind": "success",
                }
                _reply(data, True)
            elif action == "tmdl":
                with connect_semantic_model(
                    dataset=ctx["dataset_id"],
                    workspace=ctx["workspace_id"],
                    readonly=True,
                ) as tom:
                    text, language = _object_tmdl(tom.model, data.get("target") or {})
                _reply(data, True, {"text": text, "language": language})
            elif action == "dependencies":
                _reply(
                    data,
                    True,
                    _model_dependencies(ctx["dataset_id"], ctx["workspace_id"]),
                )
            elif action == "generate_descriptions":
                generated = _generate_measure_descriptions(
                    ctx["workspace_id"], list(data.get("measures") or [])
                )
                _reply(data, True, {"descriptions": generated})
            elif action == "infer_relationships":
                inferred = _infer_relationships(
                    ctx["dataset_id"],
                    ctx["workspace_id"],
                    list(data.get("pairs") or []),
                )
                _reply(data, True, {"results": inferred})
            elif action == "qso":
                state = _qso_action(ctx["dataset_id"], ctx["workspace_id"], data)
                _reply(data, True, state)
            elif action == "warm_scan":
                columns = _column_residency(ctx["dataset_id"], ctx["workspace_id"])
                _reply(data, True, {"columns": columns})
            elif action == "warm":
                warmed = _warm_cache(ctx["dataset_id"], ctx["workspace_id"], data)
                _reply(data, True, warmed)
            elif action == "vpax_workspaces":
                workspaces = list_picker_workspaces(
                    ctx["workspace_id"], widget.workspace_name
                )
                _reply(data, True, {"items": workspaces})
            elif action == "vpax_lakehouses":
                target = str(data.get("workspace_id") or "")
                _reply(data, True, {"items": list_picker_lakehouses(target)})
            elif action == "vpax_create":
                created = _create_vpax_file(
                    ctx["dataset_id"], ctx["workspace_id"], data
                )
                _reply(data, True, created)
            elif action == "save":
                with connect_semantic_model(
                    dataset=ctx["dataset_id"],
                    workspace=ctx["workspace_id"],
                    readonly=False,
                ) as tom:
                    count = _apply_changes(
                        tom,
                        list(data.get("creates") or []),
                        list(data.get("edits") or []),
                        list(data.get("ops") or []),
                    )
                _load(ctx["dataset_id"], ctx["workspace_id"])
                widget.status = {
                    "message": f"Saved {count} change{'s' if count != 1 else ''} "
                    "to the semantic model.",
                    "kind": "success",
                }
                _reply(data, True, {"count": count})
        except Exception as e:
            if action in ("list_workspaces", "list_datasets"):
                widget.status = {"message": f"Error: {e}", "kind": "error"}
            else:
                _reply(data, False, error=str(e))
                if action in ("connect", "reload", "save", "reload_view", "save_view"):
                    widget.status = {"message": f"Error: {e}", "kind": "error"}
        finally:
            if action not in _BACKGROUND_ACTIONS:
                widget.busy = ""
            if action in ("list_workspaces", "list_datasets", "connect"):
                widget.picker_loading = False

    def _on_run(_change):
        data = dict(widget.pending_action or {})
        if not data.get("action"):
            return
        if data["action"] not in _BACKGROUND_ACTIONS:
            widget.busy = str(data.get("action"))
        run_widget_task(_handle, (data,))

    widget.observe(_on_run, names=["run"])

    # The widget reference is kept alive by the observer closure; it is not
    # returned so Jupyter does not render it a second time.
    display(widget)
