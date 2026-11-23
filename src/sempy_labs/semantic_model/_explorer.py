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
/* Picker actions use the pill buttons shared by the other tools' pickers. */
.slls-sx-picker-actions .slls-sx-btn {
    height: auto; padding: 7px 16px; border-radius: 999px; font-size: 13.5px;
    background: var(--ui-surface); border-color: var(--ui-border-strong);
}
.slls-sx-picker-actions .slls-sx-btn:hover:not(:disabled) { background: var(--ui-surface-2); border-color: var(--ui-text-tertiary); }
.slls-sx-picker-actions .slls-sx-btn.primary { background: var(--ui-accent); border-color: var(--ui-accent); color: var(--ui-on-accent); }
.slls-sx-picker-actions .slls-sx-btn.primary:hover:not(:disabled) { background: var(--ui-accent-hover); border-color: var(--ui-accent-hover); }

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
    };
    const JSON_PROPS = new Set(["Annotations", "Translations", "Perspectives", "Members", "RlsFilters"]);
    const PANE_MIN = 240, PANE_MAX = 640;
    // Starting definition of a new named expression (matches the kernel side).
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
    let pickerOpen = model.get("dataset_chosen") !== true;
    let pickerWs = model.get("active_workspace_id") || "";
    let pickerDs = model.get("active_dataset_id") || "";
    let workspacesRequested = false;
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
            return `${end(node.from_table, node.from_column)} ${marks} ${end(node.to_table, node.to_column)}`;
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
        const c = createFor(node);
        pushUndo(o.coalesce ? editKey(node.key, prop) : "");
        if (c) {
            if (prop === "Name") c.name = String(newValue || "");
            else c.props[prop] = newValue;
        } else {
            const ek = editKey(node.key, prop);
            const old = originalValue(node, prop);
            if (same(old, newValue)) delete staging.edits[ek];
            else {
                staging.edits[ek] = {
                    key: node.key, target: targetOf(node), property: prop, label: o.label || prop,
                    objectType: node.type, oldValue: old, newValue, userValue: newValue,
                };
            }
            recomputeCascade();
        }
        afterChange(o);
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
            return;
        }
        renderTree();
        renderDetail();
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

    function loaded() { return model.get("dataset_chosen") === true && data && data.model; }
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
        if (model.get("dataset_chosen") !== true) sub.textContent = "No semantic model selected";
        else sub.innerHTML = `<b>${esc(ds)}</b>${ds && ws ? '<span class="slls-sx-sep">\u00b7</span>' : ""}${esc(ws)}`;
        sub.title = sub.textContent;
        titleWrap.appendChild(sub);
        header.appendChild(titleWrap);
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
        renderAll();
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
        const loading = model.get("picker_loading") === true;
        const connecting = model.get("busy") === "connect";
        const card = h("div", "slls-sx-picker-card");
        const top = h("div", "");
        top.style.cssText = "display:flex;align-items:flex-start;justify-content:space-between;gap:12px";
        const tt = h("div", "");
        tt.append(h("div", "slls-sx-picker-title", "Select a semantic model"),
            h("div", "slls-sx-picker-sub", "Choose the workspace and semantic model to explore."));
        const reload = button("sl-reload-btn" + (loading ? " sl-spinning" : ""), "", "refresh", () => {
            workspacesRequested = false;
            ensureWorkspaces();
        }, "Reload workspaces and semantic models");
        reload.disabled = loading || connecting;
        top.append(tt, reload);
        card.appendChild(top);
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
        if (model.get("dataset_chosen") === true) {
            actions.appendChild(button("slls-sx-btn", "Cancel", "", () => { pickerOpen = false; renderAll(); }));
        }
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
            const createType = { roles: "role", perspectives: "perspective", expressions: "expression" }[folder];
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
        if (!root.isConnected || mode !== "write") return;
        const inField = ev.target && /^(INPUT|TEXTAREA|SELECT)$/.test(ev.target.tagName);
        if (inField || !root.contains(document.activeElement) && document.activeElement !== document.body) return;
        if ((ev.ctrlKey || ev.metaKey) && ev.key.toLowerCase() === "z") { ev.preventDefault(); if (ev.shiftKey) redo(); else undo(); }
        else if ((ev.ctrlKey || ev.metaKey) && ev.key.toLowerCase() === "y") { ev.preventDefault(); redo(); }
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
        span.textContent = typeof v === "number" ? v.toLocaleString() : String(v);
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
        const lines = String(text || "").split("\n");
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
        { label: "Field parameter manager", icon: "field_parameter", open: () => openFieldParameterManager() },
    ];
    // An open tool window redraws itself when the explorer re-renders (mode
    // switch, save, reload).
    let toolRedraw = null;
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
        toolRedraw = () => { if (m.overlay.isConnected) draw(true); };
        m.onClose = () => { toolRedraw = null; };
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
            const sw = h("button", "slls-sx-mode" + (mode === "write" ? " write" : ""));
            sw.type = "button";
            sw.setAttribute("role", "switch");
            sw.setAttribute("aria-checked", String(mode === "write"));
            sw.title = mode === "write" ? "Read-write mode (editing enabled)" : "Read-only mode \u2014 click to enable editing";
            sw.innerHTML = (mode === "write" ? ICON.pencil : ICON.eye) + '<span class="slls-sx-track"></span>';
            sw.appendChild(document.createTextNode(mode === "write" ? "Edit" : "Read-only"));
            sw.disabled = saving;
            sw.addEventListener("click", () => setMode(mode === "write" ? "read" : "write"));
            m.actions.appendChild(sw);
            clear(m.body);
            const layout = h("div", "slls-sx-fpm");
            layout.append(drawSidebar(list), drawMain());
            m.body.appendChild(layout);
            refreshControls();
        }
        draw();
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
        if (toolRedraw) toolRedraw();
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
    fullscreen_css as _ui_fullscreen_css,
    fullscreen_setup_js as _ui_fullscreen_setup_js,
    render_attribution_html as _ui_render_attribution_html,
    scoped_attribution_css as _ui_scoped_attribution_css,
    scoped_button_press_css as _ui_scoped_button_press_css,
    scoped_header_css as _ui_scoped_header_css,
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
    "more", "arrow_up", "arrow_down", "external_link", "info",
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
    + _ui_fullscreen_css(".slls-sx", "slls-sx-fs", bg_var="var(--ui-bg-solid)")
)

_WIDGET_JS = (
    _ui_fullscreen_setup_js()
    + "\n"
    + _UI_SEARCH_SELECT_JS
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

    Parameters
    ----------
    dataset : str | uuid.UUID, default=None
        Name or ID of the semantic model. If None, the explorer opens with a
        workspace and semantic model picker.
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
    from sempy_labs._helper_functions import resolve_workspace_name_and_id
    from sempy_labs._ui_components import (
        list_picker_datasets,
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

    widget = SemanticExplorerWidget(
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
        widget.model_version += 1

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
            elif action == "format_dax":
                formatted = _format_dax_expression(str(data.get("expression") or ""))
                _reply(data, True, {"formatted": formatted})
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
                if action in ("connect", "reload", "save"):
                    widget.status = {"message": f"Error: {e}", "kind": "error"}
        finally:
            widget.busy = ""
            if action in ("list_workspaces", "list_datasets", "connect"):
                widget.picker_loading = False

    def _on_run(_change):
        data = dict(widget.pending_action or {})
        if not data.get("action"):
            return
        widget.busy = str(data.get("action"))
        run_widget_task(_handle, (data,))

    widget.observe(_on_run, names=["run"])

    # The widget reference is kept alive by the observer closure; it is not
    # returned so Jupyter does not render it a second time.
    display(widget)
