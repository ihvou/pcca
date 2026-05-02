from __future__ import annotations

import json
import logging
import secrets
import socket
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from pcca.config import Settings
from pcca.observability import monotonic_ms_since, new_action_id, summarize_payload
from pcca.services.desktop_command_service import DesktopCommandService

logger = logging.getLogger(__name__)

INDEX_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>PCCA Wizard</title>
  <style>
    :root {
      --bg: #0a0b0d;
      --surface: #111315;
      --surface-hi: #16181b;
      --ink: #e6e8eb;
      --muted: #6b7280;
      --line: #2a2d31;
      --line-hi: #3a3d42;
      --accent: #4ade80;
      --accent-dim: rgba(74, 222, 128, 0.10);
      --warn: #fbbf24;
      --warn-dim: rgba(251, 191, 36, 0.08);
      --bad: #f87171;
      --bad-dim: rgba(248, 113, 113, 0.08);
      --mono: 'JetBrains Mono', 'SF Mono', SFMono-Regular, Menlo, ui-monospace, monospace;
      --sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; color: var(--ink); background: var(--bg); font: 13px/1.5 var(--sans); -webkit-font-smoothing: antialiased; }
    .shell { max-width: 1280px; margin: 0 auto; padding: 16px 20px 32px; }
    header { display: flex; justify-content: space-between; align-items: baseline; gap: 16px; padding: 8px 0 12px; border-bottom: 1px solid var(--line); margin-bottom: 16px; }
    .brand h1 { margin: 0; font: 600 13px/1.2 var(--mono); letter-spacing: 0; text-transform: uppercase; color: var(--ink); }
    .brand h1::before { content: "▸ "; color: var(--accent); font-weight: 400; }
    .brand p { margin: 4px 0 0; color: var(--muted); font-size: 12px; line-height: 1.4; max-width: 720px; }
    .pill { border: 1px solid var(--line); background: var(--surface); border-radius: 2px; padding: 3px 8px; font: 11px var(--mono); color: var(--muted); }
    .tabs { display: flex; gap: 0; margin: 0 0 16px; border-bottom: 1px solid var(--line); padding: 0; background: transparent; box-shadow: none; }
    .tab { background: transparent; border: 0; border-bottom: 2px solid transparent; border-radius: 0; padding: 8px 14px; font: 12px var(--mono); color: var(--muted); cursor: pointer; letter-spacing: 0.04em; text-transform: uppercase; box-shadow: none; transition: color 80ms, border-color 80ms; }
    .tab:hover { color: var(--ink); }
    .tab.active { color: var(--ink); border-bottom-color: var(--accent); background: transparent; }
    .view { display: none; }
    .view.active { display: block; }
    .grid { display: grid; grid-template-columns: minmax(0, 1.4fr) minmax(280px, 0.6fr); gap: 12px; align-items: start; }
    .stack { display: grid; gap: 12px; }
    .card { border: 1px solid var(--line); background: var(--surface); border-radius: 2px; padding: 14px; box-shadow: none; backdrop-filter: none; }
    .card.tight { padding: 10px 12px; }
    h2, h3 { margin: 0 0 10px; font: 600 12px var(--mono); letter-spacing: 0.02em; text-transform: uppercase; color: var(--ink); }
    h3 { font-size: 11px; color: var(--muted); }
    p { color: var(--muted); font-size: 12px; line-height: 1.5; margin: 0 0 10px; }
    label { display: grid; gap: 4px; color: var(--muted); font: 11px var(--mono); text-transform: uppercase; letter-spacing: 0.04em; }
    input, select, textarea { width: 100%; border: 1px solid var(--line); border-radius: 2px; padding: 6px 8px; background: var(--bg); color: var(--ink); font: 13px var(--sans); outline: none; transition: border-color 80ms; }
    textarea { font: 12px/1.5 var(--mono); min-height: 96px; resize: vertical; }
    input:focus, select:focus, textarea:focus { border-color: var(--accent); box-shadow: none; }
    button { border: 1px solid var(--line); border-radius: 2px; padding: 5px 12px; color: var(--ink); background: var(--surface-hi); cursor: pointer; font: 600 12px var(--mono); letter-spacing: 0.02em; transition: border-color 80ms, color 80ms; box-shadow: none; }
    button:hover { border-color: var(--accent); color: var(--accent); }
    button.primary { background: var(--accent); color: var(--bg); border-color: var(--accent); }
    button.primary:hover { background: transparent; color: var(--accent); }
    button.secondary { color: var(--muted); }
    button.secondary:hover { color: var(--ink); border-color: var(--line-hi); }
    button.warn { background: var(--warn); color: var(--bg); border-color: var(--warn); }
    button.warn:hover { background: transparent; color: var(--warn); }
    button:disabled { opacity: 0.4; cursor: not-allowed; }
    button:disabled:hover { border-color: var(--line); color: var(--ink); }
    .actions { display: flex; gap: 6px; flex-wrap: wrap; align-items: center; }
    .split { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .row { display: grid; grid-template-columns: 1fr auto; gap: 10px; align-items: center; padding: 8px 10px; border: 1px solid var(--line); border-radius: 2px; background: var(--surface); font-size: 12px; }
    .row.clickable { cursor: pointer; transition: border-color 80ms; }
    .row.clickable:hover { border-color: var(--line-hi); }
    .row.selected { border-color: var(--accent); background: var(--accent-dim); }
    .row strong { display: block; font: 500 13px var(--mono); }
    .row small, .fine { display: block; color: var(--muted); font: 11px var(--mono); margin-top: 2px; overflow-wrap: anywhere; }
    .notice { border: 1px solid var(--warn); background: var(--warn-dim); color: var(--warn); border-radius: 2px; padding: 8px 10px; font: 12px/1.5 var(--mono); white-space: pre-wrap; }
    .notice.bad { border-color: var(--bad); background: var(--bad-dim); color: var(--bad); }
    .notice.ok { border-color: var(--accent); background: var(--accent-dim); color: var(--accent); }
    .list { display: grid; gap: 6px; }
    .logs, pre { max-height: 320px; overflow: auto; white-space: pre-wrap; font: 11px/1.5 var(--mono); color: var(--muted); background: var(--bg); border: 1px solid var(--line); border-radius: 2px; padding: 8px 10px; margin: 0; }
    .source-toolbar { display: grid; grid-template-columns: 1fr 1fr auto; gap: 8px; align-items: end; }
    @media (max-width: 860px) { .grid, .split, .source-toolbar { grid-template-columns: 1fr; } header { flex-direction: column; align-items: flex-start; } .tabs { overflow-x: auto; } }
    ::selection { background: var(--accent); color: var(--bg); }
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: var(--bg); }
    ::-webkit-scrollbar-thumb { background: var(--line); }
    ::-webkit-scrollbar-thumb:hover { background: var(--line-hi); }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div class="brand">
        <h1>PCCA · Local Curation Agent</h1>
        <p>Subjects · Sources · Briefs — local-first. Telegram is the UI.</p>
      </div>
      <div class="pill" id="agentBadge">agent: checking</div>
    </header>

    <nav class="tabs" aria-label="Wizard sections">
      <button class="tab active" data-tab="use" onclick="showTab('use')">Use</button>
      <button class="tab" data-tab="sources" onclick="showTab('sources')">Sources</button>
      <button class="tab" data-tab="config" onclick="showTab('config')">Config</button>
      <button class="tab" data-tab="debug" onclick="showTab('debug')">Debug</button>
    </nav>

    <section id="view-use" class="view active">
      <div class="grid">
        <div class="stack">
          <div class="card">
            <h2>Daily Control</h2>
            <p>Start or stop the local agent, then ask for a fresh Brief per subject. Get Brief automatically rebuilds when new content or changed preferences require it.</p>
            <div id="tokenWarning" class="notice bad" style="display:none"></div>
            <div class="actions">
              <button class="primary" onclick="postAction('/api/agent/start')">Start</button>
              <button class="secondary" onclick="postAction('/api/agent/stop')">Stop</button>
            </div>
            <div id="useStatus" class="notice" style="display:none; margin-top:12px"></div>
          </div>
          <div class="card">
            <h2>Subjects</h2>
            <p id="subjectHint">Describe your first subject in plain language. The Wizard will draft rules first; it will not save an empty-preference subject.</p>
            <div class="list" id="subjectsBox"></div>
          </div>
          <div class="card">
            <h2>Pending Drafts</h2>
            <p>Drafts from the Wizard and Telegram live in one queue. You can save or cancel them here; desktop drafts can also be confirmed from Telegram.</p>
            <div class="list" id="pendingDraftsBox"></div>
          </div>
          <div class="card">
            <h2>Add Subject</h2>
            <p>Say what should be included, avoided, and what would count as a high-quality update. A thin one-liner becomes a draft, not a saved subject.</p>
            <label>Free-form subject description
              <textarea id="subjectText" placeholder="Example: Track practical Claude Code / agentic coding updates. Include concrete workflow tips, release details, evals, and examples from builders. Avoid biography, generic AI hype, Skills tutorials with no real lesson, and listicles."></textarea>
            </label>
            <div class="actions" style="margin-top:10px">
              <button class="primary" onclick="draftSubject()">Draft Subject</button>
              <button id="saveDraftButton" onclick="confirmSubjectDraft()" disabled>Save Draft</button>
              <button class="secondary" onclick="cancelSubjectDraft()">Cancel</button>
            </div>
            <div id="subjectDraftStatus" class="notice" style="display:none; margin-top:12px"></div>
            <div id="draftBox" class="notice" style="display:none; margin-top:12px"></div>
          </div>
        </div>
        <div class="stack">
          <div class="card tight">
            <h3>Subject Detail</h3>
            <div id="subjectDetailBox" class="notice">Select a subject to inspect preferences, refine it, or reassign its Telegram route.</div>
          </div>
          <div class="card tight">
            <h3>Telegram Routes</h3>
            <p>Send /start to your bot from the chat or topic where Briefs should land.</p>
            <div class="list" id="routesBox"></div>
          </div>
          <div class="card tight">
            <h3>Current Status</h3>
            <pre id="compactState">Loading...</pre>
          </div>
        </div>
      </div>
    </section>

    <section id="view-sources" class="view">
      <div class="grid">
        <div class="stack">
          <div class="card">
            <h2>Get Sources</h2>
            <p>Choose a platform, or All, and import follows/subscriptions from logged-in browser sessions. If a per-platform session is missing, the Wizard will ask to run a repair flow inline.</p>
            <div class="source-toolbar">
              <label>Platform <select id="platform" onchange="updateActionControls(lastState || {})"></select></label>
              <label>Limit <input id="limit" type="number" min="1" max="500" value="100" /></label>
              <button id="getSourcesButton" class="primary" onclick="getSources()">Get Sources</button>
            </div>
            <p class="fine" style="margin-top:10px">Get Content runs the selected platform, or all platforms when All is selected.</p>
            <div class="actions" style="margin-top:10px"><button id="readContentButton" class="secondary" onclick="readContent()">Get Content</button></div>
            <div id="sourceStatus" class="notice" style="display:none; margin-top:12px"></div>
          </div>
          <div class="card">
            <h2>Source List</h2>
            <div class="split">
              <label>Filter platform <select id="sourceFilter" onchange="renderSources(lastState)"><option value="">All</option></select></label>
              <label>Status <select id="statusFilter" onchange="renderSources(lastState)"><option value="">All</option><option value="pending">Pending</option><option value="confirmed">Confirmed</option><option value="removed">Removed</option><option value="active">Active</option><option value="inactive">Inactive</option><option value="needs_reauth">Needs re-login</option></select></label>
            </div>
            <div class="actions" style="margin-top:10px"><button class="secondary" onclick="monitorSources()">Monitor Pending Sources</button></div>
            <div class="list" id="sourcesBox" style="margin-top:12px"></div>
          </div>
        </div>
        <div class="stack">
          <div class="card tight">
            <h3>Needs Re-login</h3>
            <div id="reauthBox" class="notice ok">No sources currently need re-login.</div>
          </div>
          <div class="card tight">
            <h3>Import Counts</h3>
            <pre id="sourceCounts">No imported sources yet.</pre>
          </div>
        </div>
      </div>
    </section>

    <section id="view-config" class="view">
      <div class="grid">
        <div class="card">
          <h2>Configuration</h2>
          <p>The token field is intentionally blank on reload. Leaving it blank preserves the existing token; entering a new value replaces it.</p>
          <div class="split">
            <label>Timezone <input id="timezone" value="UTC" /></label>
            <label>Brief time <input id="digestTime" value="08:30" /></label>
          </div>
          <label style="margin-top:10px">Telegram bot token <input id="telegramToken" type="password" placeholder="Leave blank to keep existing token" /></label>
          <div class="actions" style="margin-top:12px"><button onclick="saveSettings()">Save Config</button></div>
          <div id="configStatus" class="notice" style="display:none; margin-top:12px"></div>
        </div>
        <div class="card tight">
          <h3>Local Paths</h3>
          <pre id="pathsBox">Loading...</pre>
        </div>
      </div>
    </section>

    <section id="view-debug" class="view">
      <div class="grid">
        <div class="stack">
          <div class="card">
            <h2>State</h2>
            <pre id="stateBox">Loading...</pre>
          </div>
          <div class="card">
            <h2>Logs</h2>
            <pre id="logsBox" class="logs"></pre>
          </div>
        </div>
        <div class="stack">
          <div class="card tight">
            <h3>Debug Files</h3>
            <pre id="debugFiles">Loading...</pre>
          </div>
          <div class="card tight">
            <h3>Run Now</h3>
            <p>Manual all-platform collection and embedding repair actions.</p>
            <div class="actions">
              <button id="runAllContentButton" class="secondary" onclick="readContentAll()">Run All Content</button>
              <button id="backfillEmbeddingsButton" class="secondary" onclick="backfillEmbeddings()">Backfill Embeddings</button>
              <button id="rebuildAllRulesButton" class="secondary" onclick="rebuildAllSubjectRules()">Rebuild All Subjects</button>
            </div>
          </div>
          <div class="card tight">
            <h3>Failures</h3>
            <div id="failureBox" class="notice ok">No visible failures.</div>
          </div>
        </div>
      </div>
    </section>
  </div>
<script>
const token = new URLSearchParams(location.search).get('token');
const headers = {'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json'};
let lastState = null;
let busy = false;
let selectedSubjectId = null;
let refreshPausedUntil = 0;
function pauseRefresh(ms=15000) { refreshPausedUntil = Math.max(refreshPausedUntil, Date.now() + ms); }
function byId(id) { return document.getElementById(id); }
function formSnapshot() {
  const ids = ['subjectText', 'refineText', 'timezone', 'digestTime', 'telegramToken', 'platform', 'limit', 'sourceFilter', 'statusFilter', 'routeChat'];
  const out = {};
  for (const id of ids) {
    const el = byId(id);
    if (el) out[id] = el.value;
  }
  const active = document.activeElement && document.activeElement.id ? document.activeElement.id : null;
  return {values: out, active};
}
function restoreFormSnapshot(snapshot) {
  if (!snapshot || !snapshot.values) return;
  for (const [id, value] of Object.entries(snapshot.values)) {
    const el = byId(id);
    if (el && value !== undefined && value !== null) el.value = value;
  }
  if (snapshot.active) {
    const active = byId(snapshot.active);
    if (active) active.focus();
  }
}
function setBusy(next) { busy = next; document.querySelectorAll('button').forEach(b => b.disabled = next && !b.classList.contains('tab')); if (lastState) { renderDraft(lastState.subject_draft, lastState.subject_draft_actionable); updateActionControls(lastState); } }
function showTab(name) { document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === name)); document.querySelectorAll('.view').forEach(v => v.classList.toggle('active', v.id === `view-${name}`)); }
function notice(id, text, kind='') { const el = document.getElementById(id); el.style.display = text ? 'block' : 'none'; el.className = `notice ${kind}`.trim(); el.textContent = text || ''; }
function logLine(line) { const el = document.getElementById('logsBox'); el.textContent = `${new Date().toLocaleTimeString()} ${line}\n` + el.textContent; }
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
async function sendWizardEvent(event) {
  try {
    const payload = {
      timestamp: new Date().toISOString(),
      action_key: event.action_key || '',
      action_id: event.action_id || '',
      event_kind: event.event_kind || 'fetch_error',
      elapsed_ms: Math.max(0, Math.round(event.elapsed_ms || 0)),
      http_status: event.http_status || null,
      error_type: event.error_type || '',
      error_message: String(event.error_message || '').slice(0, 500),
    };
    let body = JSON.stringify(payload);
    if (body.length > 1024) {
      payload.error_message = payload.error_message.slice(0, 240) + '…';
      body = JSON.stringify(payload);
    }
    if (body.length <= 1024) {
      await fetch('/api/debug/wizard-events', {method:'POST', headers, body});
    }
  } catch (_err) {
    // Diagnostics must never break the user's action flow.
  }
}
function payloadFailureClass(payload) {
  return payload && payload.data ? payload.data.failure_class : (payload ? payload.failure_class : null);
}
function isSessionRepairable(errOrPayload) {
  const payload = errOrPayload && errOrPayload.payload ? errOrPayload.payload : errOrPayload;
  const status = errOrPayload && errOrPayload.httpStatus ? errOrPayload.httpStatus : (payload ? payload.http_status : null);
  return payloadFailureClass(payload) === 'session_challenge' || status === 401 || status === 403;
}
function errorFromPayload(payload, fallback='Action failed.') {
  const err = new Error((payload && (payload.message || payload.detail)) || fallback);
  err.payload = payload;
  err.failure_class = payloadFailureClass(payload);
  return err;
}
async function request(path, opts={}) {
  const timeoutMs = opts.timeoutMs || 120000;
  const startedAt = performance.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const cleanOpts = {...opts};
    delete cleanOpts.timeoutMs;
    delete cleanOpts.actionKey;
    delete cleanOpts.actionId;
    delete cleanOpts.recordSuccess;
    delete cleanOpts.allowOkFalse;
    const res = await fetch(path, {...cleanOpts, signal: controller.signal, headers: {...headers, ...(opts.headers || {})}});
    const data = await res.json();
    const alreadyRunning = data && data.data && data.data.already_running;
    if (opts.recordSuccess) {
      await sendWizardEvent({event_kind:'success', action_key: opts.actionKey, action_id: opts.actionId || data.action_id || (data.data && data.data.action_id), elapsed_ms: performance.now() - startedAt, http_status: res.status});
    }
    if (!res.ok || (data.ok === false && !alreadyRunning && !opts.allowOkFalse)) {
      const err = errorFromPayload(data, `Request failed: ${path}`);
      err.httpStatus = res.status;
      err.path = path;
      throw err;
    }
    return data;
  } catch (err) {
    const eventKind = err.name === 'AbortError' ? 'timeout' : 'fetch_error';
    await sendWizardEvent({event_kind: eventKind, action_key: opts.actionKey, action_id: opts.actionId, elapsed_ms: performance.now() - startedAt, http_status: err.httpStatus, error_type: err.name || err.constructor.name || 'Error', error_message: err.message || String(err)});
    if (err.name === 'AbortError') {
      throw new Error(`Still working after ${Math.round(timeoutMs / 1000)}s. Check Debug logs; avoid retrying immediately so the current import can finish.`);
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}
async function postAction(path, body={}, options={}) {
  try { setBusy(true); const data = await request(path, {method:'POST', timeoutMs: options.timeoutMs, body: JSON.stringify(body)}); logLine(`${data.action_id ? data.action_id + ' · ' : ''}${data.message || 'ok'}`); await loadState({force:true}); return data; }
  catch (err) { logLine(`ERROR: ${err.message}`); alert(err.message); }
  finally { setBusy(false); }
}
async function pollActionResult(actionId, actionKey, statusId=null) {
  const startedAt = performance.now();
  const deadline = Date.now() + 1800000;
  while (Date.now() < deadline) {
    await sleep(1500);
    const state = await refreshRunningState();
    const running = actionRunning(state || lastState || {}, actionKey);
    if (running && (!actionId || running.action_id === actionId)) {
      if (statusId) notice(statusId, `Running: ${running.label}`, '');
      continue;
    }
    const res = await fetch(`/api/actions/${encodeURIComponent(actionId)}/result`, {headers});
    let data = {};
    try { data = await res.json(); } catch (_err) { data = {}; }
    if (res.status === 404) continue;
    if (res.status === 410) {
      await sendWizardEvent({event_kind:'fetch_error', action_key: actionKey, action_id: actionId, elapsed_ms: performance.now() - startedAt, http_status: res.status, error_type:'ExpiredActionResult', error_message:data.message || 'Action result expired'});
      throw errorFromPayload(data, 'Action result expired. Check Debug logs; the backend may still have completed.');
    }
    if (!res.ok) {
      const err = errorFromPayload(data, `Action result failed: ${actionKey}`);
      err.httpStatus = res.status;
      throw err;
    }
    await sendWizardEvent({event_kind:'success', action_key: actionKey, action_id: actionId, elapsed_ms: performance.now() - startedAt, http_status: res.status});
    return data;
  }
  throw new Error(`Timed out waiting for ${actionKey}. Check Debug logs before retrying.`);
}
async function longAction(path, body={}, options={}) {
  const actionKey = options.actionKey || path;
  const statusId = options.statusId || null;
  const startedText = options.startedText || 'Starting action...';
  try {
    setBusy(true);
    pauseRefresh(1800000);
    if (statusId) notice(statusId, startedText, '');
    const kick = await request(path, {method:'POST', timeoutMs: 45000, actionKey, recordSuccess:true, body: JSON.stringify(body)});
    const actionId = (kick.data && kick.data.action_id) || kick.action_id;
    logLine(`${actionId ? actionId + ' · ' : ''}${kick.message || 'action started'}`);
    if (!actionId) return kick;
    const data = await pollActionResult(actionId, actionKey, statusId);
    logLine(`${actionId} · ${data.message || 'action finished'}`);
    await loadState({force:true});
    if (data.ok === false) throw errorFromPayload(data, data.message || 'Action failed.');
    return data;
  } finally {
    setBusy(false);
  }
}
async function saveSettings() {
  const data = await postAction('/api/settings', {token: byId('telegramToken').value, timezone: byId('timezone').value, digest_time: byId('digestTime').value});
  if (data) notice('configStatus', data.message, 'ok');
}
async function getSources() {
  const platformEl = byId('platform');
  const limitEl = byId('limit');
  const platform = platformEl ? platformEl.value : '';
  const isAll = !platform;
  notice('sourceStatus', isAll ? 'Staging follows for all platforms sequentially...' : `Importing ${platform} sources...`, '');
  try {
    const data = await longAction('/api/stage-follows', {platform, limit: Number(limitEl.value || 100)}, {actionKey:'stage_follows', statusId:'sourceStatus', startedText: isAll ? 'Staging follows for all platforms sequentially...' : `Importing ${platform} sources...`});
    logLine(`${data.action_id ? data.action_id + ' · ' : ''}${data.message || 'sources imported'}`);
    notice('sourceStatus', data.message, data.ok === false ? '' : 'ok');
    await loadState({force:true});
  } catch (err) {
    logLine(`ERROR: ${err.message}`);
    if (isAll) {
      notice('sourceStatus', err.message, 'bad');
      alert(err.message);
      return;
    }
    const shouldRepair = isSessionRepairable(err)
      ? confirm(`${err.message}\n\nTry a session repair from your local browser, then import again?`)
      : false;
    if (shouldRepair) {
      try {
        notice('sourceStatus', `Repairing ${platformEl.value} session...`, '');
        await request('/api/session/capture', {method:'POST', body: JSON.stringify({platform: platformEl.value})});
        const retry = await longAction('/api/stage-follows', {platform: platformEl.value, limit: Number(limitEl.value || 100)}, {actionKey:'stage_follows', statusId:'sourceStatus', startedText: `Retrying ${platformEl.value} sources...`});
        notice('sourceStatus', retry.message, 'ok');
        await loadState({force:true});
      } catch (repairErr) {
        notice('sourceStatus', repairErr.message, 'bad');
        alert(repairErr.message);
      }
    } else {
      const suffix = isSessionRepairable(err) ? '' : '\n\nNo session repair suggested for this error. Check Debug logs for details.';
      notice('sourceStatus', `${err.message}${suffix}`, 'bad');
    }
  } finally {
    setBusy(false);
  }
}
async function readContent() {
  const platformEl = byId('platform');
  const platform = platformEl ? platformEl.value : '';
  notice('sourceStatus', `Collecting ${platformLabel(platform)} content, then embedding new items...`, '');
  try {
    const data = await longAction('/api/content/read', {platform}, {actionKey:'read_content', statusId:'sourceStatus', startedText:`Collecting ${platformLabel(platform)} content, then embedding new items...`});
    if (data) notice('sourceStatus', data.message, data.ok === false ? '' : 'ok');
  } catch (err) {
    logLine(`ERROR: ${err.message}`);
    notice('sourceStatus', err.message, 'bad');
  }
}
async function readContentAll() {
  notice('sourceStatus', 'Running all-platform content collection...', '');
  try {
    const data = await longAction('/api/content/read', {}, {actionKey:'read_content', statusId:'sourceStatus', startedText:'Running all-platform content collection...'});
    if (data) notice('sourceStatus', data.message, data.ok === false ? '' : 'ok');
  } catch (err) {
    logLine(`ERROR: ${err.message}`);
    notice('sourceStatus', err.message, 'bad');
  }
}
async function backfillEmbeddings() {
  try {
    const data = await longAction('/api/embeddings/backfill', {rescore: true, include_segments: true}, {actionKey:'embedding_backfill', statusId:'sourceStatus', startedText:'Backfilling embeddings...'});
    if (data) notice('sourceStatus', data.message, data.ok === false ? '' : 'ok');
  } catch (err) {
    logLine(`ERROR: ${err.message}`);
    notice('sourceStatus', err.message, 'bad');
  }
}
async function monitorSources() { return postAction('/api/staged-sources/monitor'); }
async function removeSource(id) { return postAction('/api/staged-sources/remove', {id}); }
async function getBrief(subjectId) {
  try {
    const data = await longAction('/api/briefs', {subject_id: subjectId}, {actionKey:'get_briefs', statusId:'useStatus', startedText:'Getting Briefs...'});
    if (data) notice('useStatus', data.message, 'ok');
    return data;
  } catch (err) {
    logLine(`ERROR: ${err.message}`);
    notice('useStatus', err.message, 'bad');
    return null;
  }
}
async function draftSubject(subjectId=null, text=null) {
  const subjectTextEl = byId('subjectText');
  const raw = text !== null ? text : (subjectId ? '' : (subjectTextEl ? subjectTextEl.value : ''));
  const normalized = String(raw || '').trim();
  const statusId = subjectId ? 'refineStatus' : 'subjectDraftStatus';
  const status = byId(statusId);
  if (!normalized) {
    const message = subjectId ? 'Add refinement text first.' : 'Describe the subject first.';
    if (status) { status.style.display = 'block'; status.className = 'notice bad'; status.textContent = message; }
    return null;
  }
  pauseRefresh(45000);
  if (status) { status.style.display = 'block'; status.className = 'notice'; status.textContent = subjectId ? 'Drafting refinement...' : 'Drafting subject...'; }
  const data = await postAction('/api/subjects/draft', {text: normalized, subject_id: subjectId}, {timeoutMs: 45000});
  if (data && data.data && data.data.draft) {
    renderDraft(data.data.draft, data.data.actionable);
    const currentStatus = byId(statusId);
    if (currentStatus) {
      currentStatus.style.display = 'block';
      currentStatus.className = data.data.actionable ? 'notice ok' : 'notice';
      currentStatus.textContent = data.message || 'Draft updated.';
    }
  } else if (status) {
    status.style.display = 'block';
    status.className = 'notice bad';
    status.textContent = 'Draft request did not finish. Check Debug logs, then try again.';
  }
  return data;
}
async function rebuildSubjectRules(subjectId) {
  const status = byId('refineStatus');
  pauseRefresh(120000);
  if (status) { status.style.display = 'block'; status.className = 'notice'; status.textContent = 'Rebuilding rules from the stored subject description...'; }
  const data = await postAction('/api/subjects/rebuild-rules', {subject_id: subjectId}, {timeoutMs: 120000});
  const currentStatus = byId('refineStatus');
  if (currentStatus && data) {
    currentStatus.style.display = 'block';
    currentStatus.className = data.ok === false ? 'notice bad' : 'notice ok';
    currentStatus.textContent = data.message || 'Rules rebuilt.';
  }
  return data;
}
async function rebuildAllSubjectRules() {
  pauseRefresh(1800000);
  try {
    const data = await longAction('/api/subjects/rebuild-all-rules', {}, {actionKey:'rebuild_all_subject_rules', statusId:'sourceStatus', startedText:'Rebuilding all subjects and warming embeddings...'});
    if (data) notice('sourceStatus', data.message, 'ok');
    return data;
  } catch (err) {
    logLine(`ERROR: ${err.message}`);
    notice('sourceStatus', err.message, 'bad');
    return null;
  }
}
async function confirmSubjectDraft(chatId=null) { return postAction('/api/subjects/confirm-draft', chatId === null ? {} : {chat_id: chatId}); }
async function cancelSubjectDraft(chatId=null) { const subjectTextEl = byId('subjectText'); if (chatId === null && subjectTextEl) subjectTextEl.value = ''; return postAction('/api/subjects/cancel-draft', chatId === null ? {} : {chat_id: chatId}); }
async function unlinkRoute(subjectId, chatId, threadId) { return postAction('/api/routes/unlink', {subject_id: subjectId, chat_id: chatId, thread_id: threadId || ''}); }
async function assignRoute(subjectId, chatId) { return postAction('/api/routes/assign', {subject_id: subjectId, chat_id: Number(chatId)}); }
function selectSubject(subjectId) {
  selectedSubjectId = subjectId;
  renderSubjects(lastState);
  renderSubjectDetail(lastState);
}
function renderDraft(draft, actionable) {
  const box = byId('draftBox');
  const save = byId('saveDraftButton');
  if (!draft) { box.style.display = 'none'; save.disabled = true || busy; return; }
  const include = (draft.include_terms || []).join(', ') || '(none yet)';
  const exclude = (draft.exclude_terms || []).join(', ') || '(none yet)';
  const hasRules = actionable !== undefined ? actionable : ((draft.include_terms || []).length || (draft.exclude_terms || []).length);
  box.style.display = 'block';
  box.className = hasRules ? 'notice ok' : 'notice';
  box.textContent = [`Proposed title: ${draft.title}`, `Include: ${include}`, `Avoid: ${exclude}`, draft.quality_notes ? `Quality notes: ${draft.quality_notes}` : '', hasRules ? 'Ready to save.' : 'Needs more detail before saving.'].filter(Boolean).join('\n');
  save.disabled = !hasRules || busy;
}
function renderSubjects(state) {
  const subjects = state.subjects || [];
  const prefs = state.subject_preferences || {};
  subjectsBox.innerHTML = subjects.length ? '' : '<div class="notice">No subjects yet. Describe your first subject below.</div>';
  subjectHint.textContent = subjects.length ? 'Each subject has its own preferences and its own Get Brief action.' : 'Describe your first subject in plain language. The Wizard will draft rules first; it will not save an empty-preference subject.';
  for (const subject of subjects) {
    const pref = prefs[String(subject.id)] || {};
    const include = (pref.include_terms || []).join(', ') || 'no include rules shown';
    const exclude = (pref.exclude_terms || []).join(', ') || 'no avoid rules shown';
    const div = document.createElement('div');
    div.className = `row clickable ${selectedSubjectId === subject.id ? 'selected' : ''}`;
    div.innerHTML = `<div><strong>${subject.name}</strong><small>Include: ${include}</small><small>Avoid: ${exclude}</small><small>Full text cap: ${subject.brief_full_text_chars || 1800} chars</small></div><div class="actions"><button>Get Brief</button></div>`;
    div.onclick = () => selectSubject(subject.id);
    div.querySelector('button').onclick = (event) => { event.stopPropagation(); getBrief(subject.id); };
    subjectsBox.appendChild(div);
  }
  if (!selectedSubjectId && subjects.length) selectedSubjectId = subjects[0].id;
  if (selectedSubjectId && !subjects.some(subject => subject.id === selectedSubjectId)) selectedSubjectId = subjects[0] ? subjects[0].id : null;
  renderSubjectDetail(state);
}
function renderPendingDrafts(state) {
  const drafts = state.subject_drafts || [];
  pendingDraftsBox.innerHTML = drafts.length ? '' : '<div class="notice ok">No pending subject drafts.</div>';
  for (const draft of drafts) {
    const include = (draft.include_terms || []).join(', ') || '(none yet)';
    const exclude = (draft.exclude_terms || []).join(', ') || '(none yet)';
    const origin = draft.chat_id === -1 ? 'Wizard' : `Telegram chat ${draft.chat_id}`;
    const div = document.createElement('div');
    div.className = 'row';
    div.innerHTML = `<div><strong>${draft.title}</strong><small>${origin} · ${draft.updated_at}</small><small>Include: ${include}</small><small>Avoid: ${exclude}</small></div><div class="actions"></div>`;
    const actions = div.querySelector('.actions');
    const save = document.createElement('button');
    save.textContent = 'Save';
    save.disabled = !draft.actionable;
    save.onclick = () => confirmSubjectDraft(draft.chat_id);
    const cancel = document.createElement('button');
    cancel.className = 'secondary';
    cancel.textContent = 'Cancel';
    cancel.onclick = () => cancelSubjectDraft(draft.chat_id);
    actions.appendChild(save);
    actions.appendChild(cancel);
    pendingDraftsBox.appendChild(div);
  }
}
function renderSubjectDetail(state) {
  if (!state || !selectedSubjectId) {
    subjectDetailBox.className = 'notice';
    subjectDetailBox.textContent = 'Select a subject to inspect preferences, refine it, or reassign its Telegram route.';
    return;
  }
  const subject = (state.subjects || []).find(item => item.id === selectedSubjectId);
  if (!subject) return;
  const pref = (state.subject_preferences || {})[String(subject.id)] || {};
  const routes = (state.routes || []).filter(route => route.subject_id === subject.id);
  const suspended = ((state.subject_source_overrides || {})[String(subject.id)] || []).filter(row => row.status !== 'active');
  const include = (pref.include_terms || []).join(', ') || '(none yet)';
  const exclude = (pref.exclude_terms || []).join(', ') || '(none yet)';
  const routeText = routes.length
    ? routes.map(route => `${route.chat_title || 'chat'} (${route.chat_id}${route.thread_id ? ', topic ' + route.thread_id : ''})`).join('\\n')
    : 'No route assigned yet.';
  const suspendedText = suspended.length
    ? suspended.map(row => `${row.platform}: ${row.display_name} (${row.status})`).join('\\n')
    : 'No per-subject suspended sources.';
  const chatOptions = (state.chats || []).map(chat => `<option value="${chat.chat_id}">${chat.title || 'chat ' + chat.chat_id}</option>`).join('');
  const existingRefine = document.getElementById('refineText');
  const refineValue = existingRefine ? existingRefine.value : '';
  const refineWasFocused = document.activeElement === existingRefine;
  subjectDetailBox.className = 'notice ok';
  subjectDetailBox.innerHTML = `
<strong>${subject.name}</strong>
<div class="fine">Preferences v${pref.version || 0} · updated ${pref.updated_at || 'unknown'}</div>
<div style="margin-top:10px"><strong>Include</strong><div class="fine">${include}</div></div>
<div style="margin-top:10px"><strong>Avoid</strong><div class="fine">${exclude}</div></div>
<div style="margin-top:10px"><strong>Route</strong><pre>${routeText}</pre></div>
<div style="margin-top:10px"><strong>Suspended sources</strong><pre>${suspendedText}</pre></div>
<label style="margin-top:10px">Refine in free form<textarea id="refineText" placeholder="Example: less hype, more primary sources, exclude generic Skills tutorials"></textarea></label>
<div class="actions" style="margin-top:8px"><button id="refineButton">Draft Refinement</button><button id="rebuildRulesButton" class="secondary">Rebuild Rules</button></div>
<div class="fine">Rebuild Rules re-runs the current extractor on the stored subject description and replaces the preference version. Use it to repair subjects created before the improved extraction prompt.</div>
<div id="refineStatus" class="notice" style="display:none; margin-top:8px"></div>
<label style="margin-top:10px">Route to Telegram chat<select id="routeChat">${chatOptions || '<option value="">No Telegram chats registered</option>'}</select></label>
<div class="actions" style="margin-top:8px"><button id="assignRouteButton" ${chatOptions ? '' : 'disabled'}>Assign Route</button></div>
  `.trim();
  const refineText = document.getElementById('refineText');
  refineText.value = refineValue;
  refineText.addEventListener('input', () => pauseRefresh());
  if (refineWasFocused) refineText.focus();
  byId('refineButton').onclick = () => draftSubject(subject.id, refineText.value);
  byId('rebuildRulesButton').onclick = () => rebuildSubjectRules(subject.id);
  byId('assignRouteButton').onclick = () => assignRoute(subject.id, byId('routeChat').value);
}
function renderSources(state) {
  if (!state) return;
  const platformValue = sourceFilter.value;
  const statusValue = statusFilter.value;
  const stagedRows = (state.staged_sources || []).map(r => ({...r, source_kind: 'staged', effective_status: r.status || 'pending'}));
  const monitoredRows = (state.monitored_sources || []).map(r => ({...r, source_kind: 'monitored', effective_status: r.follow_state || 'active'}));
  const rows = [...stagedRows, ...monitoredRows].filter(r => (!platformValue || r.platform === platformValue) && (!statusValue || r.effective_status === statusValue));
  sourcesBox.innerHTML = rows.length ? '' : '<div class="notice">No sources match this filter yet.</div>';
  for (const row of rows) {
    const div = document.createElement('div');
    div.className = 'row';
    const inactiveReason = row.metadata && row.metadata.inactive_reason ? ` · ${row.metadata.inactive_reason}` : '';
    const hint = row.effective_status === 'inactive'
      ? '<small>Channel not found. Re-run Get Sources to re-add it, or leave it inactive so collection skips it.</small>'
      : '';
    div.innerHTML = `<div><strong>${row.display_name}</strong><small>${row.platform} · ${row.source_kind} · ${row.effective_status}${inactiveReason}</small><small>${row.account_or_channel_id}</small>${hint}</div><div class="actions"></div>`;
    if (row.source_kind === 'staged' && row.status === 'pending') {
      const btn = document.createElement('button');
      btn.className = 'secondary';
      btn.textContent = 'Remove';
      btn.onclick = () => removeSource(row.id);
      div.querySelector('.actions').appendChild(btn);
    }
    sourcesBox.appendChild(div);
  }
  const counts = Object.entries(state.staged_counts || {}).sort().map(([k, v]) => `${k}: ${v}`).join('\n');
  sourceCounts.textContent = counts || 'No pending imports.';
}
function renderRoutes(rows=[]) {
  routesBox.innerHTML = rows.length ? '' : '<div class="notice">No Telegram routes yet.</div>';
  for (const row of rows) {
    const div = document.createElement('div');
    div.className = 'row';
    const title = row.chat_title || `chat ${row.chat_id}`;
    div.innerHTML = `<div><strong>${row.subject_name}</strong><small>${title} · ${row.chat_id}${row.thread_id ? ' · topic ' + row.thread_id : ''}</small></div><div class="actions"><button class="secondary">Unlink</button></div>`;
    div.querySelector('button').onclick = () => unlinkRoute(row.subject_id, row.chat_id, row.thread_id || '');
    routesBox.appendChild(div);
  }
}
function renderReauth(rows=[]) {
  if (!rows.length) { reauthBox.className = 'notice ok'; reauthBox.textContent = 'No sources currently need re-login.'; return; }
  reauthBox.className = 'notice bad';
  reauthBox.textContent = ['Sources needing re-login:', ...rows.map(r => `${r.platform}: ${r.display_name}`)].join('\n');
}
function fillPlatformSelects(platforms=[]) {
  const platformEl = byId('platform');
  const sourceFilterEl = byId('sourceFilter');
  if (platformEl.options.length === 0) {
    platformEl.add(new Option('All', ''));
    platforms.forEach(p => platformEl.add(new Option(p, p)));
  }
  if (sourceFilterEl.options.length === 1) platforms.forEach(p => sourceFilterEl.add(new Option(p, p)));
}
function platformLabel(value) {
  if (!value) return 'All';
  return value.split('_').map(part => part ? part[0].toUpperCase() + part.slice(1) : part).join(' ');
}
function actionRunning(state, key) {
  return (state.inflight_actions || []).find(action => action.key === key);
}
function updateActionControls(state) {
  const platformEl = byId('platform');
  const selectedPlatform = platformEl ? platformEl.value : '';
  const readButton = byId('readContentButton');
  const getSourcesButton = byId('getSourcesButton');
  const runAllButton = byId('runAllContentButton');
  const backfillButton = byId('backfillEmbeddingsButton');
  const rebuildAllRulesButton = byId('rebuildAllRulesButton');
  const readRunning = actionRunning(state, 'read_content');
  const stageRunning = actionRunning(state, 'stage_follows');
  const backfillRunning = actionRunning(state, 'embedding_backfill');
  const rebuildAllRulesRunning = actionRunning(state, 'rebuild_all_subject_rules');
  const briefsRunning = actionRunning(state, 'get_briefs');
  if (briefsRunning) notice('useStatus', `Running: ${briefsRunning.label} (started ${String(briefsRunning.started_at || '').slice(11, 16) || 'now'}).`, '');
  else notice('useStatus', '', '');
  if (readButton) {
    readButton.textContent = readRunning ? `Running: ${readRunning.label}` : `Get Content (${platformLabel(selectedPlatform)})`;
    readButton.disabled = busy || Boolean(readRunning);
  }
  if (runAllButton) runAllButton.disabled = busy || Boolean(readRunning);
  if (backfillButton) {
    backfillButton.textContent = backfillRunning ? `Running: ${backfillRunning.label}` : 'Backfill Embeddings';
    backfillButton.disabled = busy || Boolean(backfillRunning);
  }
  if (rebuildAllRulesButton) {
    rebuildAllRulesButton.textContent = rebuildAllRulesRunning ? `Running: ${rebuildAllRulesRunning.label}` : 'Rebuild All Subjects';
    rebuildAllRulesButton.disabled = busy || Boolean(rebuildAllRulesRunning);
  }
  if (getSourcesButton) {
    getSourcesButton.textContent = stageRunning ? `Running: ${stageRunning.label}` : `Get Sources (${platformLabel(selectedPlatform)})`;
    getSourcesButton.disabled = busy || Boolean(stageRunning);
  }
  const sourceStatusEl = byId('sourceStatus');
  if ((readRunning || stageRunning || backfillRunning || rebuildAllRulesRunning) && sourceStatusEl) {
    const running = readRunning || stageRunning || backfillRunning || rebuildAllRulesRunning;
    notice('sourceStatus', `Running: ${running.label} (started ${String(running.started_at || '').slice(11, 16) || 'now'}).`, '');
  }
}
function defaultTab(state) {
  const hasToken = state.settings && state.settings.telegram_token_configured;
  if (!hasToken) return 'config';
  return 'use';
}
async function loadState(options={}) {
  if (!options.force && (busy || Date.now() < refreshPausedUntil)) return;
  const active = document.activeElement;
  if (!options.force && active && ['TEXTAREA', 'INPUT', 'SELECT'].includes(active.tagName)) {
    pauseRefresh();
    return;
  }
  const snapshot = formSnapshot();
  try {
    const data = await request('/api/state');
    lastState = data;
    const s = data.settings || {};
    byId('timezone').value = s.timezone || 'UTC';
    byId('digestTime').value = s.digest_time || '08:30';
    fillPlatformSelects(data.platforms || []);
    updateActionControls(data);
    renderSubjects(data);
    renderDraft(data.subject_draft, data.subject_draft_actionable);
    renderPendingDrafts(data);
    renderSources(data);
    renderRoutes(data.routes || []);
    renderReauth(data.reauth_sources || []);
    agentBadge.textContent = `agent: ${data.agent_running ? 'running' : 'stopped'}`;
    tokenWarning.style.display = s.telegram_token_missing ? 'block' : 'none';
    tokenWarning.textContent = s.telegram_status || '';
    compactState.textContent = JSON.stringify({agent: data.agent_running ? 'running' : 'stopped', token: s.telegram_token_configured ? 'configured' : 'missing', subjects: (data.subjects || []).map(x => x.name), routes: (data.routes || []).length}, null, 2);
    stateBox.textContent = JSON.stringify(data, null, 2);
    pathsBox.textContent = JSON.stringify({data_dir: s.data_dir, db_path: s.db_path, log_file: s.log_file, debug_dir: s.debug_dir, browser_channel: s.browser_channel}, null, 2);
    debugFiles.textContent = JSON.stringify({log_file: s.log_file, debug_dir: s.debug_dir, recent_run_logs: data.recent_run_logs || []}, null, 2);
    logsBox.textContent = (data.logs || []).slice().reverse().join('\n');
    const failures = [];
    if (s.telegram_token_missing) failures.push(s.telegram_status);
    if ((data.reauth_sources || []).length) failures.push(`${data.reauth_sources.length} source(s) need re-login.`);
    const inactiveSources = (data.monitored_sources || []).filter(source => source.follow_state === 'inactive');
    if (inactiveSources.length) failures.push(`${inactiveSources.length} source(s) are inactive, usually because a public feed/channel was not found. Check Sources.`);
    if ((data.circuit_broken || []).length) {
      const reasons = data.circuit_broken_reasons_by_platform || {};
      const paused = (data.circuit_broken || []).map(platform => `${platform} (${reasons[platform] || 'bot_shaped'})`).join(', ');
      failures.push(`Collection paused for: ${paused}. Check debug bundle snapshots before retrying.`);
    }
    if (data.embedding_degraded && data.embedding_degraded.degraded) {
      const names = (data.embedding_degraded.subjects || []).map(s => s.subject_name || s.subject_id).filter(Boolean).join(', ');
      failures.push(`Embedding scoring degraded${names ? ` for: ${names}` : ''}. Check Ollama and run Backfill Embeddings in Debug.`);
    }
    if (data.embedding_not_warmed && data.embedding_not_warmed.not_warmed) {
      const names = (data.embedding_not_warmed.subjects || []).map(s => s.subject_name || s.subject_id).filter(Boolean).join(', ');
      failures.push(`Embeddings not yet warmed${names ? ` for: ${names}` : ''}. Run Backfill Embeddings or Rebuild All Subjects in Debug to enable embedding scoring.`);
    }
    failureBox.className = failures.length ? 'notice bad' : 'notice ok';
    failureBox.textContent = failures.join('\n') || 'No visible failures.';
    if (!document.body.dataset.initialTab) { showTab(defaultTab(data)); document.body.dataset.initialTab = '1'; }
    restoreFormSnapshot(snapshot);
  } catch (err) { logLine(`ERROR: ${err.message}`); }
}
async function refreshRunningState() {
  try {
    const data = await request('/api/state');
    lastState = data;
    updateActionControls(data);
    logsBox.textContent = (data.logs || []).slice().reverse().join('\n');
    return data;
  } catch (err) {
    logLine(`ERROR: ${err.message}`);
    return null;
  }
}
document.addEventListener('input', event => {
  if (event.target && ['TEXTAREA', 'INPUT', 'SELECT'].includes(event.target.tagName)) pauseRefresh();
});
loadState({force:true});
setInterval(() => busy ? refreshRunningState() : loadState(), 5000);
</script>
</body>
</html>
"""


@dataclass
class DesktopWebServer:
    settings: Settings
    token: str = field(default_factory=lambda: secrets.token_urlsafe(32))
    host: str = "127.0.0.1"
    port: int = field(default_factory=lambda: _find_free_port())
    service: DesktopCommandService | None = None
    _server: Any = field(default=None, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.service is None:
            self.service = DesktopCommandService(settings_factory=lambda: self.settings)

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/?token={self.token}"

    def create_app(self):
        try:
            from starlette.applications import Starlette
            from starlette.middleware.base import BaseHTTPMiddleware
            from starlette.responses import HTMLResponse, JSONResponse
            from starlette.routing import Route
        except Exception as exc:  # pragma: no cover - depends on optional GUI deps
            raise RuntimeError(
                "Desktop web dependencies are missing. Run: pip install -e ."
            ) from exc

        async def require_auth(request, call_next):
            path = request.url.path
            if path == "/" and request.query_params.get("token") == self.token:
                return await call_next(request)
            auth = request.headers.get("authorization", "")
            if auth == f"Bearer {self.token}":
                return await call_next(request)
            if path == "/api/events" and request.query_params.get("token") == self.token:
                return await call_next(request)
            return JSONResponse({"ok": False, "message": "Unauthorized desktop session."}, status_code=401)

        async def read_json(request) -> dict[str, Any]:
            if request.method == "GET":
                return {}
            try:
                return await request.json()
            except Exception:
                return {}

        async def run_result(
            handler: Callable[..., Awaitable[Any]],
            request,
            *,
            pass_action_id: bool = False,
        ):
            started_at = time.monotonic()
            action_id = new_action_id(request.url.path.strip("/").replace("/", "_") or "index")
            try:
                payload = await read_json(request)
                logger.info(
                    "Desktop action started action_id=%s path=%s payload=%s",
                    action_id,
                    request.url.path,
                    summarize_payload(payload),
                )
                result = await handler(payload, action_id) if pass_action_id else await handler(payload)
                if hasattr(result, "to_dict"):
                    body = result.to_dict()
                else:
                    body = result
                if isinstance(body, dict):
                    body = {**body, "action_id": action_id}
                    ok = body.get("ok", True)
                else:
                    ok = True
                logger.info(
                    "Desktop action finished action_id=%s path=%s ok=%s duration_ms=%d",
                    action_id,
                    request.url.path,
                    ok,
                    monotonic_ms_since(started_at),
                )
                status_code = 202 if isinstance(body, dict) and body.get("data", {}).get("pending") else 200
                return JSONResponse(body, status_code=status_code)
            except ValueError as exc:
                logger.warning(
                    "Desktop request rejected action_id=%s path=%s message=%s duration_ms=%d",
                    action_id,
                    request.url.path,
                    str(exc),
                    monotonic_ms_since(started_at),
                )
                return JSONResponse({"ok": False, "message": str(exc), "action_id": action_id}, status_code=400)
            except Exception as exc:
                logger.exception(
                    "Desktop request failed action_id=%s path=%s duration_ms=%d",
                    action_id,
                    request.url.path,
                    monotonic_ms_since(started_at),
                )
                return JSONResponse({"ok": False, "message": str(exc), "action_id": action_id}, status_code=500)

        async def index(_request):
            return HTMLResponse(INDEX_HTML)

        async def state(_request):
            assert self.service is not None
            return JSONResponse(await self.service.get_state())

        async def action_result(request):
            assert self.service is not None
            action_id = str(request.path_params.get("action_id") or "")
            status_code, result = await self.service.get_action_result(
                action_id=action_id
            )
            body = result.to_dict()
            body["action_id"] = action_id
            return JSONResponse(body, status_code=status_code)

        async def wizard_event(request):
            assert self.service is not None
            body = await request.body()
            if len(body) > 1024:
                return JSONResponse({"ok": False, "message": "Wizard event is too large."}, status_code=413)
            try:
                payload = json.loads(body.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                return JSONResponse({"ok": False, "message": "Invalid wizard event JSON."}, status_code=400)
            try:
                result = await self.service.record_wizard_event(payload if isinstance(payload, dict) else {})
            except ValueError as exc:
                return JSONResponse({"ok": False, "message": str(exc)}, status_code=400)
            return JSONResponse(result.to_dict())

        async def settings(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.save_runtime_settings(
                    token=str(p.get("token") or ""),
                    timezone=str(p.get("timezone") or "UTC"),
                    digest_time=str(p.get("digest_time") or "08:30"),
                ),
                request,
            )

        async def init_db(request):
            assert self.service is not None
            return await run_result(lambda _p: self.service.init_db(), request)

        async def start_agent(request):
            assert self.service is not None
            return await run_result(lambda _p: self.service.start_agent(), request)

        async def stop_agent(request):
            assert self.service is not None
            return await run_result(lambda _p: self.service.stop_agent(), request)

        async def login(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.open_login_window(platform=str(p.get("platform") or "")),
                request,
            )

        async def capture_session(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.capture_session(
                    platform=str(p.get("platform") or ""),
                    browser=str(p.get("browser") or "") or None,
                ),
                request,
            )

        async def stage_follows(request):
            assert self.service is not None
            return await run_result(
                lambda p, action_id: self.service.stage_follows(
                    platform=str(p.get("platform") or ""),
                    limit=int(p.get("limit") or 100),
                    async_response=True,
                    action_id=action_id,
                ),
                request,
                pass_action_id=True,
            )

        async def staged_sources(request):
            assert self.service is not None
            return await run_result(lambda _p: self.service.list_staged_sources(), request)

        async def draft_subject(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.draft_subject(
                    text=str(p.get("text") or ""),
                    subject_id=int(p.get("subject_id") or 0) if p.get("subject_id") else None,
                ),
                request,
            )

        async def rebuild_subject_rules(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.rebuild_subject_rules(
                    subject_id=int(p.get("subject_id") or 0),
                    text=str(p.get("text") or "") or None,
                ),
                request,
            )

        async def rebuild_all_subject_rules(request):
            assert self.service is not None
            return await run_result(
                lambda _p, action_id: self.service.rebuild_all_subject_rules(
                    async_response=True,
                    action_id=action_id,
                ),
                request,
                pass_action_id=True,
            )

        async def confirm_subject_draft(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.confirm_subject_draft(
                    chat_id=int(p.get("chat_id") or 0) if p.get("chat_id") is not None else None,
                ),
                request,
            )

        async def cancel_subject_draft(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.cancel_subject_draft(
                    chat_id=int(p.get("chat_id") or 0) if p.get("chat_id") is not None else None,
                ),
                request,
            )

        async def remove_staged_source(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.remove_staged_source(source_id=int(p.get("id") or 0)),
                request,
            )

        async def monitor_staged_sources(request):
            assert self.service is not None
            return await run_result(lambda _p: self.service.monitor_staged_sources(), request)

        async def confirm_staged_sources(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.confirm_staged_sources(
                    subject=str(p.get("subject") or ""),
                    include_terms=list(p.get("include_terms") or []),
                    exclude_terms=list(p.get("exclude_terms") or []),
                    high_quality_examples=p.get("high_quality_examples"),
                ),
                request,
            )

        async def smoke(request):
            assert self.service is not None
            return await run_result(lambda _p: self.service.run_smoke_crawl_and_digest(), request)

        async def read_content(request):
            assert self.service is not None
            return await run_result(
                lambda p, action_id: self.service.read_content(
                    platform=str(p.get("platform") or "") or None,
                    async_response=True,
                    action_id=action_id,
                ),
                request,
                pass_action_id=True,
            )

        async def backfill_embeddings(request):
            assert self.service is not None
            return await run_result(
                lambda p, action_id: self.service.backfill_embeddings(
                    concurrency=int(p["concurrency"]) if p.get("concurrency") else None,
                    limit=int(p.get("limit")) if p.get("limit") else None,
                    rescore=bool(p.get("rescore", True)),
                    include_segments=bool(p.get("include_segments", True)),
                    async_response=True,
                    action_id=action_id,
                ),
                request,
                pass_action_id=True,
            )

        async def briefs(request):
            assert self.service is not None
            return await run_result(
                lambda p, action_id: self.service.get_briefs(
                    subject_id=int(p.get("subject_id") or 0) if p.get("subject_id") else None,
                    async_response=True,
                    action_id=action_id,
                ),
                request,
                pass_action_id=True,
            )

        async def rebuild_digest(request):
            assert self.service is not None
            return await run_result(lambda _p: self.service.rebuild_todays_digest(), request)

        async def unlink_route(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.unlink_subject_route(
                    subject_id=int(p.get("subject_id") or 0),
                    chat_id=int(p.get("chat_id") or 0),
                    thread_id=str(p.get("thread_id") or "") or None,
                ),
                request,
            )

        async def move_route(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.move_subject_route(
                    subject_id=int(p.get("subject_id") or 0),
                    from_chat_id=int(p.get("from_chat_id") or 0),
                    from_thread_id=str(p.get("from_thread_id") or "") or None,
                    to_chat_id=int(p.get("to_chat_id") or 0),
                ),
                request,
            )

        async def assign_route(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.reassign_subject_route(
                    subject_id=int(p.get("subject_id") or 0),
                    chat_id=int(p.get("chat_id") or 0),
                ),
                request,
            )

        async def shutdown_service() -> None:
            if self.service is not None:
                await self.service.shutdown()

        @asynccontextmanager
        async def lifespan(_app):
            if self.service is not None:
                result = await self.service.startup_for_wizard()
                if not result.ok:
                    logger.warning("Wizard startup completed with warning: %s", result.message)
            yield
            await shutdown_service()

        routes = [
            Route("/", index, methods=["GET"]),
            Route("/api/state", state, methods=["GET"]),
            Route("/api/actions/{action_id}/result", action_result, methods=["GET"]),
            Route("/api/debug/wizard-events", wizard_event, methods=["POST"]),
            Route("/api/settings", settings, methods=["POST"]),
            Route("/api/init-db", init_db, methods=["POST"]),
            Route("/api/agent/start", start_agent, methods=["POST"]),
            Route("/api/agent/stop", stop_agent, methods=["POST"]),
            Route("/api/login", login, methods=["POST"]),
            Route("/api/session/capture", capture_session, methods=["POST"]),
            Route("/api/stage-follows", stage_follows, methods=["POST"]),
            Route("/api/staged-sources", staged_sources, methods=["GET"]),
            Route("/api/subjects/draft", draft_subject, methods=["POST"]),
            Route("/api/subjects/rebuild-rules", rebuild_subject_rules, methods=["POST"]),
            Route("/api/subjects/rebuild-all-rules", rebuild_all_subject_rules, methods=["POST"]),
            Route("/api/subjects/confirm-draft", confirm_subject_draft, methods=["POST"]),
            Route("/api/subjects/cancel-draft", cancel_subject_draft, methods=["POST"]),
            Route("/api/staged-sources/remove", remove_staged_source, methods=["POST"]),
            Route("/api/staged-sources/monitor", monitor_staged_sources, methods=["POST"]),
            Route("/api/confirm-staged-sources", confirm_staged_sources, methods=["POST"]),
            Route("/api/smoke", smoke, methods=["POST"]),
            Route("/api/content/read", read_content, methods=["POST"]),
            Route("/api/embeddings/backfill", backfill_embeddings, methods=["POST"]),
            Route("/api/briefs", briefs, methods=["POST"]),
            Route("/api/digest/rebuild", rebuild_digest, methods=["POST"]),
            Route("/api/routes/unlink", unlink_route, methods=["POST"]),
            Route("/api/routes/move", move_route, methods=["POST"]),
            Route("/api/routes/assign", assign_route, methods=["POST"]),
        ]
        app = Starlette(routes=routes, lifespan=lifespan)
        app.add_middleware(BaseHTTPMiddleware, dispatch=require_auth)
        return app

    def start(self) -> None:
        try:
            import uvicorn
        except Exception as exc:  # pragma: no cover - depends on optional GUI deps
            raise RuntimeError("Desktop server dependency missing. Run: pip install -e .") from exc
        config = uvicorn.Config(
            self.create_app(),
            host=self.host,
            port=self.port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        deadline = time.time() + 8
        while time.time() < deadline:
            if getattr(self._server, "started", False):
                return
            time.sleep(0.05)
        raise RuntimeError("Desktop server did not start in time.")

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=5)


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
