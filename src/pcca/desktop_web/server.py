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
      --bg: #08110f;
      --panel: rgba(241, 247, 239, .08);
      --panel-strong: rgba(241, 247, 239, .13);
      --ink: #eef7ef;
      --muted: #9ab0a3;
      --line: rgba(238, 247, 239, .16);
      --accent: #73f2b5;
      --accent-2: #ffb86c;
      --bad: #ff7777;
      --ok: #73f2b5;
      --shadow: 0 28px 90px rgba(0, 0, 0, .36);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      font-family: Avenir Next, Trebuchet MS, Verdana, sans-serif;
      background:
        radial-gradient(circle at 12% 8%, rgba(115, 242, 181, .18), transparent 28rem),
        radial-gradient(circle at 84% 18%, rgba(255, 184, 108, .15), transparent 24rem),
        linear-gradient(135deg, #07100e 0%, #10211d 54%, #07100e 100%);
    }
    body::before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      opacity: .28;
      background-image: linear-gradient(rgba(238,247,239,.06) 1px, transparent 1px), linear-gradient(90deg, rgba(238,247,239,.05) 1px, transparent 1px);
      background-size: 42px 42px;
      mask-image: radial-gradient(circle at 50% 20%, black, transparent 74%);
    }
    .shell { max-width: 1160px; margin: 0 auto; padding: 24px; position: relative; }
    header { display: flex; justify-content: space-between; align-items: flex-start; gap: 20px; margin-bottom: 18px; }
    .brand h1 { margin: 0; font-size: clamp(30px, 5vw, 58px); line-height: .88; letter-spacing: -.055em; }
    .brand p { max-width: 640px; margin: 12px 0 0; color: var(--muted); line-height: 1.45; }
    .pill { border: 1px solid var(--line); background: rgba(8,17,15,.52); border-radius: 999px; padding: 9px 12px; font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; color: var(--muted); }
    .tabs { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin: 16px 0; padding: 6px; border: 1px solid var(--line); background: rgba(8,17,15,.42); border-radius: 22px; box-shadow: var(--shadow); }
    .tab { box-shadow: none; background: transparent; color: var(--muted); border-radius: 16px; padding: 12px; }
    .tab.active { background: var(--accent); color: #07100e; }
    .view { display: none; }
    .view.active { display: block; animation: rise .24s ease-out; }
    @keyframes rise { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
    .grid { display: grid; grid-template-columns: minmax(0, 1.25fr) minmax(280px, .75fr); gap: 14px; align-items: start; }
    .stack { display: grid; gap: 14px; }
    .card { border: 1px solid var(--line); background: var(--panel); border-radius: 24px; padding: 18px; box-shadow: var(--shadow); backdrop-filter: blur(18px); }
    .card.tight { padding: 14px; }
    h2, h3 { margin: 0 0 8px; letter-spacing: -.025em; }
    h2 { font-size: 24px; }
    h3 { font-size: 17px; }
    p { color: var(--muted); line-height: 1.45; margin: 0 0 12px; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }
    input, select, textarea { width: 100%; border: 1px solid var(--line); border-radius: 15px; padding: 12px 13px; background: rgba(238,247,239,.08); color: var(--ink); font: 14px Avenir Next, Trebuchet MS, Verdana, sans-serif; outline: none; }
    textarea { min-height: 118px; resize: vertical; line-height: 1.45; }
    input:focus, select:focus, textarea:focus { border-color: rgba(115,242,181,.72); box-shadow: 0 0 0 3px rgba(115,242,181,.12); }
    button { border: 0; border-radius: 15px; padding: 11px 13px; color: #07100e; background: var(--accent); cursor: pointer; font: 700 13px Avenir Next, Trebuchet MS, Verdana, sans-serif; box-shadow: 0 12px 28px rgba(115,242,181,.16); }
    button.secondary { background: rgba(238,247,239,.13); color: var(--ink); border: 1px solid var(--line); box-shadow: none; }
    button.warn { background: var(--accent-2); color: #1d1208; }
    button:disabled { opacity: .42; cursor: wait; }
    .actions { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .split { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
    .row { display: grid; grid-template-columns: 1fr auto; gap: 10px; align-items: center; padding: 12px; border: 1px solid var(--line); border-radius: 18px; background: rgba(238,247,239,.06); }
    .row.clickable { cursor: pointer; }
    .row.selected { border-color: rgba(115,242,181,.7); background: rgba(115,242,181,.1); }
    .row strong { display: block; font-size: 14px; }
    .row small, .fine { display: block; color: var(--muted); font-size: 12px; margin-top: 3px; overflow-wrap: anywhere; }
    .notice { border: 1px solid rgba(255,184,108,.34); background: rgba(255,184,108,.09); color: #ffd9ad; border-radius: 18px; padding: 12px; white-space: pre-wrap; }
    .notice.bad { border-color: rgba(255,119,119,.38); background: rgba(255,119,119,.1); color: #ffc4c4; }
    .notice.ok { border-color: rgba(115,242,181,.34); background: rgba(115,242,181,.09); color: #c8ffe0; }
    .list { display: grid; gap: 8px; }
    .logs, pre { max-height: 360px; overflow: auto; white-space: pre-wrap; font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; color: #c8d8ce; }
    .source-toolbar { display: grid; grid-template-columns: 1fr 1fr auto; gap: 10px; align-items: end; }
    @media (max-width: 860px) { .grid, .split, .source-toolbar { grid-template-columns: 1fr; } header { display: block; } .tabs { grid-template-columns: repeat(2, 1fr); } }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div class="brand">
        <h1>Cut through<br/>the feed fog.</h1>
        <p>A local-first curation agent: import followed sources, collect fresh content, and send Telegram Briefs shaped by your subject preferences.</p>
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
              <button onclick="postAction('/api/agent/start')">Start</button>
              <button class="secondary" onclick="postAction('/api/agent/stop')">Stop</button>
            </div>
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
              <button onclick="draftSubject()">Draft Subject</button>
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
            <p>Choose a platform and import follows/subscriptions from the logged-in browser session. If the session is missing, the Wizard will ask to run a repair flow inline.</p>
            <div class="source-toolbar">
              <label>Platform <select id="platform"></select></label>
              <label>Limit <input id="limit" type="number" min="1" max="500" value="100" /></label>
              <button onclick="getSources()">Get Sources</button>
            </div>
            <div class="actions" style="margin-top:10px"><button class="secondary" onclick="readContent()">Get Content</button></div>
            <div id="sourceStatus" class="notice" style="display:none; margin-top:12px"></div>
          </div>
          <div class="card">
            <h2>Source List</h2>
            <div class="split">
              <label>Filter platform <select id="sourceFilter" onchange="renderSources(lastState)"><option value="">All</option></select></label>
              <label>Status <select id="statusFilter" onchange="renderSources(lastState)"><option value="">All</option><option value="pending">Pending</option><option value="confirmed">Confirmed</option><option value="removed">Removed</option></select></label>
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
function setBusy(next) { busy = next; document.querySelectorAll('button').forEach(b => b.disabled = next && !b.classList.contains('tab')); if (lastState) renderDraft(lastState.subject_draft, lastState.subject_draft_actionable); }
function showTab(name) { document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === name)); document.querySelectorAll('.view').forEach(v => v.classList.toggle('active', v.id === `view-${name}`)); }
function notice(id, text, kind='') { const el = document.getElementById(id); el.style.display = text ? 'block' : 'none'; el.className = `notice ${kind}`.trim(); el.textContent = text || ''; }
function logLine(line) { const el = document.getElementById('logsBox'); el.textContent = `${new Date().toLocaleTimeString()} ${line}\n` + el.textContent; }
async function request(path, opts={}) {
  const timeoutMs = opts.timeoutMs || 120000;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const cleanOpts = {...opts};
    delete cleanOpts.timeoutMs;
    const res = await fetch(path, {...cleanOpts, signal: controller.signal, headers: {...headers, ...(opts.headers || {})}});
    const data = await res.json();
    if (!res.ok || data.ok === false) throw new Error(data.message || data.detail || `Request failed: ${path}`);
    return data;
  } catch (err) {
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
async function saveSettings() {
  const data = await postAction('/api/settings', {token: byId('telegramToken').value, timezone: byId('timezone').value, digest_time: byId('digestTime').value});
  if (data) notice('configStatus', data.message, 'ok');
}
async function getSources() {
  const platformEl = byId('platform');
  const limitEl = byId('limit');
  notice('sourceStatus', `Importing ${platformEl.value} sources...`, '');
  try {
    setBusy(true);
    const data = await request('/api/stage-follows', {method:'POST', timeoutMs: 120000, body: JSON.stringify({platform: platformEl.value, limit: Number(limitEl.value || 100)})});
    logLine(`${data.action_id ? data.action_id + ' · ' : ''}${data.message || 'sources imported'}`);
    notice('sourceStatus', data.message, 'ok');
    await loadState({force:true});
  } catch (err) {
    logLine(`ERROR: ${err.message}`);
    const shouldRepair = confirm(`${err.message}\n\nTry a session repair from your local browser, then import again?`);
    if (shouldRepair) {
      try {
        notice('sourceStatus', `Repairing ${platformEl.value} session...`, '');
        await request('/api/session/capture', {method:'POST', body: JSON.stringify({platform: platformEl.value})});
        const retry = await request('/api/stage-follows', {method:'POST', timeoutMs: 120000, body: JSON.stringify({platform: platformEl.value, limit: Number(limitEl.value || 100)})});
        notice('sourceStatus', retry.message, 'ok');
        await loadState({force:true});
      } catch (repairErr) {
        notice('sourceStatus', repairErr.message, 'bad');
        alert(repairErr.message);
      }
    } else {
      notice('sourceStatus', err.message, 'bad');
    }
  } finally {
    setBusy(false);
  }
}
async function readContent() { const data = await postAction('/api/content/read'); if (data) notice('sourceStatus', data.message, 'ok'); }
async function monitorSources() { return postAction('/api/staged-sources/monitor'); }
async function removeSource(id) { return postAction('/api/staged-sources/remove', {id}); }
async function getBrief(subjectId) { return postAction('/api/briefs', {subject_id: subjectId}); }
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
<div class="actions" style="margin-top:8px"><button id="refineButton">Draft Refinement</button></div>
<div id="refineStatus" class="notice" style="display:none; margin-top:8px"></div>
<label style="margin-top:10px">Route to Telegram chat<select id="routeChat">${chatOptions || '<option value="">No Telegram chats registered</option>'}</select></label>
<div class="actions" style="margin-top:8px"><button id="assignRouteButton" ${chatOptions ? '' : 'disabled'}>Assign Route</button></div>
  `.trim();
  const refineText = document.getElementById('refineText');
  refineText.value = refineValue;
  refineText.addEventListener('input', () => pauseRefresh());
  if (refineWasFocused) refineText.focus();
  byId('refineButton').onclick = () => draftSubject(subject.id, refineText.value);
  byId('assignRouteButton').onclick = () => assignRoute(subject.id, byId('routeChat').value);
}
function renderSources(state) {
  if (!state) return;
  const platformValue = sourceFilter.value;
  const statusValue = statusFilter.value;
  const rows = (state.staged_sources || []).filter(r => (!platformValue || r.platform === platformValue) && (!statusValue || r.status === statusValue));
  sourcesBox.innerHTML = rows.length ? '' : '<div class="notice">No sources match this filter yet.</div>';
  for (const row of rows) {
    const div = document.createElement('div');
    div.className = 'row';
    div.innerHTML = `<div><strong>${row.display_name}</strong><small>${row.platform} · ${row.status}</small><small>${row.account_or_channel_id}</small></div><div class="actions"></div>`;
    if (row.status === 'pending') {
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
  if (platformEl.options.length === 0) platforms.forEach(p => platformEl.add(new Option(p, p)));
  if (sourceFilterEl.options.length === 1) platforms.forEach(p => sourceFilterEl.add(new Option(p, p)));
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
    if ((data.circuit_broken || []).length) failures.push(`Collection paused for: ${(data.circuit_broken || []).join(', ')}. Check debug bundle snapshots before retrying.`);
    failureBox.className = failures.length ? 'notice bad' : 'notice ok';
    failureBox.textContent = failures.join('\n') || 'No visible failures.';
    if (!document.body.dataset.initialTab) { showTab(defaultTab(data)); document.body.dataset.initialTab = '1'; }
    restoreFormSnapshot(snapshot);
  } catch (err) { logLine(`ERROR: ${err.message}`); }
}
document.addEventListener('input', event => {
  if (event.target && ['TEXTAREA', 'INPUT', 'SELECT'].includes(event.target.tagName)) pauseRefresh();
});
loadState({force:true});
setInterval(() => loadState(), 5000);
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

        async def run_result(handler: Callable[[dict[str, Any]], Awaitable[Any]], request):
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
                result = await handler(payload)
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
                return JSONResponse(body)
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
                lambda p: self.service.stage_follows(
                    platform=str(p.get("platform") or ""),
                    limit=int(p.get("limit") or 100),
                ),
                request,
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
            return await run_result(lambda _p: self.service.read_content(), request)

        async def briefs(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.get_briefs(
                    subject_id=int(p.get("subject_id") or 0) if p.get("subject_id") else None
                ),
                request,
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
            Route("/api/settings", settings, methods=["POST"]),
            Route("/api/init-db", init_db, methods=["POST"]),
            Route("/api/agent/start", start_agent, methods=["POST"]),
            Route("/api/agent/stop", stop_agent, methods=["POST"]),
            Route("/api/login", login, methods=["POST"]),
            Route("/api/session/capture", capture_session, methods=["POST"]),
            Route("/api/stage-follows", stage_follows, methods=["POST"]),
            Route("/api/staged-sources", staged_sources, methods=["GET"]),
            Route("/api/subjects/draft", draft_subject, methods=["POST"]),
            Route("/api/subjects/confirm-draft", confirm_subject_draft, methods=["POST"]),
            Route("/api/subjects/cancel-draft", cancel_subject_draft, methods=["POST"]),
            Route("/api/staged-sources/remove", remove_staged_source, methods=["POST"]),
            Route("/api/staged-sources/monitor", monitor_staged_sources, methods=["POST"]),
            Route("/api/confirm-staged-sources", confirm_staged_sources, methods=["POST"]),
            Route("/api/smoke", smoke, methods=["POST"]),
            Route("/api/content/read", read_content, methods=["POST"]),
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
