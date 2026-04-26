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
from pcca.services.desktop_command_service import DesktopCommandService

logger = logging.getLogger(__name__)

INDEX_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>PCCA Onboarding</title>
  <style>
    :root {
      --ink: #17201a;
      --muted: #637069;
      --paper: #fffaf0;
      --panel: rgba(255,255,255,.76);
      --line: rgba(23,32,26,.14);
      --accent: #1f7a5a;
      --accent-2: #d9743f;
      --bad: #b3261e;
      --ok: #176b45;
      --shadow: 0 24px 80px rgba(35, 45, 38, .18);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: ui-serif, Georgia, Cambria, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 12% 18%, rgba(217,116,63,.22), transparent 28rem),
        radial-gradient(circle at 82% 8%, rgba(31,122,90,.20), transparent 24rem),
        linear-gradient(135deg, #f4ead7, #eef4df 52%, #e4efe9);
    }
    .shell { max-width: 1180px; margin: 0 auto; padding: 36px 28px 28px; }
    header { display: flex; justify-content: space-between; gap: 24px; align-items: flex-start; margin-bottom: 26px; }
    h1 { font-size: clamp(36px, 6vw, 72px); line-height: .9; margin: 0; letter-spacing: -.055em; }
    h2 { font-size: 24px; margin: 0 0 6px; letter-spacing: -.02em; }
    p { color: var(--muted); line-height: 1.45; }
    .badge { display: inline-flex; gap: 8px; align-items: center; border: 1px solid var(--line); border-radius: 999px; padding: 8px 12px; background: rgba(255,255,255,.55); font: 13px ui-monospace, SFMono-Regular, Menlo, monospace; }
    .grid { display: grid; grid-template-columns: minmax(0, 1fr) 340px; gap: 18px; align-items: start; }
    .steps { display: grid; gap: 14px; }
    section { background: var(--panel); border: 1px solid var(--line); border-radius: 28px; padding: 20px; box-shadow: var(--shadow); backdrop-filter: blur(18px); }
    section.done { opacity: .72; box-shadow: none; }
    section.active { outline: 3px solid rgba(31,122,90,.22); }
    .row { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
    label { display: grid; gap: 6px; font-weight: 700; font-size: 14px; }
    input, select, textarea { width: 100%; border: 1px solid var(--line); border-radius: 15px; padding: 12px 13px; background: rgba(255,255,255,.82); color: var(--ink); font: 15px ui-sans-serif, system-ui, sans-serif; }
    textarea { min-height: 76px; resize: vertical; }
    button { border: 0; border-radius: 16px; padding: 12px 15px; color: white; background: var(--accent); cursor: pointer; font: 700 14px ui-sans-serif, system-ui, sans-serif; box-shadow: 0 10px 24px rgba(31,122,90,.22); }
    button.secondary { background: #39463f; }
    button.warn { background: var(--accent-2); }
    button:disabled { opacity: .45; cursor: wait; }
    .actions { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 14px; }
    .sources { display: grid; gap: 8px; margin-top: 12px; }
    .source { display: grid; grid-template-columns: 1fr auto; gap: 10px; align-items: center; border: 1px solid var(--line); background: rgba(255,255,255,.55); padding: 10px; border-radius: 16px; font: 13px ui-sans-serif, system-ui, sans-serif; }
    .source strong { display: block; font-size: 14px; }
    .source small { color: var(--muted); }
    aside { position: sticky; top: 20px; display: grid; gap: 14px; }
    .status-line { padding: 12px; border-radius: 16px; background: rgba(255,255,255,.55); border: 1px solid var(--line); font: 13px ui-monospace, SFMono-Regular, Menlo, monospace; white-space: pre-wrap; }
    .ok { color: var(--ok); }
    .bad { color: var(--bad); }
    .logs { max-height: 300px; overflow: auto; font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; }
    @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } aside { position: static; } .row { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <h1>Personal signal,<br/>not platform noise.</h1>
        <p>Scenario 1 setup runs locally: connect Telegram, capture sessions from your browser, stage follows, create your first subject, then send a real test digest.</p>
      </div>
      <div class="badge" id="agentBadge">agent: checking</div>
    </header>
    <div class="grid">
      <main class="steps">
        <section data-step="runtime_configured">
          <h2>1. Runtime</h2>
          <p>Set timezone, morning digest time, and your individual Telegram bot token. The token is stored locally and never echoed back.</p>
          <div class="row">
            <label>Timezone <input id="timezone" value="UTC" /></label>
            <label>Digest time <input id="digestTime" value="08:30" /></label>
          </div>
          <label style="margin-top:12px">Telegram bot token <input id="telegramToken" type="password" placeholder="123456:ABC..." /></label>
          <div class="actions"><button onclick="saveSettings()">Save Runtime Settings</button></div>
        </section>
        <section data-step="db_initialized">
          <h2>2. Local Agent</h2>
          <p>Initialize local storage and start the Telegram/scheduler agent. Then send <code>/start</code> to your Telegram bot.</p>
          <div class="actions">
            <button onclick="postAction('/api/init-db')">Init DB</button>
            <button onclick="postAction('/api/agent/start')">Start Agent</button>
            <button class="secondary" onclick="postAction('/api/agent/stop')">Stop Agent</button>
          </div>
        </section>
        <section data-step="sources_imported">
          <h2>3. Connect Sources</h2>
          <p>Log into the platform in your normal browser first, then capture the local session. Use Auto unless you know exactly which browser/profile contains the login. PCCA does not drive social-login flows.</p>
          <div class="row">
            <label>Platform <select id="platform"></select></label>
            <label>Browser for capture <select id="browser"><option value="">Auto (all Chromium browsers)</option><option value="chrome">Chrome</option><option value="arc">Arc</option><option value="brave">Brave</option><option value="edge">Edge</option></select></label>
          </div>
          <div class="row" style="margin-top:12px">
            <label>Import limit <input id="limit" type="number" value="100" min="1" max="500" /></label>
          </div>
          <div class="actions">
            <button class="warn" onclick="captureSession()">Capture Session</button>
            <button onclick="stageFollows()">Stage Follows</button>
          </div>
        </section>
        <section data-step="sources_reviewed">
          <h2>4. Review Staged Sources</h2>
          <p>Remove noisy accounts before attaching the remaining staged sources to your first subject.</p>
          <div class="actions"><button onclick="loadState()">Refresh Sources</button></div>
          <div class="sources" id="sources"></div>
        </section>
        <section data-step="subject_confirmed">
          <h2>5. First Subject</h2>
          <p>Describe what you want. This is the seed of the taste model for this topic.</p>
          <label>Subject name <input id="subject" value="Vibe Coding" /></label>
          <div class="row" style="margin-top:12px">
            <label>Include terms <textarea id="include" placeholder="claude code, release notes, practical agent workflows"></textarea></label>
            <label>Exclude terms <textarea id="exclude" placeholder="biography, generic motivation, listicles"></textarea></label>
          </div>
          <label style="margin-top:12px">High-quality examples <textarea id="examples" placeholder="Examples of posts/videos that would be worth waking up for"></textarea></label>
          <div class="actions"><button onclick="confirmSubject()">Create Subject + Confirm Sources</button></div>
        </section>
        <section data-step="completed">
          <h2>6. Smoke Crawl + Test Digest</h2>
          <p>The wizard only completes if at least one item is collected and at least one Telegram delivery succeeds.</p>
          <div class="actions"><button onclick="smoke()">Smoke Crawl + Test Digest</button></div>
          <div class="status-line" id="smokeStatus">Not run yet.</div>
        </section>
      </main>
      <aside>
        <section>
          <h2>State</h2>
          <div class="status-line" id="stateBox">Loading...</div>
        </section>
        <section>
          <h2>Logs</h2>
          <div class="logs status-line" id="logs"></div>
        </section>
      </aside>
    </div>
  </div>
<script>
const token = new URLSearchParams(location.search).get('token');
const headers = {'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json'};
let busy = false;
function splitTerms(value) { return value.split(',').map(v => v.trim()).filter(Boolean); }
function setBusy(next) { busy = next; document.querySelectorAll('button').forEach(b => b.disabled = next); }
function logLine(line) { const el = document.getElementById('logs'); el.textContent = `${new Date().toLocaleTimeString()} ${line}\n` + el.textContent; }
async function request(path, opts={}) {
  const res = await fetch(path, {...opts, headers: {...headers, ...(opts.headers || {})}});
  const data = await res.json();
  if (!res.ok || data.ok === false) throw new Error(data.message || data.detail || `Request failed: ${path}`);
  return data;
}
async function postAction(path, body={}) {
  try { setBusy(true); const data = await request(path, {method:'POST', body: JSON.stringify(body)}); logLine(data.message || 'ok'); await loadState(); return data; }
  catch (err) { logLine(`ERROR: ${err.message}`); alert(err.message); }
  finally { setBusy(false); }
}
async function saveSettings() {
  return postAction('/api/settings', {token: telegramToken.value, timezone: timezone.value, digest_time: digestTime.value});
}
async function captureSession() { return postAction('/api/session/capture', {platform: platform.value, browser: browser.value}); }
async function loginPlatform() { return postAction('/api/login', {platform: platform.value}); }
async function stageFollows() { return postAction('/api/stage-follows', {platform: platform.value, limit: Number(limit.value || 100)}); }
async function removeSource(id) { return postAction('/api/staged-sources/remove', {id}); }
async function confirmSubject() {
  return postAction('/api/confirm-staged-sources', {
    subject: subject.value,
    include_terms: splitTerms(include.value),
    exclude_terms: splitTerms(exclude.value),
    high_quality_examples: examples.value
  });
}
async function smoke() {
  const data = await postAction('/api/smoke');
  if (data) {
    const smoke = data.data.smoke;
    smokeStatus.textContent = smoke.message;
    smokeStatus.className = `status-line ${smoke.ok ? 'ok' : 'bad'}`;
  }
}
function renderSources(rows) {
  const pending = rows.filter(r => r.status === 'pending');
  sources.innerHTML = pending.length ? '' : '<p>No pending staged sources yet.</p>';
  for (const row of pending) {
    const div = document.createElement('div');
    div.className = 'source';
    div.innerHTML = `<div><strong>${row.display_name}</strong><small>${row.platform} · ${row.account_or_channel_id}</small></div><button class="secondary">Remove</button>`;
    div.querySelector('button').onclick = () => removeSource(row.id);
    sources.appendChild(div);
  }
}
function renderSteps(current) {
  const order = ['runtime_configured','db_initialized','sources_imported','sources_reviewed','subject_confirmed','completed'];
  const index = Math.max(0, order.indexOf(current));
  document.querySelectorAll('section[data-step]').forEach(section => {
    const i = order.indexOf(section.dataset.step);
    section.classList.toggle('done', i < index);
    section.classList.toggle('active', i === index || (current === 'start' && i === 0));
  });
}
async function loadState() {
  try {
    const data = await request('/api/state');
    const s = data.settings;
    timezone.value = s.timezone || 'UTC';
    digestTime.value = s.digest_time || '08:30';
    if (platform.options.length === 0) data.platforms.forEach(p => platform.add(new Option(p, p)));
    renderSources(data.staged_sources || []);
    renderSteps(data.onboarding.current_step || 'start');
    agentBadge.textContent = `agent: ${data.agent_running ? 'running' : 'stopped'}`;
    stateBox.textContent = JSON.stringify({step: data.onboarding.current_step, browser_channel: s.browser_channel, token_configured: s.telegram_token_configured, subjects: data.subjects.map(x => x.name)}, null, 2);
    logs.textContent = (data.logs || []).slice().reverse().join('\n');
  } catch (err) { logLine(`ERROR: ${err.message}`); }
}
loadState();
setInterval(loadState, 5000);
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
            try:
                payload = await read_json(request)
                result = await handler(payload)
                if hasattr(result, "to_dict"):
                    return JSONResponse(result.to_dict())
                return JSONResponse(result)
            except Exception as exc:
                logger.exception("Desktop request failed path=%s", request.url.path)
                return JSONResponse({"ok": False, "message": str(exc)}, status_code=500)

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

        async def remove_staged_source(request):
            assert self.service is not None
            return await run_result(
                lambda p: self.service.remove_staged_source(source_id=int(p.get("id") or 0)),
                request,
            )

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

        async def shutdown_service() -> None:
            if self.service is not None:
                await self.service.shutdown()

        @asynccontextmanager
        async def lifespan(_app):
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
            Route("/api/staged-sources/remove", remove_staged_source, methods=["POST"]),
            Route("/api/confirm-staged-sources", confirm_staged_sources, methods=["POST"]),
            Route("/api/smoke", smoke, methods=["POST"]),
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
