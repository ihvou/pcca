from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pcca.browser.session_manager import BrowserSessionManager
from pcca.config import Settings
from pcca.services.session_capture_service import (
    BrowserCookieSource,
    BrowserProfileCookieDb,
    CapturedCookie,
    ChromiumCookieReader,
    PLATFORM_COOKIE_TARGETS,
    SessionRefreshService,
    chrome_time_to_unix,
    domain_matches,
    missing_requirement_descriptions,
    required_requirement_descriptions,
    strip_pkcs7,
    target_cookie_names,
)


def make_settings(tmp_path: Path, *, refresh_enabled: bool = True, cooldown_seconds: int = 1800) -> Settings:
    data_dir = tmp_path / ".pcca"
    return Settings(
        timezone="UTC",
        nightly_cron="0 1 * * *",
        morning_cron="30 8 * * *",
        digest_auto_send=False,
        data_dir=data_dir,
        db_path=data_dir / "pcca.db",
        browser_profiles_dir=data_dir / "browser_profiles",
        browser_headless=True,
        browser_headful_platforms={"x", "linkedin"},
        browser_channel="chrome",
        ollama_enabled=False,
        ollama_base_url="http://localhost:11434",
        ollama_model="qwen2.5:7b",
        telegram_bot_token=None,
        session_refresh_enabled=refresh_enabled,
        session_refresh_cooldown_seconds=cooldown_seconds,
        session_refresh_browser=None,
    )


def test_domain_matching_accepts_subdomains() -> None:
    assert domain_matches(".x.com", ["x.com"]) is True
    assert domain_matches("api.x.com", ["x.com"]) is True
    assert domain_matches("example.com", ["x.com"]) is False


def test_chrome_time_to_unix_handles_session_and_expiring_cookies() -> None:
    assert chrome_time_to_unix(0) is None
    assert chrome_time_to_unix(11644473600 * 1_000_000 + 42_000_000) == 42


def test_captured_cookie_to_playwright_does_not_expose_value_in_repr() -> None:
    cookie = CapturedCookie(
        name="auth_token",
        value="secret",
        domain=".x.com",
        expires=42,
        same_site="Lax",
    )

    assert "secret" not in repr(cookie)
    assert cookie.to_playwright() == {
        "name": "auth_token",
        "value": "secret",
        "domain": ".x.com",
        "path": "/",
        "httpOnly": True,
        "secure": True,
        "expires": 42,
        "sameSite": "Lax",
    }


def test_reader_extracts_httponly_target_cookies_from_chrome_db(tmp_path: Path) -> None:
    db_path = tmp_path / "Cookies"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE cookies (
          host_key TEXT,
          name TEXT,
          value TEXT,
          encrypted_value BLOB,
          path TEXT,
          expires_utc INTEGER,
          is_secure INTEGER,
          is_httponly INTEGER,
          samesite INTEGER
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO cookies(host_key, name, value, encrypted_value, path, expires_utc, is_secure, is_httponly, samesite)
        VALUES (?, ?, ?, ?, '/', 0, 1, 1, 1)
        """,
        [
            (".x.com", "auth_token", "", b"encrypted-auth"),
            (".x.com", "ct0", "csrf", b""),
            (".example.com", "auth_token", "wrong-domain", b""),
        ],
    )
    conn.commit()
    conn.close()

    def fake_decryptor(**kwargs):
        if kwargs["value"]:
            return kwargs["value"]
        return "auth"

    reader = ChromiumCookieReader(
        keychain_password_reader=lambda _service: "safe-storage",
        decryptor=fake_decryptor,
    )

    cookies = reader._read_cookie_db_copy(
        db_path=db_path,
        keychain_password="safe-storage",
        wanted_names={"auth_token", "ct0"},
        domains=["x.com"],
    )

    assert {cookie.name: cookie.value for cookie in cookies} == {
        "auth_token": "auth",
        "ct0": "csrf",
    }
    assert all(cookie.http_only for cookie in cookies)


def test_platform_cookie_targets_cover_logged_in_platforms() -> None:
    assert set(PLATFORM_COOKIE_TARGETS) == {
        "x",
        "linkedin",
        "youtube",
        "spotify",
        "substack",
        "medium",
        "apple_podcasts",
    }

    for target in PLATFORM_COOKIE_TARGETS.values():
        assert target["domains"]
        assert target_cookie_names(target)
        assert required_requirement_descriptions(target)


def test_missing_requirement_descriptions_supports_any_groups() -> None:
    target = PLATFORM_COOKIE_TARGETS["youtube"]

    assert missing_requirement_descriptions(
        target,
        {"SID", "SAPISID"},
    ) == []
    assert missing_requirement_descriptions(target, {"SID"}) == [
        "one of: SAPISID, APISID, __Secure-1PAPISID, __Secure-3PAPISID"
    ]


def test_reader_extracts_non_x_platform_target_cookies_from_chrome_db(tmp_path: Path) -> None:
    db_path = tmp_path / "Cookies"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE cookies (
          host_key TEXT,
          name TEXT,
          value TEXT,
          encrypted_value BLOB,
          path TEXT,
          expires_utc INTEGER,
          is_secure INTEGER,
          is_httponly INTEGER,
          samesite INTEGER
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO cookies(host_key, name, value, encrypted_value, path, expires_utc, is_secure, is_httponly, samesite)
        VALUES (?, ?, ?, ?, '/', 0, 1, 1, 1)
        """,
        [
            (".linkedin.com", "li_at", "linkedin-auth", b""),
            (".youtube.com", "SID", "youtube-sid", b""),
            (".google.com", "SAPISID", "youtube-sapisid", b""),
            (".spotify.com", "sp_dc", "spotify-auth", b""),
            (".substack.com", "substack.sid", "substack-auth", b""),
            (".medium.com", "sid", "medium-auth", b""),
            (".apple.com", "myacinfo", "apple-auth", b""),
        ],
    )
    conn.commit()
    conn.close()

    reader = ChromiumCookieReader(
        keychain_password_reader=lambda _service: "safe-storage",
    )

    samples = {
        "linkedin": {"li_at"},
        "youtube": {"SID", "SAPISID"},
        "spotify": {"sp_dc"},
        "substack": {"substack.sid"},
        "medium": {"sid"},
        "apple_podcasts": {"myacinfo"},
    }
    for platform, expected_names in samples.items():
        target = PLATFORM_COOKIE_TARGETS[platform]
        cookies = reader._read_cookie_db_copy(
            db_path=db_path,
            keychain_password="safe-storage",
            wanted_names=target_cookie_names(target),
            domains=target["domains"],
        )

        captured_names = {cookie.name for cookie in cookies}
        assert expected_names <= captured_names
        assert missing_requirement_descriptions(target, captured_names) == []


def test_strip_pkcs7() -> None:
    assert strip_pkcs7(b"hello\x0b" + b"\x0b" * 10) == b"hello"
    assert strip_pkcs7(b"hello") == b"hello"


@pytest.mark.asyncio
async def test_browser_manager_injects_session_cookies(tmp_path: Path, monkeypatch) -> None:
    class FakePage:
        def __init__(self) -> None:
            self.gotos: list[str] = []
            self.closed = False

        async def goto(self, url, wait_until=None, timeout=None):
            self.gotos.append(url)

        async def wait_for_timeout(self, ms):
            return None

        def is_closed(self) -> bool:
            return self.closed

        async def close(self) -> None:
            self.closed = True

    class FakeContext:
        def __init__(self) -> None:
            self.cookies = None
            self.page = FakePage()

        async def add_cookies(self, cookies):
            self.cookies = cookies

        async def new_page(self):
            return self.page

    context = FakeContext()
    manager = BrowserSessionManager(profiles_root=tmp_path)

    async def fake_get_context(platform: str):
        assert platform == "x"
        return context

    monkeypatch.setattr(manager, "get_context", fake_get_context)

    count = await manager.inject_session_cookies(
        platform="x",
        cookies=[{"name": "auth_token", "value": "secret", "domain": ".x.com", "path": "/"}],
    )

    assert count == 1
    assert context.cookies == [{"name": "auth_token", "value": "secret", "domain": ".x.com", "path": "/"}]
    # Cookie commit requires an actual navigation event; verify we visited the
    # primary cookie domain and closed the page cleanly afterwards.
    assert context.page.gotos == ["https://x.com/"]
    assert context.page.closed is True


@pytest.mark.asyncio
async def test_session_refresh_injects_and_respects_cooldown(tmp_path: Path) -> None:
    class FakeCookieReader:
        calls = 0

        def read_platform_cookies(self, *, platform: str, browser: str | None = None):
            self.calls += 1
            assert platform == "x"
            assert browser is None
            return (
                BrowserProfileCookieDb(
                    source=BrowserCookieSource(
                        browser="arc",
                        display_name="Arc",
                        user_data_root=tmp_path,
                        keychain_service="Arc Safe Storage",
                    ),
                    profile_name="Default",
                    cookie_db_path=tmp_path / "Cookies",
                ),
                [
                    CapturedCookie(name="auth_token", value="auth", domain=".x.com"),
                    CapturedCookie(name="ct0", value="csrf", domain=".x.com"),
                ],
            )

    class FakeSessionManager:
        def __init__(self) -> None:
            self.injected: list[tuple[str, list[dict]]] = []

        async def inject_session_cookies(self, *, platform: str, cookies: list[dict]) -> int:
            self.injected.append((platform, cookies))
            return len(cookies)

    reader = FakeCookieReader()
    manager = FakeSessionManager()
    service = SessionRefreshService(
        settings=make_settings(tmp_path, cooldown_seconds=1800),
        session_manager=manager,  # type: ignore[arg-type]
        cookie_reader=reader,  # type: ignore[arg-type]
    )

    first = await service.refresh_platform("x")
    second = await service.refresh_platform("x")

    assert first.refreshed is True
    assert first.browser == "arc"
    assert first.injected_cookie_count == 2
    assert second.skipped is True
    assert second.reason == "cooldown"
    assert reader.calls == 1
    assert len(manager.injected) == 1


@pytest.mark.asyncio
async def test_session_refresh_missing_required_cookies_does_not_inject(tmp_path: Path) -> None:
    class FakeCookieReader:
        def read_platform_cookies(self, *, platform: str, browser: str | None = None):
            return (
                BrowserProfileCookieDb(
                    source=BrowserCookieSource(
                        browser="chrome",
                        display_name="Chrome",
                        user_data_root=tmp_path,
                        keychain_service="Chrome Safe Storage",
                    ),
                    profile_name="Default",
                    cookie_db_path=tmp_path / "Cookies",
                ),
                [CapturedCookie(name="guest_id", value="guest", domain=".x.com")],
            )

    class FakeSessionManager:
        async def inject_session_cookies(self, *, platform: str, cookies: list[dict]) -> int:
            raise AssertionError("Should not inject incomplete sessions")

    service = SessionRefreshService(
        settings=make_settings(tmp_path, cooldown_seconds=0),
        session_manager=FakeSessionManager(),  # type: ignore[arg-type]
        cookie_reader=FakeCookieReader(),  # type: ignore[arg-type]
    )

    result = await service.refresh_platform("x")

    assert result.refreshed is False
    assert result.reason == "missing_required_cookies"
    assert result.missing_cookie_names == ["auth_token", "ct0"]


@pytest.mark.asyncio
async def test_session_refresh_read_error_falls_back_without_injecting(tmp_path: Path) -> None:
    class FakeCookieReader:
        def read_platform_cookies(self, *, platform: str, browser: str | None = None):
            _ = platform, browser
            raise RuntimeError("keychain denied")

    class FakeSessionManager:
        async def inject_session_cookies(self, *, platform: str, cookies: list[dict]) -> int:
            _ = platform, cookies
            raise AssertionError("Should keep the existing PCCA profile untouched")

    service = SessionRefreshService(
        settings=make_settings(tmp_path, cooldown_seconds=0),
        session_manager=FakeSessionManager(),  # type: ignore[arg-type]
        cookie_reader=FakeCookieReader(),  # type: ignore[arg-type]
    )

    result = await service.refresh_platform("youtube")

    assert result.refreshed is False
    assert result.skipped is False
    assert result.reason.startswith("cookie_read_failed:")
    assert result.missing_cookie_names == [
        "one of: SID, __Secure-1PSID, __Secure-3PSID",
        "one of: SAPISID, APISID, __Secure-1PAPISID, __Secure-3PAPISID",
    ]
