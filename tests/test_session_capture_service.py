from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pcca.browser.session_manager import BrowserSessionManager
from pcca.services.session_capture_service import (
    CapturedCookie,
    ChromiumCookieReader,
    PLATFORM_COOKIE_TARGETS,
    chrome_time_to_unix,
    domain_matches,
    missing_requirement_descriptions,
    required_requirement_descriptions,
    strip_pkcs7,
    target_cookie_names,
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
