from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pcca.browser.session_manager import BrowserSessionManager
from pcca.services.session_capture_service import (
    CapturedCookie,
    ChromiumCookieReader,
    chrome_time_to_unix,
    domain_matches,
    strip_pkcs7,
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


def test_strip_pkcs7() -> None:
    assert strip_pkcs7(b"hello\x0b" + b"\x0b" * 10) == b"hello"
    assert strip_pkcs7(b"hello") == b"hello"


@pytest.mark.asyncio
async def test_browser_manager_injects_session_cookies(tmp_path: Path, monkeypatch) -> None:
    class FakeContext:
        def __init__(self) -> None:
            self.cookies = None

        async def add_cookies(self, cookies):
            self.cookies = cookies

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
