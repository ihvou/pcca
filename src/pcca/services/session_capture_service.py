from __future__ import annotations

import hashlib
import hmac
import shutil
import sqlite3
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from pcca.browser.session_manager import BrowserSessionManager
from pcca.config import Settings

CHROME_EPOCH_OFFSET_SECONDS = 11644473600


@dataclass(frozen=True)
class BrowserCookieSource:
    browser: str
    display_name: str
    user_data_root: Path
    keychain_service: str


@dataclass(frozen=True)
class BrowserProfileCookieDb:
    source: BrowserCookieSource
    profile_name: str
    cookie_db_path: Path


@dataclass
class CapturedCookie:
    name: str
    value: str = field(repr=False)
    domain: str
    path: str = "/"
    expires: int | None = None
    http_only: bool = True
    secure: bool = True
    same_site: str | None = None

    def to_playwright(self) -> dict:
        cookie = {
            "name": self.name,
            "value": self.value,
            "domain": self.domain,
            "path": self.path or "/",
            "httpOnly": self.http_only,
            "secure": self.secure,
        }
        if self.expires is not None:
            cookie["expires"] = self.expires
        if self.same_site is not None:
            cookie["sameSite"] = self.same_site
        return cookie


@dataclass
class SessionCaptureResult:
    platform: str
    browser: str
    profile_name: str
    injected_cookie_count: int
    required_cookie_names: list[str]
    captured_cookie_names: list[str]
    missing_cookie_names: list[str]

    @property
    def ok(self) -> bool:
        return not self.missing_cookie_names

    def safe_summary(self) -> dict:
        return {
            "platform": self.platform,
            "browser": self.browser,
            "profile_name": self.profile_name,
            "injected_cookie_count": self.injected_cookie_count,
            "required_cookie_names": self.required_cookie_names,
            "captured_cookie_names": self.captured_cookie_names,
            "missing_cookie_names": self.missing_cookie_names,
        }


PLATFORM_COOKIE_TARGETS = {
    "x": {
        "domains": ["x.com", "twitter.com"],
        "required": ["auth_token", "ct0"],
        "optional": ["twid"],
    }
}


def chromium_cookie_sources(home: Path | None = None) -> list[BrowserCookieSource]:
    root = home or Path.home()
    app_support = root / "Library" / "Application Support"
    return [
        BrowserCookieSource(
            browser="chrome",
            display_name="Google Chrome",
            user_data_root=app_support / "Google" / "Chrome",
            keychain_service="Chrome Safe Storage",
        ),
        BrowserCookieSource(
            browser="arc",
            display_name="Arc",
            user_data_root=app_support / "Arc" / "User Data",
            keychain_service="Arc Safe Storage",
        ),
        BrowserCookieSource(
            browser="brave",
            display_name="Brave",
            user_data_root=app_support / "BraveSoftware" / "Brave-Browser",
            keychain_service="Brave Safe Storage",
        ),
        BrowserCookieSource(
            browser="edge",
            display_name="Microsoft Edge",
            user_data_root=app_support / "Microsoft Edge",
            keychain_service="Microsoft Edge Safe Storage",
        ),
    ]


def chrome_time_to_unix(expires_utc: int | None) -> int | None:
    if not expires_utc:
        return None
    seconds = int(expires_utc / 1_000_000) - CHROME_EPOCH_OFFSET_SECONDS
    return seconds if seconds > 0 else None


def domain_matches(host_key: str, domains: Iterable[str]) -> bool:
    normalized = host_key.lstrip(".").lower()
    return any(normalized == domain or normalized.endswith(f".{domain}") for domain in domains)


def same_site_from_chrome(value: int | None) -> str | None:
    return {0: "None", 1: "Lax", 2: "Strict"}.get(value if value is not None else -1)


class ChromiumCookieReader:
    def __init__(
        self,
        *,
        home: Path | None = None,
        keychain_password_reader=None,
        decryptor=None,
    ) -> None:
        self.home = home or Path.home()
        self.keychain_password_reader = keychain_password_reader or read_macos_keychain_password
        self.decryptor = decryptor or decrypt_chromium_cookie_value
        self._password_cache: dict[str, str] = {}

    def sources(self, browser: str | None = None) -> list[BrowserCookieSource]:
        sources = chromium_cookie_sources(self.home)
        if browser:
            requested = browser.strip().lower()
            sources = [source for source in sources if source.browser == requested]
        return sources

    def cookie_dbs(self, browser: str | None = None) -> list[BrowserProfileCookieDb]:
        dbs: list[BrowserProfileCookieDb] = []
        for source in self.sources(browser):
            if not source.user_data_root.exists():
                continue
            for profile_dir in sorted(source.user_data_root.iterdir()):
                if not profile_dir.is_dir():
                    continue
                candidates = [
                    profile_dir / "Network" / "Cookies",
                    profile_dir / "Cookies",
                ]
                for candidate in candidates:
                    if candidate.exists():
                        dbs.append(
                            BrowserProfileCookieDb(
                                source=source,
                                profile_name=profile_dir.name,
                                cookie_db_path=candidate,
                            )
                        )
                        break
        return dbs

    def read_platform_cookies(
        self,
        *,
        platform: str,
        browser: str | None = None,
    ) -> tuple[BrowserProfileCookieDb, list[CapturedCookie]]:
        target = PLATFORM_COOKIE_TARGETS.get(platform)
        if target is None:
            raise ValueError(f"Session capture is not implemented for platform '{platform}' yet.")

        wanted_names = set(target["required"]) | set(target.get("optional", []))
        domains = list(target["domains"])
        required = set(target["required"])
        best: tuple[BrowserProfileCookieDb, list[CapturedCookie]] | None = None
        tried: list[str] = []
        errors: list[str] = []

        for db in self.cookie_dbs(browser):
            tried.append(str(db.cookie_db_path))
            try:
                password = self._password_cache.get(db.source.keychain_service)
                if password is None:
                    password = self.keychain_password_reader(db.source.keychain_service)
                    self._password_cache[db.source.keychain_service] = password
                cookies = self.read_cookie_db(
                    db_path=db.cookie_db_path,
                    keychain_password=password,
                    wanted_names=wanted_names,
                    domains=domains,
                )
            except Exception as exc:
                errors.append(f"{db.source.browser}/{db.profile_name}: {exc}")
                continue
            captured_names = {cookie.name for cookie in cookies}
            if best is None or len(captured_names & required) > len({cookie.name for cookie in best[1]} & required):
                best = (db, cookies)
            if required <= captured_names:
                return db, cookies

        if best is not None and best[1]:
            return best
        hint = "No Chromium browser cookie DBs were found." if not tried else "Tried: " + ", ".join(tried)
        if errors:
            hint += ". Errors: " + "; ".join(errors)
        raise RuntimeError(
            f"Could not find required {platform} cookies in the selected browser profiles. {hint}"
        )

    def read_cookie_db(
        self,
        *,
        db_path: Path,
        keychain_password: str,
        wanted_names: set[str],
        domains: list[str],
    ) -> list[CapturedCookie]:
        with tempfile.TemporaryDirectory(prefix="pcca-cookies-") as tmp:
            tmp_db = Path(tmp) / "Cookies"
            shutil.copy2(db_path, tmp_db)
            for suffix in ("-wal", "-shm"):
                sidecar = Path(str(db_path) + suffix)
                if sidecar.exists():
                    shutil.copy2(sidecar, Path(str(tmp_db) + suffix))
            return self._read_cookie_db_copy(
                db_path=tmp_db,
                keychain_password=keychain_password,
                wanted_names=wanted_names,
                domains=domains,
            )

    def _read_cookie_db_copy(
        self,
        *,
        db_path: Path,
        keychain_password: str,
        wanted_names: set[str],
        domains: list[str],
    ) -> list[CapturedCookie]:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            placeholders = ",".join("?" for _ in wanted_names)
            rows = conn.execute(
                f"""
                SELECT host_key, name, value, encrypted_value, path, expires_utc,
                       is_secure, is_httponly, samesite
                FROM cookies
                WHERE name IN ({placeholders})
                """,
                tuple(sorted(wanted_names)),
            ).fetchall()
        finally:
            conn.close()

        cookies: list[CapturedCookie] = []
        for row in rows:
            domain = str(row["host_key"])
            if not domain_matches(domain, domains):
                continue
            value = self.decryptor(
                host_key=domain,
                value=row["value"],
                encrypted_value=row["encrypted_value"],
                keychain_password=keychain_password,
            )
            if not value:
                continue
            cookies.append(
                CapturedCookie(
                    name=str(row["name"]),
                    value=value,
                    domain=domain,
                    path=str(row["path"] or "/"),
                    expires=chrome_time_to_unix(row["expires_utc"]),
                    http_only=bool(row["is_httponly"]),
                    secure=bool(row["is_secure"]),
                    same_site=same_site_from_chrome(row["samesite"]),
                )
            )
        return cookies


class SessionCaptureService:
    def __init__(
        self,
        *,
        settings: Settings,
        cookie_reader: ChromiumCookieReader | None = None,
    ) -> None:
        self.settings = settings
        self.cookie_reader = cookie_reader or ChromiumCookieReader()

    async def capture_and_inject(self, *, platform: str, browser: str | None = None) -> SessionCaptureResult:
        platform = platform.strip().lower()
        target = PLATFORM_COOKIE_TARGETS.get(platform)
        if target is None:
            raise ValueError(f"Session capture is not implemented for platform '{platform}' yet.")

        profile_db, cookies = self.cookie_reader.read_platform_cookies(platform=platform, browser=browser)
        manager = BrowserSessionManager(
            profiles_root=self.settings.browser_profiles_dir,
            headless=True,
            browser_channel=self.settings.browser_channel,
            headful_platforms=set(),
        )
        await manager.start()
        try:
            await manager.inject_session_cookies(
                platform=platform,
                cookies=[cookie.to_playwright() for cookie in cookies],
            )
        finally:
            await manager.stop()

        captured_names = sorted({cookie.name for cookie in cookies})
        required = list(target["required"])
        missing = [name for name in required if name not in captured_names]
        return SessionCaptureResult(
            platform=platform,
            browser=profile_db.source.browser,
            profile_name=profile_db.profile_name,
            injected_cookie_count=len(cookies),
            required_cookie_names=required,
            captured_cookie_names=captured_names,
            missing_cookie_names=missing,
        )


def read_macos_keychain_password(service_name: str) -> str:
    proc = subprocess.run(
        ["security", "find-generic-password", "-w", "-s", service_name],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip() or "no keychain details returned"
        raise RuntimeError(f"Could not read macOS Keychain item '{service_name}': {detail}")
    return proc.stdout.strip()


def decrypt_chromium_cookie_value(
    *,
    host_key: str,
    value: str | bytes | None,
    encrypted_value: bytes | None,
    keychain_password: str,
) -> str:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, bytes) and value:
        return value.decode("utf-8", errors="replace")
    if not encrypted_value:
        return ""

    encrypted = bytes(encrypted_value)
    if encrypted.startswith(b"v10") or encrypted.startswith(b"v11"):
        encrypted = encrypted[3:]
    key = hashlib.pbkdf2_hmac(
        "sha1",
        keychain_password.encode("utf-8"),
        b"saltysalt",
        1003,
        dklen=16,
    )
    decrypted = openssl_aes_128_cbc_decrypt(encrypted, key=key, iv=b" " * 16)
    decrypted = strip_pkcs7(decrypted)

    host_digest = hashlib.sha256(host_key.encode("utf-8")).digest()
    if hmac.compare_digest(decrypted[:32], host_digest):
        decrypted = decrypted[32:]
    return decrypted.decode("utf-8", errors="replace")


def strip_pkcs7(data: bytes) -> bytes:
    if not data:
        return data
    padding = data[-1]
    if padding < 1 or padding > 16:
        return data
    if data[-padding:] != bytes([padding]) * padding:
        return data
    return data[:-padding]


def openssl_aes_128_cbc_decrypt(encrypted: bytes, *, key: bytes, iv: bytes) -> bytes:
    proc = subprocess.run(
        [
            "openssl",
            "enc",
            "-d",
            "-aes-128-cbc",
            "-K",
            key.hex(),
            "-iv",
            iv.hex(),
            "-nopad",
        ],
        input=encrypted,
        check=False,
        capture_output=True,
    )
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"OpenSSL cookie decryption failed: {detail}")
    return proc.stdout
