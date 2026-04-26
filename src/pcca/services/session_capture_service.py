from __future__ import annotations

import hashlib
import hmac
import logging
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
logger = logging.getLogger(__name__)


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
        "optional": ["twid", "guest_id", "personalization_id"],
    },
    "linkedin": {
        "domains": ["linkedin.com"],
        "required": ["li_at"],
        "optional": ["JSESSIONID", "bcookie", "bscookie", "liap", "lidc"],
    },
    "youtube": {
        "domains": ["youtube.com", "google.com", "accounts.google.com"],
        "required_any": [
            ["SID", "__Secure-1PSID", "__Secure-3PSID"],
            ["SAPISID", "APISID", "__Secure-1PAPISID", "__Secure-3PAPISID"],
        ],
        "optional": [
            "HSID",
            "SSID",
            "LOGIN_INFO",
            "VISITOR_INFO1_LIVE",
            "__Secure-1PSIDTS",
            "__Secure-3PSIDTS",
        ],
    },
    "spotify": {
        "domains": ["spotify.com", "open.spotify.com"],
        "required": ["sp_dc"],
        "optional": ["sp_key", "sp_t", "sp_landing"],
    },
    "substack": {
        "domains": ["substack.com"],
        "required": ["substack.sid"],
        "optional": ["substack.lli", "substack.uis"],
    },
    "medium": {
        "domains": ["medium.com"],
        "required_any": [["sid", "uid"]],
        "optional": ["xsrf", "sz"],
    },
    "apple_podcasts": {
        "domains": ["apple.com", "podcasts.apple.com", "idmsa.apple.com"],
        "required_any": [["myacinfo", "aidsp", "itctx", "itspod"]],
        "optional": ["dslang", "site", "geo", "acn01"],
    },
}


def target_cookie_names(target: dict) -> set[str]:
    names = set(target.get("required", [])) | set(target.get("optional", []))
    for group in target.get("required_any", []):
        names.update(group)
    return names


def missing_requirement_descriptions(target: dict, captured_names: set[str]) -> list[str]:
    missing = [name for name in target.get("required", []) if name not in captured_names]
    for group in target.get("required_any", []):
        if not captured_names.intersection(group):
            missing.append("one of: " + ", ".join(group))
    return missing


def required_requirement_descriptions(target: dict) -> list[str]:
    required = list(target.get("required", []))
    required.extend("one of: " + ", ".join(group) for group in target.get("required_any", []))
    return required


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

        wanted_names = target_cookie_names(target)
        domains = list(target["domains"])
        best: tuple[BrowserProfileCookieDb, list[CapturedCookie]] | None = None
        tried: list[str] = []
        errors: list[str] = []
        logger.debug(
            "Searching local browser cookies platform=%s browser=%s domains=%s target_names=%s",
            platform,
            browser or "auto",
            domains,
            sorted(wanted_names),
        )

        for db in self.cookie_dbs(browser):
            tried.append(str(db.cookie_db_path))
            logger.debug(
                "Trying browser cookie DB platform=%s browser=%s profile=%s path=%s",
                platform,
                db.source.browser,
                db.profile_name,
                db.cookie_db_path,
            )
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
                logger.debug(
                    "Cookie DB read failed platform=%s browser=%s profile=%s error=%s",
                    platform,
                    db.source.browser,
                    db.profile_name,
                    exc,
                )
                continue
            captured_names = {cookie.name for cookie in cookies}
            logger.debug(
                "Cookie DB candidate platform=%s browser=%s profile=%s captured_names=%s missing=%s",
                platform,
                db.source.browser,
                db.profile_name,
                sorted(captured_names),
                missing_requirement_descriptions(target, captured_names),
            )
            if best is None or len(captured_names & wanted_names) > len({cookie.name for cookie in best[1]} & wanted_names):
                best = (db, cookies)
            if not missing_requirement_descriptions(target, captured_names):
                logger.info(
                    "Selected browser session platform=%s browser=%s profile=%s captured_names=%s",
                    platform,
                    db.source.browser,
                    db.profile_name,
                    sorted(captured_names),
                )
                return db, cookies

        if best is not None and best[1]:
            logger.info(
                "Selected partial browser session platform=%s browser=%s profile=%s captured_names=%s missing=%s",
                platform,
                best[0].source.browser,
                best[0].profile_name,
                sorted({cookie.name for cookie in best[1]}),
                missing_requirement_descriptions(target, {cookie.name for cookie in best[1]}),
            )
            return best
        hint = "No Chromium browser cookie DBs were found." if not tried else "Tried: " + ", ".join(tried)
        if errors:
            hint += ". Errors: " + "; ".join(errors)
        logger.warning(
            "No usable browser session found platform=%s browser=%s tried=%s errors=%s",
            platform,
            browser or "auto",
            tried,
            errors,
        )
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

        logger.info("Starting session capture platform=%s browser=%s", platform, browser or "auto")
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
        missing = missing_requirement_descriptions(target, set(captured_names))
        logger.info(
            "Session capture complete platform=%s browser=%s profile=%s injected_cookie_count=%d missing=%s",
            platform,
            profile_db.source.browser,
            profile_db.profile_name,
            len(cookies),
            missing,
        )
        return SessionCaptureResult(
            platform=platform,
            browser=profile_db.source.browser,
            profile_name=profile_db.profile_name,
            injected_cookie_count=len(cookies),
            required_cookie_names=required_requirement_descriptions(target),
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
