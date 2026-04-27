from pcca.config import Settings


def test_settings_loads_local_dotenv_without_overriding_environment(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "\n".join(
            [
                "PCCA_TELEGRAM_BOT_TOKEN=from-dotenv",
                "PCCA_BROWSER_HEADFUL_PLATFORMS=x,linkedin,spotify",
                "PCCA_OLLAMA_ENABLED=true",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("PCCA_TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.setenv("PCCA_OLLAMA_ENABLED", "false")

    settings = Settings.from_env()

    assert settings.telegram_bot_token == "from-dotenv"
    assert settings.timezone == "UTC"
    assert settings.browser_headful_platforms == {"x", "linkedin", "spotify"}
    assert settings.browser_channel == "chrome"
    assert settings.ollama_enabled is False
    assert settings.session_refresh_enabled is True
    assert settings.session_refresh_cooldown_seconds == 1800
    assert settings.session_refresh_browser is None


def test_browser_channel_can_use_bundled_chromium(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("PCCA_BROWSER_CHANNEL=bundled\n", encoding="utf-8")

    settings = Settings.from_env()

    assert settings.browser_channel == "bundled"


def test_session_refresh_settings(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "PCCA_SESSION_REFRESH_ENABLED=false",
                "PCCA_SESSION_REFRESH_COOLDOWN_SECONDS=42",
                "PCCA_SESSION_REFRESH_BROWSER=arc",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings.from_env()

    assert settings.session_refresh_enabled is False
    assert settings.session_refresh_cooldown_seconds == 42
    assert settings.session_refresh_browser == "arc"
