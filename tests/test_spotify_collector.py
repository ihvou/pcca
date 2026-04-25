from pcca.collectors.spotify_collector import is_spotify_login_url, normalize_spotify_show_source


def test_normalize_spotify_show_source() -> None:
    assert (
        normalize_spotify_show_source("https://open.spotify.com/show/2MAi0BvDc6GTFvKFPXnkCL?si=abc")
        == "https://open.spotify.com/show/2MAi0BvDc6GTFvKFPXnkCL"
    )
    assert normalize_spotify_show_source("2MAi0BvDc6GTFvKFPXnkCL") == "https://open.spotify.com/show/2MAi0BvDc6GTFvKFPXnkCL"


def test_spotify_login_url_detection() -> None:
    assert is_spotify_login_url("https://accounts.spotify.com/en/login")
    assert is_spotify_login_url("https://open.spotify.com/login")
    assert not is_spotify_login_url("https://open.spotify.com/show/2MAi0BvDc6GTFvKFPXnkCL")
