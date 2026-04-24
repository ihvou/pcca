from pcca.collectors.spotify_collector import normalize_spotify_show_source


def test_normalize_spotify_show_source() -> None:
    assert (
        normalize_spotify_show_source("https://open.spotify.com/show/2MAi0BvDc6GTFvKFPXnkCL?si=abc")
        == "https://open.spotify.com/show/2MAi0BvDc6GTFvKFPXnkCL"
    )
    assert normalize_spotify_show_source("2MAi0BvDc6GTFvKFPXnkCL") == "https://open.spotify.com/show/2MAi0BvDc6GTFvKFPXnkCL"
