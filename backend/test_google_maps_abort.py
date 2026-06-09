import pytest

from backend.scraper.google_maps import GoogleMapsScraper


def test_abort_callback_allows_scrape_to_continue() -> None:
    scraper = GoogleMapsScraper(headless=True)

    scraper.register_abort_callback(lambda: False)

    scraper._raise_if_aborted()


def test_abort_callback_raises_when_stop_requested() -> None:
    scraper = GoogleMapsScraper(headless=True)

    scraper.register_abort_callback(lambda: True)

    with pytest.raises(RuntimeError, match="Stopped by user"):
        scraper._raise_if_aborted()
