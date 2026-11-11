"""Regression tests for the crawl hard deadline (TIMEOUT_LIMIT) and for
client disconnect handling.

These tests run fully offline and in-process: the reactor runs in a
background thread, local HTTP pages (one normal, one hanging forever) are
served by an in-process Twisted web server, and the crawl timeout is driven
by a controlled clock (twisted.internet.task.Clock), so no real time is
ever waited out.
"""

import http.client
import logging
import sys
import threading
import time
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock
from urllib.parse import urlencode

import pytest
from twisted.internet import defer
from twisted.internet.task import Clock
from twisted.internet.threads import blockingCallFromThread
from twisted.web import resource, server
from twisted.web.error import Error

from scrapyrt.conf import app_settings
from scrapyrt.core import CrawlManager, ScrapyrtCrawlerRunner
from scrapyrt.resources import RealtimeApi

from . import SAMPLE_DATA

if TYPE_CHECKING:
    from twisted.web.resource import IResource

OK_PAGE = b"<html><body><h1>Page 1</h1></body></html>"


def wait_until(predicate, timeout=10.0, interval=0.05):
    """Wait until predicate() is true; return False on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class OkResource(resource.Resource):
    isLeaf = True

    def render_GET(self, request):
        return OK_PAGE


class HangResource(resource.Resource):
    """A page that accepts the request but never responds."""

    isLeaf = True

    def __init__(self):
        super().__init__()
        self.requested = threading.Event()
        self.requests = []
        self._waiters = []

    def render_GET(self, request):
        self.requests.append(request)
        self.requested.set()
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            waiter.callback(request)
        return server.NOT_DONE_YET

    def wait_request(self):
        """Return a Deferred firing when the next request arrives."""
        waiter: defer.Deferred = defer.Deferred()
        self._waiters.append(waiter)
        return waiter


@pytest.fixture(scope="module")
def live_reactor():
    """Run the reactor in a background thread for this module.

    The reactor is started on first use and stopped when the module is
    done, so other test modules find it in the same (not running) state
    they would without these tests.
    """
    from twisted.internet import reactor as _reactor

    # The reactor object is typed as a module by mypy; it is really the
    # installed reactor instance.
    reactor = cast("Any", _reactor)

    started_here = False
    if not reactor.running:
        thread = threading.Thread(
            target=reactor.run,
            kwargs={"installSignalHandlers": False},
            daemon=True,
        )
        thread.start()
        assert wait_until(lambda: reactor.running)
        started_here = True
    # Make sure the loop actually processes calls from other threads.
    blockingCallFromThread(reactor, lambda: None)
    yield reactor
    if started_here:
        blockingCallFromThread(reactor, reactor.stop)
        assert wait_until(lambda: not reactor.running)


@pytest.fixture(scope="module")
def project(tmp_path_factory):
    """Make the sample scrapy project importable and known to scrapyrt."""
    project_dir = SAMPLE_DATA / "testproject"
    sys.path.insert(0, str(project_dir))
    old_project_settings = app_settings.PROJECT_SETTINGS
    old_log_dir = app_settings.LOG_DIR
    log_dir = tmp_path_factory.mktemp("scrapyrt-logs")
    app_settings.PROJECT_SETTINGS = "testproject.settings"
    app_settings.LOG_DIR = str(log_dir)
    try:
        yield log_dir
    finally:
        app_settings.PROJECT_SETTINGS = old_project_settings
        app_settings.LOG_DIR = old_log_dir
        sys.path.remove(str(project_dir))


@pytest.fixture(scope="module")
def http_sites(live_reactor):
    """Serve local HTTP pages and the scrapyrt API on the live reactor."""
    reactor = live_reactor
    hang = HangResource()
    pages = resource.Resource()
    pages.putChild(b"ok", cast("IResource", OkResource()))
    pages.putChild(b"hang", cast("IResource", hang))
    page_port = blockingCallFromThread(
        reactor,
        reactor.listenTCP,
        0,
        server.Site(pages),
        interface="127.0.0.1",
    )
    api_port = blockingCallFromThread(
        reactor,
        reactor.listenTCP,
        0,
        server.Site(RealtimeApi()),
        interface="127.0.0.1",
    )
    sites = SimpleNamespace(
        hang=hang,
        page_url=f"http://127.0.0.1:{page_port.getHost().port}",
        api_url=f"http://127.0.0.1:{api_port.getHost().port}",
        api_port=api_port.getHost().port,
    )
    try:
        yield sites
    finally:
        blockingCallFromThread(reactor, page_port.stopListening)
        blockingCallFromThread(reactor, api_port.stopListening)


@pytest.fixture
def harness(monkeypatch):
    """Test harness for observing and controlling crawls.

    Records every CrawlManager created by the resource layer, replaces
    the clock used for crawl timeouts with a controlled one and records
    write()/finish() calls on server-side requests.
    """

    created = []

    class RecordingCrawlManager(CrawlManager):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    monkeypatch.setattr("scrapyrt.core.CrawlManager", RecordingCrawlManager)

    fake_clock = Clock()
    monkeypatch.setattr(CrawlManager, "clock", fake_clock)

    writes = []
    finishes = []
    original_write = server.Request.write
    original_finish = server.Request.finish

    def write_spy(request, data):
        writes.append((request, data))
        return original_write(request, data)

    def finish_spy(request):
        finishes.append(request)
        return original_finish(request)

    monkeypatch.setattr(server.Request, "write", write_spy)
    monkeypatch.setattr(server.Request, "finish", finish_spy)

    return SimpleNamespace(
        managers=created,
        clock=fake_clock,
        writes=writes,
        finishes=finishes,
    )


def crawl_json_requests(requests_):
    return [request for request in requests_ if b"crawl.json" in request.uri]


def get_in_thread(url, params):
    """Perform a blocking GET in a thread; return (thread, result_box)."""
    box: dict[str, Any] = {}

    def _get():
        import requests

        try:
            box["response"] = requests.get(url, params=params, timeout=30)
        except Exception as exc:  # noqa: BLE001
            box["error"] = exc

    thread = threading.Thread(target=_get, daemon=True)
    thread.start()
    return thread, box


class TestHardDeadlineHttp:
    """End-to-end tests through the HTTP API with a controlled clock."""

    def test_hanging_download_returns_504_and_stops_crawl(
        self,
        live_reactor,
        http_sites,
        project,
        harness,
    ):
        handlers_before = len(logging.root.handlers)
        thread, box = get_in_thread(
            f"{http_sites.api_url}/crawl.json",
            {"spider_name": "test", "url": f"{http_sites.page_url}/hang"},
        )
        try:
            # The download is in flight and hangs; no new requests will
            # ever be scheduled, yet the hard deadline must fire.
            assert http_sites.hang.requested.wait(10)
            assert wait_until(lambda: len(harness.managers) == 1, timeout=5)
            manager = harness.managers[0]
            blockingCallFromThread(
                live_reactor,
                harness.clock.advance,
                manager.timeout_limit + 1,
            )
            thread.join(10)
        finally:
            thread.join(10)
        assert "error" not in box
        response = box["response"]
        # The existing error format is used, with the 504 status code.
        assert response.status_code == 504
        data = response.json()
        assert data["status"] == "error"
        assert data["code"] == 504
        assert "timeout" in data["message"].lower()
        # The response was written exactly once.
        assert len(crawl_json_requests(harness.finishes)) == 1
        # The crawl was stopped.
        assert manager._terminal_state == "timeout"
        assert manager._crawl_done
        assert manager.crawler.stats.get_value("finish_reason") == "timeout"
        # Resources were released: the timer, the spider log handler.
        assert manager._timeout_call is None
        assert not harness.clock.getDelayedCalls()
        assert manager._cleanup_handler is None
        assert len(logging.root.handlers) == handlers_before
        # A later clock advance changes nothing: the first terminal
        # state won the race.
        blockingCallFromThread(
            live_reactor,
            harness.clock.advance,
            manager.timeout_limit + 100,
        )
        assert manager._terminal_state == "timeout"
        assert len(crawl_json_requests(harness.finishes)) == 1

    def test_normal_completion_unchanged(
        self,
        live_reactor,
        http_sites,
        project,
        harness,
    ):
        import requests

        handlers_before = len(logging.root.handlers)
        response = requests.get(
            f"{http_sites.api_url}/crawl.json",
            params={"spider_name": "test", "url": f"{http_sites.page_url}/ok"},
            timeout=30,
        )
        assert response.status_code == 200
        data = response.json()
        # The normal JSON structure is unchanged.
        assert data["status"] == "ok"
        assert data["items"] == [{"name": ["Page 1"]}]
        assert data["items_dropped"] == []
        assert data["spider_name"] == "test"
        assert isinstance(data["stats"], dict)
        assert data["stats"]["finish_reason"] == "finished"
        assert wait_until(lambda: len(harness.managers) == 1, timeout=5)
        manager = harness.managers[0]
        # The timeout was released and the log resources cleaned up.
        assert manager._terminal_state == "finished"
        assert manager._timeout_call is None
        assert not harness.clock.getDelayedCalls()
        assert manager._cleanup_handler is None
        assert len(logging.root.handlers) == handlers_before
        assert len(crawl_json_requests(harness.finishes)) == 1
        # A timeout firing after completion must not change anything:
        # completion was the first terminal state.
        blockingCallFromThread(
            live_reactor,
            harness.clock.advance,
            manager.timeout_limit + 1,
        )
        assert manager._terminal_state == "finished"
        assert manager.user_error is None
        assert len(crawl_json_requests(harness.finishes)) == 1

    # get_item() calls the deprecated engine.close_spider(); the warning is
    # pre-existing behavior, unrelated to the hard deadline.
    @pytest.mark.filterwarnings(
        "ignore::scrapy.exceptions.ScrapyDeprecationWarning",
    )
    def test_max_items_behavior_unchanged(
        self,
        http_sites,
        project,
        harness,
    ):
        import requests

        params: dict[str, Any] = {
            "spider_name": "test",
            "url": f"{http_sites.page_url}/ok",
            "callback": "return_many",
            "max_items": 3,
        }
        response = requests.get(
            f"{http_sites.api_url}/crawl.json",
            params=params,
            timeout=30,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["items"] == [{"name": [str(i)]} for i in range(3)]
        assert data["stats"]["finish_reason"] == (
            "stop generating items, only 3 items allowed"
        )

    def test_client_disconnect_cancels_crawl(
        self,
        live_reactor,
        http_sites,
        project,
        harness,
    ):
        handlers_before = len(logging.root.handlers)
        query = urlencode(
            {"spider_name": "test", "url": f"{http_sites.page_url}/hang"},
        )
        conn = http.client.HTTPConnection("127.0.0.1", http_sites.api_port)
        conn.request("GET", f"/crawl.json?{query}")
        # Wait until the crawl is running and stuck on the hanging page.
        assert http_sites.hang.requested.wait(10)
        assert wait_until(lambda: len(harness.managers) == 1, timeout=5)
        manager = harness.managers[0]
        # The client goes away before the response is ready.
        conn.close()
        # The crawl must be cancelled.
        assert wait_until(lambda: manager._crawl_done, timeout=10), (
            "crawl was not cancelled after the client disconnected"
        )
        assert manager._terminal_state == "cancelled"
        assert manager.crawler.stats.get_value("finish_reason") == "cancelled"
        # Resources were released.
        assert manager._timeout_call is None
        assert not harness.clock.getDelayedCalls()
        assert manager._cleanup_handler is None
        assert wait_until(
            lambda: len(logging.root.handlers) == handlers_before,
            timeout=5,
        )
        # Nothing was written to or finished on the disconnected request.
        assert crawl_json_requests(harness.finishes) == []
        crawl_writes = [
            request
            for request, _data in harness.writes
            if b"crawl.json" in request.uri
        ]
        assert crawl_writes == []
        # A later timeout must not change the cancelled crawl: the
        # disconnect was the first terminal state.
        blockingCallFromThread(
            live_reactor,
            harness.clock.advance,
            manager.timeout_limit + 1,
        )
        assert manager._terminal_state == "cancelled"
        assert manager.user_error is None


class TestHardDeadlineManager:
    """Tests for the built-in CrawlManager itself."""

    def test_timeout_stops_hanging_crawl(
        self,
        live_reactor,
        http_sites,
        project,
    ):
        @defer.inlineCallbacks
        def run():
            fake_clock = Clock()
            manager = CrawlManager(
                "test",
                {"url": f"{http_sites.page_url}/hang"},
            )
            manager.clock = fake_clock
            dfd = manager.crawl()
            # Wait until the download of the hanging page has started.
            yield http_sites.hang.wait_request()
            fake_clock.advance(manager.timeout_limit + 1)
            result = yield dfd
            return manager, fake_clock, result

        manager, fake_clock, result = blockingCallFromThread(live_reactor, run)
        # The crawl failed with a 504 error in the usual result structure.
        assert manager._terminal_state == "timeout"
        user_error = result["user_error"]
        assert isinstance(user_error, Error)
        assert user_error.status == b"504"
        message = user_error.message
        assert message is not None
        assert b"timeout" in message.lower()
        # The crawl was stopped and the download was cancelled.
        assert manager._crawl_done
        assert manager.crawler.stats.get_value("finish_reason") == "timeout"
        # Resources were released.
        assert manager._timeout_call is None
        assert not fake_clock.getDelayedCalls()
        assert manager._cleanup_handler is None

    def test_concurrent_crawls_are_independent(
        self,
        live_reactor,
        http_sites,
        project,
    ):
        @defer.inlineCallbacks
        def run():
            clock_one = Clock()
            manager_one = CrawlManager(
                "test",
                {"url": f"{http_sites.page_url}/hang"},
            )
            manager_one.clock = clock_one
            clock_two = Clock()
            manager_two = CrawlManager(
                "test",
                {"url": f"{http_sites.page_url}/ok"},
            )
            manager_two.clock = clock_two
            dfd_one = manager_one.crawl()
            dfd_two = manager_two.crawl()
            # Only the first crawl times out.
            clock_one.advance(manager_one.timeout_limit + 1)
            result_one = yield dfd_one
            result_two = yield dfd_two
            return manager_one, manager_two, result_one, result_two

        manager_one, manager_two, result_one, result_two = blockingCallFromThread(
            live_reactor,
            run,
        )
        assert manager_one._terminal_state == "timeout"
        assert result_one["user_error"].status == b"504"
        # The other crawl completed normally and was not affected.
        assert manager_two._terminal_state == "finished"
        assert result_two["user_error"] is None
        assert result_two["items"] == [{"name": ["Page 1"]}]

    def test_startup_failure_releases_resources(self, project, monkeypatch):
        fake_clock = Clock()
        manager = CrawlManager("test", {"url": "http://127.0.0.1/"})
        manager.clock = fake_clock
        cleanup = Mock()
        manager._cleanup_handler = cleanup

        def failing_crawl(*args, **kwargs):
            raise RuntimeError("startup failed")

        monkeypatch.setattr(ScrapyrtCrawlerRunner, "crawl", failing_crawl)
        with pytest.raises(RuntimeError, match="startup failed"):
            manager.crawl()
        # The timer was cancelled and the log resources were cleaned up.
        assert manager._timeout_call is None
        assert not fake_clock.getDelayedCalls()
        cleanup.assert_called_once_with()
        assert manager._cleanup_handler is None
