"""Deterministic tests for the hard crawl deadline, cancellation and races.

The tests drive the whole HTTP resource with a fake Twisted request and a
controllable ``twisted.internet.task.Clock``, so no real time passes and no
network is involved. The crawl itself is replaced by a deferred the test fires
when it wants the crawl to finish.
"""

import io
import json
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest
from scrapy.settings import Settings
from twisted.internet.defer import Deferred
from twisted.internet.error import ConnectionLost
from twisted.internet.task import Clock
from twisted.python.failure import Failure
from twisted.web.error import Error
from twisted.web.server import NOT_DONE_YET

from scrapyrt.conf import app_settings
from scrapyrt.core import CrawlManager
from scrapyrt.resources import CrawlResource

from .spiders import MetaSpider

TIMEOUT = 5


class FakeRequest:
    """Minimal stand-in for ``twisted.web.server.Request``."""

    def __init__(self, method=b"GET", body=b""):
        self.method = method
        self.code = 200
        self.args = {}
        self.content = io.BytesIO(body)
        self.written = []
        self.finished = 0
        self._notify_finish = Deferred()

    def notifyFinish(self):
        return self._notify_finish

    def setResponseCode(self, code):
        self.code = int(code)

    def setHeader(self, name, value):
        pass

    def write(self, data):
        self.written.append(data)

    def finish(self):
        self.finished += 1
        if not self._notify_finish.called:
            self._notify_finish.callback(None)

    def disconnect(self):
        assert not self._notify_finish.called, "already finished"
        self._notify_finish.errback(Failure(ConnectionLost("client went away")))


class FakeCrawlerRunner:
    """Runner whose crawl never progresses on its own.

    ``crawl_dfd`` represents the crawl life cycle: tests fire it when they
    want the crawl to be over (or to fail at startup).
    """

    instances: ClassVar[list] = []

    def __init__(self, settings, manager):
        self.manager = manager
        self.spider_loader = MagicMock()
        self.spider_loader.load.return_value = MetaSpider
        self.crawl_dfd = Deferred()
        self.crawl_calls = []
        self.crawler = MagicMock()
        self.crawler.spider = MagicMock()
        self.crawler.engine.close_spider_async.return_value = Deferred()
        self.crawler.stats.get_stats.return_value = {"finish_reason": "finished"}
        FakeCrawlerRunner.instances.append(self)

    def crawl(self, spidercls, *args, **kwargs):
        self.crawl_calls.append((spidercls, args, kwargs))
        self.manager.crawler = self.crawler
        return self.crawl_dfd


@pytest.fixture
def crawl_env(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(app_settings, "TIMEOUT_LIMIT", TIMEOUT)
    with (
        patch("scrapyrt.core.ScrapyrtCrawlerRunner", FakeCrawlerRunner),
        patch.object(CrawlManager, "get_project_settings", lambda self: Settings()),
        patch.object(CrawlManager, "clock", clock),
    ):
        FakeCrawlerRunner.instances.clear()
        yield SimpleNamespace(clock=clock, timeout=TIMEOUT)


def start_get_crawl():
    resource = CrawlResource()
    request = FakeRequest()
    request.args = {
        b"spider_name": [b"meta"],
        b"url": [b"http://localhost/page1.html"],
        # avoid replacing the spider start methods just for the test
        b"spider_start": [b"1"],
    }
    result = resource.render(request)
    assert result == NOT_DONE_YET
    return request, FakeCrawlerRunner.instances[-1]


def response_json(request):
    assert len(request.written) == 1
    return json.loads(request.written[0])


def test_deadline_is_scheduled_from_crawl_start(crawl_env):
    _request, runner = start_get_crawl()
    manager = runner.manager
    assert manager._timeout_call is not None
    assert manager._timeout_call.active()
    assert len(crawl_env.clock.getDelayedCalls()) == 1


def test_timeout_returns_504_and_stops_hanging_crawl(crawl_env):
    request, runner = start_get_crawl()
    manager = runner.manager

    # no request is scheduled and the download hangs: only the clock moves
    crawl_env.clock.advance(crawl_env.timeout)

    assert request.code == 504
    assert response_json(request) == {
        "status": "error",
        "message": "Crawl of spider meta exceeded TIMEOUT_LIMIT of 5 seconds",
        "code": 504,
    }
    assert request.finished == 1
    # "fast" mode drops the in-flight (hanging) downloads
    runner.crawler.engine.close_spider_async.assert_called_once_with(
        reason="timeout",
        mode="fast",
    )
    # deadline is released as soon as it fires
    assert manager._timeout_call is None
    assert crawl_env.clock.getDelayedCalls() == []


def test_timeout_releases_logging_when_crawl_stops(crawl_env):
    request, runner = start_get_crawl()
    manager = runner.manager
    cleanup = MagicMock()
    manager._cleanup_handler = cleanup

    crawl_env.clock.advance(crawl_env.timeout)
    assert request.finished == 1
    # the crawl is still shutting down; the log handler is kept until it ends
    assert cleanup.call_count == 0

    # the engine finishes closing the spider
    runner.crawl_dfd.callback(None)

    assert cleanup.call_count == 1
    assert manager._timeout_call is None
    assert len(request.written) == 1
    assert request.finished == 1


def test_disconnect_cancels_crawl_without_touching_response(crawl_env):
    request, runner = start_get_crawl()
    manager = runner.manager
    cleanup = MagicMock()
    manager._cleanup_handler = cleanup

    request.disconnect()

    assert manager._terminal_state == "cancelled"
    runner.crawler.engine.close_spider_async.assert_called_once_with(
        reason="client disconnected",
        mode="fast",
    )
    assert manager._timeout_call is None
    assert crawl_env.clock.getDelayedCalls() == []
    # nothing must be written to or finished on a disconnected response
    assert request.written == []
    assert request.finished == 0

    # the crawl finishes closing later: still no response at all
    runner.crawl_dfd.callback(None)
    assert cleanup.call_count == 1
    assert request.written == []
    assert request.finished == 0


def test_disconnect_after_timeout_does_not_write_again(crawl_env):
    request, _runner = start_get_crawl()

    crawl_env.clock.advance(crawl_env.timeout)
    assert request.finished == 1

    # a late disconnect cannot reach notifyFinish (the response is finished)
    # and no extra response may be produced
    assert len(request.written) == 1
    assert request.code == 504


def test_normal_completion_keeps_json_structure(crawl_env):
    request, runner = start_get_crawl()
    manager = runner.manager
    cleanup = MagicMock()
    manager._cleanup_handler = cleanup
    assert manager._timeout_call is not None

    runner.crawl_dfd.callback(None)

    assert request.finished == 1
    payload = response_json(request)
    assert payload == {
        "status": "ok",
        "items": [],
        "items_dropped": [],
        "stats": {"finish_reason": "finished"},
        "spider_name": "meta",
    }
    assert cleanup.call_count == 1
    assert manager._timeout_call is None
    assert crawl_env.clock.getDelayedCalls() == []


def test_completion_wins_over_late_timeout(crawl_env):
    request, runner = start_get_crawl()

    runner.crawl_dfd.callback(None)
    assert request.code == 200
    assert request.finished == 1

    crawl_env.clock.advance(crawl_env.timeout)

    assert len(request.written) == 1
    assert request.code == 200
    assert request.finished == 1
    runner.crawler.engine.close_spider_async.assert_not_called()


def test_timeout_wins_over_late_completion(crawl_env):
    request, runner = start_get_crawl()

    crawl_env.clock.advance(crawl_env.timeout)
    assert request.code == 504
    assert request.finished == 1

    runner.crawl_dfd.callback(None)

    assert len(request.written) == 1
    assert request.code == 504
    assert request.finished == 1


def test_timeout_and_disconnect_do_not_duplicate_cancellation(crawl_env):
    request, runner = start_get_crawl()
    manager = runner.manager

    crawl_env.clock.advance(crawl_env.timeout)
    assert manager._terminal_state == "timeout"

    # a client disconnect arriving during the timeout shutdown is ignored:
    # the first terminal state already won
    manager.cancel()

    assert manager._terminal_state == "timeout"
    assert runner.crawler.engine.close_spider_async.call_count == 1
    assert len(request.written) == 1
    assert request.finished == 1


def test_startup_failure_cleans_up_timer_and_logging(crawl_env):
    request, runner = start_get_crawl()
    manager = runner.manager
    cleanup = MagicMock()
    manager._cleanup_handler = cleanup

    runner.crawl_dfd.errback(Failure(RuntimeError("spider could not start")))

    assert cleanup.call_count == 1
    assert manager._timeout_call is None
    assert crawl_env.clock.getDelayedCalls() == []
    assert request.code == 500
    assert request.finished == 1
    assert response_json(request)["status"] == "error"


def test_synchronous_startup_failure_leaves_no_timer(crawl_env):
    class FailingRunner(FakeCrawlerRunner):
        def crawl(self, spidercls, *args, **kwargs):
            raise Error(400, message=b"cannot start")

    resource = CrawlResource()
    request = FakeRequest()
    request.args = {b"spider_name": [b"meta"], b"url": [b"http://localhost"]}

    with patch("scrapyrt.core.ScrapyrtCrawlerRunner", FailingRunner):
        body = resource.render(request)

    assert body == (b'{"status": "error", "message": "cannot start", "code": 400}\n')
    assert request.written == []
    assert crawl_env.clock.getDelayedCalls() == []
    assert FailingRunner.instances[-1].manager._timeout_call is None


def test_immediate_startup_failure_leaves_no_timer(crawl_env):
    """A crawl deferred that is already failed when crawl() returns."""

    class ImmediateFailureRunner(FakeCrawlerRunner):
        def crawl(self, spidercls, *args, **kwargs):
            self.manager.crawler = self.crawler
            self.crawl_dfd.errback(Failure(RuntimeError("boom")))
            return self.crawl_dfd

    resource = CrawlResource()
    request = FakeRequest()
    request.args = {
        b"spider_name": [b"meta"],
        b"url": [b"http://localhost"],
        b"spider_start": [b"1"],
    }

    with patch("scrapyrt.core.ScrapyrtCrawlerRunner", ImmediateFailureRunner):
        assert resource.render(request) == NOT_DONE_YET

    assert crawl_env.clock.getDelayedCalls() == []
    assert ImmediateFailureRunner.instances[-1].manager._timeout_call is None
    assert request.code == 500
    assert request.finished == 1


def test_post_request_disconnect_cancels_crawl(crawl_env):
    body = json.dumps(
        {
            "spider_name": "meta",
            "request": {"url": "http://localhost/page1.html"},
            "spider_start": True,
        },
    ).encode()
    request = FakeRequest(method=b"POST", body=body)
    resource = CrawlResource()
    assert resource.render(request) == NOT_DONE_YET

    request.disconnect()

    runner = FakeCrawlerRunner.instances[-1]
    runner.crawler.engine.close_spider_async.assert_called_once_with(
        reason="client disconnected",
        mode="fast",
    )
    assert request.written == []
    assert request.finished == 0


def test_concurrent_crawls_have_independent_deadlines(crawl_env):
    crawl_env.clock.advance(2)  # t=2
    request_a, runner_a = start_get_crawl()  # deadline at t=7
    crawl_env.clock.advance(3)  # t=5
    request_b, runner_b = start_get_crawl()  # deadline at t=10

    runner_b.crawl_dfd.callback(None)  # crawl B finishes normally
    assert request_b.code == 200
    assert request_b.finished == 1

    crawl_env.clock.advance(2)  # t=7: only crawl A reaches its deadline

    assert request_a.code == 504
    assert request_a.finished == 1
    assert len(request_b.written) == 1
    assert request_b.code == 200
    assert runner_a.crawler.engine.close_spider_async.call_count == 1
    assert runner_b.crawler.engine.close_spider_async.call_count == 0


def test_stop_crawl_falls_back_to_sync_close(crawl_env):
    manager = CrawlManager(
        "meta",
        {"url": "http://localhost"},
        spider_start=True,
    )
    crawler = MagicMock()
    engine = MagicMock(spec=["close_spider"])
    engine.close_spider.return_value = Deferred()
    crawler.engine = engine
    crawler.spider = MagicMock()
    manager.crawler = crawler

    manager._stop_crawl(reason="timeout")

    engine.close_spider.assert_called_once_with(crawler.spider, reason="timeout")


def test_stop_crawl_handles_close_without_fast_mode(crawl_env):
    # Scrapy 2.14-2.19 expose close_spider_async() without a "mode" argument.
    calls = []

    manager = CrawlManager(
        "meta",
        {"url": "http://localhost"},
        spider_start=True,
    )
    crawler = MagicMock()
    engine = MagicMock()
    engine.close_spider_async = lambda *, reason="cancelled": (
        calls.append(reason) or Deferred()
    )
    crawler.engine = engine
    manager.crawler = crawler

    manager._stop_crawl(reason="timeout")

    assert calls == ["timeout"]
