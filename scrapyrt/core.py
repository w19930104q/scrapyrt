from __future__ import annotations

import datetime as dt
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path
from warnings import warn

from scrapy import Spider, signals
from scrapy.crawler import Crawler, CrawlerRunner
from scrapy.exceptions import DontCloseSpider
from scrapy.http import Request
from scrapy.utils.defer import deferred_from_coro
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure
from twisted.web.error import Error

from . import log
from .conf import app_settings
from .conf.spider_settings import get_project_settings, get_scrapyrt_settings
from .log import setup_spider_logging


class ScrapyrtCrawlerRunner(CrawlerRunner):
    def __init__(self, settings, scrapyrt_manager):
        super().__init__(settings)
        self.scrapyrt_manager = scrapyrt_manager

    def create_crawler(
        self,
        crawler_or_spidercls: type[Spider] | str | Crawler,
    ) -> Crawler:
        crawler = super().create_crawler(crawler_or_spidercls)
        self.scrapyrt_manager.crawler = crawler
        crawler.signals.connect(self.scrapyrt_manager.get_item, signals.item_scraped)
        crawler.signals.connect(
            self.scrapyrt_manager.collect_dropped,
            signals.item_dropped,
        )
        crawler.signals.connect(self.scrapyrt_manager.spider_idle, signals.spider_idle)
        crawler.signals.connect(
            self.scrapyrt_manager.handle_spider_error,
            signals.spider_error,
        )
        crawler.signals.connect(
            self.scrapyrt_manager.handle_scheduling,
            signals.request_scheduled,
        )
        crawler.signals.connect(
            self.scrapyrt_manager.read_spider,
            signals.spider_opened,
        )
        return crawler


class CrawlManager:  # pylint: disable=too-many-instance-attributes
    """Runs crawls."""

    # Clock used to schedule the hard crawl deadline. It is kept as ``None``
    # here so that the reactor is imported lazily, and so that tests can
    # replace it with a ``twisted.internet.task.Clock`` to control time.
    clock = None

    def __init__(  # noqa: PLR0913  # pylint: disable=too-many-positional-arguments
        self,
        spider_name,
        request_kwargs,
        max_requests=None,
        start_requests=None,
        spider_start=None,
        max_items=None,
    ):
        self.spider_name = spider_name
        self.log_dir = Path(app_settings.LOG_DIR)
        self.items = []
        self.items_dropped = []
        self.errors = []
        self.user_error = None
        self.max_items = int(max_items) if max_items else None
        self.max_requests = int(max_requests) if max_requests else None
        self.timeout_limit = int(app_settings.TIMEOUT_LIMIT)
        self.request_count = 0
        self.debug = app_settings.DEBUG
        self.crawler_runner = None
        self.crawler = None
        # Only the first terminal state (finished, timeout or client
        # disconnect) is allowed to take effect.
        self._terminal_state = None
        self._timeout_call = None
        self._result = None
        # callback will be added after instantiation of crawler object
        # because we need to know if spider has method available
        self.callback_name = request_kwargs.pop("callback", None) or "parse"
        # do the same for errback
        self.errback_name = (
            request_kwargs.pop("errback", None) or app_settings.DEFAULT_ERRBACK_NAME
        )

        if request_kwargs.get("url"):
            self.request = self.create_spider_request(deepcopy(request_kwargs))
        else:
            self.request = None
        self._request_scheduled = False
        self.original_start_methods = {}
        self._cleanup_handler = None
        self._init_spider_start(start_requests, spider_start)

    def _init_spider_start(self, start_requests, spider_start):
        if start_requests is not None:
            warn(
                "The start_requests parameter of CrawlManager() is "
                "deprecated, use spider_start instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if spider_start is None:
                spider_start = start_requests
        if spider_start is None:
            self.spider_start = False
        else:
            self.spider_start = spider_start

    @property
    def start_requests(self):
        warn(
            "CrawlManager.start_requests is deprecated, use "
            "CrawlManager.spider_start instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.spider_start

    @start_requests.setter
    def start_requests(self, value: bool):
        warn(
            "CrawlManager.start_requests is deprecated, use "
            "CrawlManager.spider_start instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.spider_start = value

    def crawl(self, *args, **kwargs):
        settings = self.get_project_settings()
        self.crawler_runner = ScrapyrtCrawlerRunner(settings, self)
        spidercls = self.crawler_runner.spider_loader.load(self.spider_name)
        for kw in kwargs:
            attr_or_m = getattr(spidercls, kw, None)
            if attr_or_m and callable(attr_or_m):
                msg = "Crawl argument cannot override spider method."
                msg += " Got argument {} that overrides spider method {}"
                raise Error(
                    400,
                    message=msg.format(kw, getattr(spidercls, kw)).encode(),
                )
        if not self.spider_start:
            self.set_dummy_start_methods(spidercls)
        self._result = Deferred()
        dfd = self.crawler_runner.crawl(spidercls, *args, **kwargs)

        def cleanup_logging(result):
            if self._cleanup_handler:
                self._cleanup_handler()
            return result

        dfd.addBoth(self.restore_start_methods)
        dfd.addBoth(cleanup_logging)
        dfd.addBoth(self._cancel_timeout)
        dfd.addCallback(self.return_items)
        dfd.addBoth(self._on_crawl_done)
        self._start_timeout()
        if self._terminal_state is not None:
            # The crawl already ended while the chain above was being
            # assembled (e.g. it failed before the deadline was scheduled);
            # don't leave a deadline behind.
            self._cancel_timeout()
        return self._result

    def _start_timeout(self):
        """Schedule a hard deadline for the whole crawl.

        Unlike the old behaviour, the deadline is measured from the moment the
        crawl starts and does not depend on new requests being scheduled, so a
        stuck download still triggers it.
        """
        clock = self.clock
        if clock is None:
            # Imported lazily, the reactor must be installed before importing
            # ScrapyRT.
            from twisted.internet import (
                reactor,  # pylint: disable=import-outside-toplevel
            )

            clock = reactor
        self._timeout_call = clock.callLater(self.timeout_limit, self._on_timeout)

    def _cancel_timeout(self, result=None):
        """Cancel the pending deadline, if any. Leaves ``result`` untouched."""
        if self._timeout_call is not None:
            if self._timeout_call.active():
                self._timeout_call.cancel()
            self._timeout_call = None
        return result

    def _set_terminal_state(self, state):
        """Register the first terminal state and return whether it won."""
        if self._terminal_state is not None:
            return False
        self._terminal_state = state
        self._cancel_timeout()
        return True

    def _on_timeout(self):
        self._timeout_call = None
        if not self._set_terminal_state("timeout"):
            return
        message = (
            f"Crawl of spider {self.spider_name} exceeded TIMEOUT_LIMIT "
            f"of {self.timeout_limit} seconds"
        )
        log.msg(message, level=log.WARNING)
        self._stop_crawl(reason="timeout")
        self._result.errback(Failure(Error(504, message=message.encode())))

    def cancel(self, reason="client disconnected"):
        """Cancel the crawl because the client is gone.

        The response cannot be delivered anymore, so the crawl result is not
        fired at all: only the crawl itself is stopped and cleaned up.
        """
        if not self._set_terminal_state("cancelled"):
            return
        log.msg(f"Cancelling crawl of spider {self.spider_name}: {reason}")
        self._stop_crawl(reason=reason)

    def _stop_crawl(self, reason):
        """Stop the crawl and cancel in-flight downloads when supported."""
        if self.crawler is None or self.crawler.engine is None:
            return
        engine = self.crawler.engine
        close_async = getattr(engine, "close_spider_async", None)
        if close_async is not None:
            try:
                # Scrapy >= 2.20: "fast" mode drops the pending downloads,
                # which is what makes a crawl stuck on a hanging download
                # actually terminate.
                dfd = deferred_from_coro(close_async(reason=reason, mode="fast"))
            except TypeError:
                # Older Scrapy versions close the spider gracefully, waiting
                # for the in-flight downloads to finish.
                dfd = deferred_from_coro(close_async(reason=reason))
        else:
            spider = self.crawler.spider
            if spider is None:
                return
            dfd = engine.close_spider(spider, reason=reason)
        dfd.addErrback(self._ignore_stop_error)

    @staticmethod
    def _ignore_stop_error(failure):
        # The spider may still be opening (or the engine may be gone), in
        # which case there is nothing to close.
        failure.trap(RuntimeError)

    def _on_crawl_done(self, result):
        """Deliver the crawl result unless another terminal state won."""
        if not self._set_terminal_state("finished"):
            # Timeout or client disconnect already won; consume the late
            # result so that it isn't reported as an unhandled error.
            return
        if isinstance(result, Failure):
            self._result.errback(result)
        else:
            self._result.callback(result)
        return

    def set_dummy_start_methods(self, spidercls):
        if hasattr(spidercls, "start"):

            async def dummy_start(*_args, **_kwargs):
                return
                yield

            self.original_start_methods["start"] = spidercls.start
            spidercls.start = dummy_start
        if hasattr(spidercls, "start_requests"):

            def dummy_start_requests(*_args, **_kwargs):
                return
                yield

            self.original_start_methods["start_requests"] = spidercls.start_requests
            spidercls.start_requests = dummy_start_requests

    def restore_start_methods(self, result):
        assert self.crawler is not None
        for method_name in list(self.original_start_methods):
            original_method = self.original_start_methods.pop(method_name)
            setattr(self.crawler.spider.__class__, method_name, original_method)
        return result

    def read_spider(self, spider):
        self._cleanup_handler = setup_spider_logging(spider, spider.settings)

    def _get_log_file_path(self):
        log_dir = self.log_dir / self.spider_name
        if not log_dir.exists():
            log_dir.mkdir(parents=True, exist_ok=True)
        time_format = app_settings.SPIDER_LOG_FILE_TIMEFORMAT
        filename = dt.datetime.now().strftime(time_format) + ".log"
        return log_dir / filename

    def get_project_settings(self):
        # set logfile for a job
        log_file = self._get_log_file_path()
        custom_settings = get_scrapyrt_settings(log_file=log_file)
        return get_project_settings(custom_settings=custom_settings)

    def spider_idle(self, spider):
        """Handler of spider_idle signal.

        Schedule request for url given to api, with optional callback and
        errback that can be passed as GET parameter.

        spider_idle signal is used because we want to optionally enable start()
        or start_requests() for the spider and if request is scheduled in
        spider_opened signal handler it's fired earlier than start() or
        start_requests() which is totally wrong.

        """
        assert self.crawler is not None
        if (
            spider is self.crawler.spider
            and self.request
            and not self._request_scheduled
        ):
            try:
                callback = getattr(self.crawler.spider, self.callback_name)
                assert callable(callback), "Invalid callback"
                self.request = self.request.replace(callback=callback)
            except (AssertionError, AttributeError):
                msg = f"Invalid spider callback {self.callback_name}, callback not callable or not a method of a spider {self.spider_name}".encode()
                self.user_error = Error(400, message=msg)
            try:
                if self.errback_name:
                    errback = getattr(self.crawler.spider, self.errback_name)
                    assert callable(errback), "Invalid errback"
                    self.request = self.request.replace(errback=errback)
            except (AssertionError, AttributeError):
                msg = f"Invalid spider errback {self.errback_name}, errback not callable or not a method of a spider {self.spider_name}".encode()
                self.user_error = Error(400, message=msg)
            if self.user_error:
                log.msg(self.user_error.message, level=log.ERROR)
                return

            modify_request = getattr(
                self.crawler.spider,
                "modify_realtime_request",
                None,
            )
            if callable(modify_request):
                self.request = modify_request(self.request)

            spider.crawler.engine.crawl(self.request)
            self._request_scheduled = True
            raise DontCloseSpider

    def handle_scheduling(self, request, spider):  # pylint: disable=unused-argument
        """Handler of request_scheduled signal.

        For every scheduled request check if number of requests is less
        then limit.

        The crawl runtime is not checked here: ``CrawlManager`` enforces it
        with a hard deadline scheduled when the crawl starts, so a crawl with
        no new requests (e.g. one stuck downloading) still times out.

        """
        assert self.crawler is not None
        if spider is self.crawler.spider:
            self.limit_requests(spider)

    def limit_requests(self, spider):
        """Stop crawl after reaching max_requests."""
        if self.max_requests and self.max_requests <= self.request_count:
            reason = (
                f"stop generating requests, only {self.max_requests} requests allowed"
            )
            spider.crawler.engine.close_spider(spider, reason=reason)
        else:
            self.request_count += 1

    def handle_spider_error(self, failure, spider):
        assert self.crawler is not None
        if spider is self.crawler.spider and self.debug:
            fail_data = failure.getTraceback()
            self.errors.append(fail_data)

    def get_item(self, item, response, spider):  # pylint: disable=unused-argument
        assert self.crawler is not None
        if spider is not self.crawler.spider:
            return
        if self.max_items and len(self.items) >= self.max_items:
            # Items scraped while the spider is closing are discarded.
            return
        self.items.append(item)
        if self.max_items and len(self.items) >= self.max_items:
            reason = f"stop generating items, only {self.max_items} items allowed"
            spider.crawler.engine.close_spider(spider, reason=reason)

    def collect_dropped(self, item, response, exception, spider):
        assert self.crawler is not None
        if spider is self.crawler.spider:
            self.items_dropped.append(
                {"item": item, "exception": str(exception), "response": response},
            )

    def return_items(self, result):  # pylint: disable=unused-argument
        assert self.crawler is not None
        stats = self.crawler.stats.get_stats()
        stats = OrderedDict((k, v) for k, v in sorted(stats.items()))
        results = {
            "items": self.items,
            "items_dropped": self.items_dropped,
            "stats": stats,
            "spider_name": self.spider_name,
        }

        results["user_error"] = self.user_error

        if self.debug:
            results["errors"] = self.errors
        return results

    def create_spider_request(self, kwargs):
        url = kwargs.pop("url")
        try:
            req = Request(url, **kwargs)
        except (TypeError, ValueError) as e:
            message = f"Error while creating Scrapy Request, {e}".encode()
            raise Error(400, message=message) from e

        req.dont_filter = True
        msg = "Created request for spider {} with url {} and kwargs {}"
        msg = msg.format(self.spider_name, url, repr(kwargs))
        log.msg(msg)
        return req
