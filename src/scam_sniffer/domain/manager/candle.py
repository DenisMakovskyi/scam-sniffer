"""Candle synchronization workflow manager."""

from __future__ import annotations

from datetime import UTC, datetime
from itertools import pairwise
from collections.abc import Awaitable, Callable

import asyncio

from scam_sniffer.core.log.logger import get_logger
from scam_sniffer.core.events.proto import EventPublisher

from scam_sniffer.domain.errors import (
    DomainError,
    ManagerError,
    DomainErrorReason,
    ManagerErrorReason,
)
from scam_sniffer.domain.models import Candle, Market, Timeframe
from scam_sniffer.domain.repository.candle import CandleRepository
from scam_sniffer.domain.manager.config import CandleManagerConfig
from scam_sniffer.domain.manager.events import (
    CandleEvent,
    CandleClosed,
    CandlesSynchronized,
)

from scam_sniffer.utils.datetime import now_utc

_LOGGER = get_logger()

class CandleManager:
    """Coordinate historical candle synchronization and live streams."""

    def __init__(
        self,
        config: CandleManagerConfig,
        publisher: EventPublisher[CandleEvent],
        repository: CandleRepository,
        time_provider: Callable[[], datetime] | None = None,
        sleep_provider: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        """Initialize the manager with its dependencies and synchronization limits.

        Args:
            config: Synchronization limits and stream reconnect delays.
            publisher: Event publisher notified after successful persistence.
            repository: Domain repository used for synchronization and reads.
            time_provider: Optional timezone-aware clock for deterministic execution.
            sleep_provider: Optional asynchronous delay provider for reconnects.
        """
        self._config = config
        self._publisher = publisher
        self._repository = repository
        self._time_provider = time_provider or now_utc
        self._sleep_provider = sleep_provider or asyncio.sleep
        self._stream_async_tasks: dict[tuple[str, Timeframe], asyncio.Task[None]] = {}

    async def backfill(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
    ) -> int:
        """Load n-closed candles when a local series is empty.

        Args:
            market: Market-data provider that owns the series.
            symbol: Trading pair symbol.
            timeframe: Domain candle interval.

        Returns:
            Number of synchronized candles, or zero when a closed candle exists.

        Raises:
            ManagerError: If the clock is invalid or repository access fails.
        """
        candle = await self.__get_last_cc(
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            operation="backfill",
        )
        if candle is not None:
            _LOGGER.debug(
                event="Candle backfill skipped - series not empty",
                market=market,
                symbol=symbol,
                timeframe=timeframe,
            )
            return 0

        end_time = _cc_finish_time(
            curr_time=self._time_provider(),
            timeframe=timeframe,
            operation="backfill",
        )
        start_time = end_time - timeframe.duration * self._config.backfill_size
        _LOGGER.info(
            event="Candle backfill started",
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            finish_time=end_time,
            expected_count=self._config.backfill_size,
        )
        sync_count = await self.__time_range_sync(
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            finish_time=end_time,
        )
        if sync_count:
            await self.__publish(
                event=CandlesSynchronized(
                    market=market,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_time=start_time,
                    finish_time=end_time,
                    synchronized_count=sync_count,
                ),
                operation="backfill",
            )
        _LOGGER.info(
            event="Candle backfill completed",
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            synchronized_count=sync_count,
        )
        return sync_count

    async def batch_fetch(
        self,
        symbol: str,
        k_limit: int,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[Candle]:
        """Fetch and persist one bounded candle batch.

        Args:
            symbol: Exchange trading pair symbol.
            k_limit: Maximum number of candles to synchronize.
            timeframe: Domain candle interval.
            start_time: Inclusive range boundary.
            finish_time: Exclusive range boundary.

        Returns:
            Synchronized candles in repository order.

        Raises:
            ManagerError: If repository synchronization fails.
        """
        try:
            candles = await self._repository.fetch_candles(
                symbol=symbol,
                k_limit=k_limit,
                timeframe=timeframe,
                start_time=start_time,
                finish_time=finish_time,
            )
        except DomainError as error:
            raise ManagerError(
                reason=ManagerErrorReason.REPOSITORY,
                message="Candle batch fetch failed",
                operation="batch_fetch",
                root_cause=error,
            ) from error
        _LOGGER.debug(
            event="Candle batch fected",
            symbol=symbol,
            k_limit=k_limit,
            timeframe=timeframe,
            start_time=start_time,
            finish_time=finish_time,
            synchronized_count=len(candles),
        )
        return candles

    async def batch_audit(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
    ) -> int:
        """Find and synchronize internal gaps in recent closed candles.

        The audit is bounded to the n-intervals ending after the latest
        locally persisted closed candle. Only gaps bracketed by closed candles
        are recovered; initial and tail gaps belong to their dedicated flows.

        Args:
            market: Market-data provider that owns the series.
            symbol: Trading pair symbol.
            timeframe: Domain candle interval.

        Returns:
            Number of synchronized candles across all internal gaps.

        Raises:
            ManagerError: If repository access fails or candle spacing is invalid.
        """
        candle = await self.__get_last_cc(
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            operation="batch_audit",
        )
        if candle is None:
            _LOGGER.debug(
                event="Candle batch audit skipped",
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                reason="series_empty",
            )
            return 0

        end_time = candle.open_time + timeframe.duration
        start_time = end_time - timeframe.duration * self._config.backfill_size

        candles = await self.__get_cc_range(
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            finish_time=end_time,
        )
        candles = [*candles, candle]

        gap_ranges = _cc_range_gaps(
            candles=candles,
            timeframe=timeframe,
        )
        sync_count = 0
        for gap_start_time, gap_finish_time in gap_ranges:
            sync_count += await self.__time_range_sync(
                symbol=symbol,
                timeframe=timeframe,
                start_time=gap_start_time,
                finish_time=gap_finish_time,
            )
        if sync_count:
            await self.__publish(
                event=CandlesSynchronized(
                    market=market,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_time=start_time,
                    finish_time=end_time,
                    synchronized_count=sync_count,
                ),
                operation="batch_audit",
            )
        _LOGGER.info(
            "Candle batch audit completed",
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            gap_count=len(gap_ranges),
            synchronized_count=sync_count,
        )
        return sync_count

    async def tail_gap_sync(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
    ) -> int:
        """Synchronize the closed-candle gap after the local series tail.

        Args:
            market: Market-data provider that owns the series.
            symbol: Trading pair symbol.
            timeframe: Domain candle interval.

        Returns:
            Number of synchronized candles, or zero when no tail gap exists.

        Raises:
            ManagerError: If the clock is invalid or repository access fails.
        """
        candle = await self.__get_last_cc(
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            operation="tail_gap_sync",
        )
        if candle is None:
            _LOGGER.debug(
                event="Candle gap synchronization skipped",
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                reason="series_empty",
            )
            return 0

        end_time = _cc_finish_time(
            curr_time=self._time_provider(),
            timeframe=timeframe,
            operation="tail_gap_sync",
        )
        start_time = candle.open_time + timeframe.duration

        sync_count = await self.__time_range_sync(
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            finish_time=end_time,
        )
        if sync_count:
            await self.__publish(
                event=CandlesSynchronized(
                    market=market,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_time=start_time,
                    finish_time=end_time,
                    synchronized_count=sync_count,
                ),
                operation="tail_gap_sync",
            )
        _LOGGER.info(
            event="Candle gap synchronization completed",
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            synchronized_count=sync_count,
        )
        return sync_count

    def start_stream_task(
        self,
        symbol: str,
        timeframe: Timeframe,
    ) -> asyncio.Task[None]:
        """Start or reuse a managed candle stream task.

        Args:
            symbol: Exchange trading pair symbol.
            timeframe: Domain candle interval.

        Returns:
            Active task that owns the stream and its reconnect lifecycle.

        Raises:
            ManagerError: If the symbol is empty or no event loop is running.
        """
        symbol = symbol.upper().strip()
        if not symbol:
            raise ManagerError(
                reason=ManagerErrorReason.PARAMS,
                message="Candle stream symbol cannot be empty",
                operation="start_stream_task",
            )

        stream_key = (symbol, timeframe)
        stream_task = self._stream_async_tasks.get(stream_key)
        if stream_task is not None:
            if not stream_task.done():
                _LOGGER.debug(
                    event="Candle stream task reused",
                    symbol=symbol,
                    timeframe=timeframe,
                )
                return stream_task
            if not stream_task.cancelled():
                task_error = stream_task.exception()
                if isinstance(task_error, ManagerError):
                    raise task_error
                if isinstance(task_error, Exception):
                    raise ManagerError(
                        reason=ManagerErrorReason.LIFECYCLE,
                        message="Candle stream task failed",
                        operation="start_stream_task",
                        root_cause=task_error,
                    ) from task_error
            self._stream_async_tasks.pop(stream_key)

        try:
            event_loop = asyncio.get_running_loop()
        except RuntimeError as error:
            raise ManagerError(
                reason=ManagerErrorReason.LIFECYCLE,
                message="Candle stream requires a running event loop",
                operation="start_stream_task",
                root_cause=error,
            ) from error

        stream_task = event_loop.create_task(
            coro=self.__streaming_loop(symbol=symbol, timeframe=timeframe),
            name=f"candle_stream:{symbol}:{timeframe.value}",
        )
        self._stream_async_tasks[stream_key] = stream_task
        _LOGGER.info(
            event="Candle stream task started",
            symbol=symbol,
            timeframe=timeframe,
        )
        return stream_task

    async def gather_stream_tasks(self) -> None:
        """Cancel and await every managed candle stream.

        Raises:
            ManagerError: If a stream task failed before shutdown.
        """
        tasks = list(self._stream_async_tasks.values())
        self._stream_async_tasks.clear()

        if not tasks:
            _LOGGER.debug(event="Candle stream gathering skipped - no active streams")
            return

        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if result is None or isinstance(result, asyncio.CancelledError):
                continue
            if isinstance(result, ManagerError):
                raise result
            if isinstance(result, Exception):
                raise ManagerError(
                    reason=ManagerErrorReason.LIFECYCLE,
                    message="Candle stream gather failed",
                    operation="gather_stream_tasks",
                    root_cause=result,
                ) from result
        _LOGGER.info(event="Candle stream tasks stopped", stream_count=len(tasks))

    async def __publish(
        self,
        event: CandleEvent,
        operation: str,
    ) -> None:
        """Publish a candle event and translate downstream failures.

        Args:
            event: Persisted candle event ready for downstream processing.
            operation: Manager operation that produced the event.

        Raises:
            ManagerError: If a downstream subscriber cannot process the event.
        """
        try:
            await self._publisher.publish(event)
        except Exception as error:
            raise ManagerError(
                reason=ManagerErrorReason.PUBLISHER,
                message="Candle event publication failed",
                operation=operation,
                root_cause=error,
            ) from error
        _LOGGER.debug(
            event="Candle event published",
            event_type=type(event).__name__,
        )

    async def __get_last_cc(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
        operation: str,
    ) -> Candle | None:
        """Read the latest closed candle and translate repository failures.

        Args:
            market: Market-data provider that owns the series.
            symbol: Trading pair symbol.
            timeframe: Domain candle interval.
            operation: Public manager operation requesting the read.

        Returns:
            Latest closed candle, or ``None`` when none is stored.

        Raises:
            ManagerError: If the repository read fails.
        """
        try:
            return await self._repository.select_latest_closed_candle(
                market=market,
                symbol=symbol,
                timeframe=timeframe,
            )
        except DomainError as error:
            raise ManagerError(
                reason=ManagerErrorReason.REPOSITORY,
                message="Latest closed candle reading failed",
                operation=operation,
                root_cause=error,
            ) from error

    async def __get_cc_range(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[Candle]:
        """Read a candle range and translate repository failures.

        Args:
            market: Market-data provider that owns the series.
            symbol: Trading pair symbol.
            timeframe: Domain candle interval.
            start_time: Inclusive range boundary.
            finish_time: Exclusive range boundary.

        Returns:
            Persisted candles ordered by open time.

        Raises:
            ManagerError: If the repository read fails.
        """
        try:
            return await self._repository.select_candles(
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                start_time=start_time,
                finish_time=finish_time,
            )
        except DomainError as error:
            raise ManagerError(
                reason=ManagerErrorReason.REPOSITORY,
                message="Candle continuity range reading failed",
                operation="batch_audit",
                root_cause=error,
            ) from error

    async def __time_range_sync(
        self,
        symbol: str,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> int:
        """Synchronize a time range in exchange-sized sequential batches.

        Args:
            symbol: Exchange trading pair symbol.
            timeframe: Domain candle interval.
            start_time: Inclusive range boundary.
            finish_time: Exclusive range boundary.

        Returns:
            Total number of candles returned by all batches.

        Raises:
            ManagerError: If any batch synchronization fails.
        """
        sync_count = 0
        while start_time + timeframe.duration <= finish_time:
            batch_size = min(
                self._config.batch_size,
                int((finish_time - start_time) // timeframe.duration),
            )
            batch_time = start_time + timeframe.duration * batch_size

            candles = await self.batch_fetch(
                symbol=symbol,
                k_limit=batch_size,
                timeframe=timeframe,
                start_time=start_time,
                finish_time=batch_time,
            )
            sync_count += len(candles)
            start_time = batch_time
        return sync_count

    async def __streaming_loop(
        self,
        symbol: str,
        timeframe: Timeframe,
    ) -> None:
        """Consume a persisted stream, recover gaps, and reconnect remote failures.

        Args:
            symbol: Exchange trading pair symbol.
            timeframe: Domain candle interval.

        Raises:
            ManagerError: If mapping or storage fails while consuming the stream.
        """
        last_candle: Candle | None = None
        retry_delay = self._config.stream_retry_delay
        while True:
            has_updates = False
            _LOGGER.info(
                event="Candle stream connecting",
                symbol=symbol,
                timeframe=timeframe,
            )
            try:
                async for candle in self._repository.stream_candles(
                    symbol=symbol,
                    timeframe=timeframe,
                ):
                    if not has_updates:
                        _LOGGER.info(
                            event="Candle stream connected",
                            symbol=symbol,
                            timeframe=timeframe,
                        )
                    if last_candle is not None and candle.open_time > last_candle.open_time:
                        start_time = (
                            last_candle.open_time + timeframe.duration
                            if last_candle.is_closed
                            else last_candle.open_time
                        )
                        if start_time < candle.open_time:
                            sync_count = await self.__time_range_sync(
                                symbol=symbol,
                                timeframe=timeframe,
                                start_time=start_time,
                                finish_time=candle.open_time,
                            )
                            if sync_count:
                                await self.__publish(
                                    event=CandlesSynchronized(
                                        market=candle.market,
                                        symbol=symbol,
                                        timeframe=timeframe,
                                        start_time=start_time,
                                        finish_time=candle.open_time,
                                        synchronized_count=sync_count,
                                    ),
                                    operation="streaming_loop",
                                )

                    is_newer = last_candle is None or candle.open_time > last_candle.open_time
                    is_closing = (
                        last_candle is not None
                        and candle.open_time == last_candle.open_time
                        and candle.is_closed
                    )
                    if is_newer or is_closing:
                        last_candle = candle
                    if candle.is_closed:
                        await self.__publish(
                            event=CandleClosed(candle=candle),
                            operation="streaming_loop",
                        )
                    has_updates = True
                    retry_delay = self._config.stream_retry_delay
            except asyncio.CancelledError:
                raise
            except DomainError as error:
                if error.reason is not DomainErrorReason.REMOTE:
                    raise ManagerError(
                        reason=ManagerErrorReason.REPOSITORY,
                        message="Candle stream synchronization failed",
                        operation="streaming_loop",
                        root_cause=error,
                    ) from error

                _LOGGER.warning(
                    event="Candle stream disconnected",
                    reason=error.reason,
                    symbol=symbol,
                    timeframe=timeframe,
                    retry_delay=retry_delay,
                )
            else:
                _LOGGER.warning(
                    event="Candle stream ended",
                    symbol=symbol,
                    timeframe=timeframe,
                    retry_delay=retry_delay,
                )

            await self._sleep_provider(retry_delay)
            if not has_updates:
                retry_delay = min(
                    self._config.stream_retry_max_delay,
                    retry_delay * 2,
                )

def _cc_range_gaps(
    candles: list[Candle],
    timeframe: Timeframe,
) -> list[tuple[datetime, datetime]]:
    """Find internal missing ranges between finalized candles.

    Args:
        candles: Persisted candles from the bounded audit range.
        timeframe: Expected candle interval.

    Returns:
        Half-open missing ranges ordered by start time.

    Raises:
        ManagerError: If adjacent candles are not aligned to the timeframe grid.
    """
    open_times = sorted({candle.open_time for candle in candles if candle.is_closed})
    gap_ranges: list[tuple[datetime, datetime]] = []

    for prev_time, curr_time in pairwise(open_times):
        time_delta = curr_time - prev_time
        if time_delta == timeframe.duration:
            continue
        if time_delta % timeframe.duration:
            raise ManagerError(
                reason=ManagerErrorReason.CONTINUITY,
                message="Closed candles are not aligned to the timeframe grid",
                operation="batch_audit",
            )
        gap_ranges.append((prev_time + timeframe.duration, curr_time))
    return gap_ranges

def _cc_finish_time(
    curr_time: datetime,
    timeframe: Timeframe,
    operation: str,
) -> datetime:
    """Align a clock value to the exclusive end of available closed candles.

    Args:
        curr_time: Current timezone-aware clock value.
        timeframe: Domain candle interval used for alignment.
        operation: Manager operation requesting the boundary.

    Returns:
        UTC timeframe boundary at or before the current time.

    Raises:
        ManagerError: If the current time is timezone-naive.
    """
    if curr_time.tzinfo is None:
        raise ManagerError(
            reason=ManagerErrorReason.PARAMS,
            message="Candle manager time must be timezone-aware",
            operation=operation,
        )

    curr_time = curr_time.astimezone(UTC)
    duration_seconds = int(timeframe.duration.total_seconds())
    finish_timestamp = int(curr_time.timestamp()) // duration_seconds * duration_seconds
    return datetime.fromtimestamp(timestamp=finish_timestamp, tz=UTC)
