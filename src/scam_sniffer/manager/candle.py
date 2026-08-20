"""Candle synchronization workflow manager."""

from __future__ import annotations

from datetime import UTC, datetime
from collections.abc import Callable

from scam_sniffer.domain.errors import DomainError
from scam_sniffer.domain.models import Candle, Market, Timeframe
from scam_sniffer.domain.repository.candle import CandleRepository

from scam_sniffer.manager.errors import ManagerError, ManagerErrorReason

from scam_sniffer.utils.datetime import now_utc

class CandleManager:
    """Coordinate bounded historical candle synchronization workflows."""

    __BATCH_SIZE = 1_500
    __BACKFILL_SIZE = 10_000

    def __init__(
        self,
        repository: CandleRepository,
        time_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize the manager with a repository and clock.

        Args:
            repository: Domain repository used for synchronization and reads.
            time_provider: Optional timezone-aware clock for deterministic execution.
        """
        self._repository = repository
        self._time_provider = time_provider or now_utc

    async def batch_sync(
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
            return await self._repository.fetch_candles(
                symbol=symbol,
                k_limit=k_limit,
                timeframe=timeframe,
                start_time=start_time,
                finish_time=finish_time,
            )
        except DomainError as error:
            raise ManagerError(
                reason=ManagerErrorReason.REPO,
                message="Candle batch synchronization failed",
                operation="batch_sync",
                root_cause=error,
            ) from error

    async def initial_backfill(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
    ) -> int:
        """Load 10,000 closed candles when a local series is empty.

        Args:
            market: Market-data provider that owns the series.
            symbol: Trading pair symbol.
            timeframe: Domain candle interval.

        Returns:
            Number of synchronized candles, or zero when a closed candle exists.

        Raises:
            ManagerError: If the clock is invalid or repository access fails.
        """
        candle = await self.__latest_cc(
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            operation="initial_backfill",
        )
        if candle is not None:
            return 0

        end_time = _cc_finish_time(
            curr_time=self._time_provider(),
            timeframe=timeframe,
            operation="initial_backfill",
        )
        start_time = end_time - timeframe.duration * self.__BACKFILL_SIZE
        return await self.__range_sync(
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            finish_time=end_time,
        )

    async def tail_gap_recovery(
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
        candle = await self.__latest_cc(
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            operation="tail_gap_recovery",
        )
        if candle is None:
            return 0

        start_time = candle.open_time + timeframe.duration
        finish_time = _cc_finish_time(
            curr_time=self._time_provider(),
            timeframe=timeframe,
            operation="tail_gap_recovery",
        )
        return await self.__range_sync(
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            finish_time=finish_time,
        )

    async def __latest_cc(
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
                reason=ManagerErrorReason.REPO,
                message="Latest closed candle reading failed",
                operation=operation,
                root_cause=error,
            ) from error

    async def __range_sync(
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
                self.__BATCH_SIZE,
                int((finish_time - start_time) // timeframe.duration),
            )
            batch_time = start_time + timeframe.duration * batch_size

            candles = await self.batch_sync(
                symbol=symbol,
                k_limit=batch_size,
                timeframe=timeframe,
                start_time=start_time,
                finish_time=batch_time,
            )
            sync_count += len(candles)
            start_time = batch_time
        return sync_count

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
            reason=ManagerErrorReason.CONF,
            message="Candle manager time must be timezone-aware",
            operation=operation,
        )

    curr_time = curr_time.astimezone(UTC)
    duration_seconds = int(timeframe.duration.total_seconds())
    finish_timestamp = int(curr_time.timestamp()) // duration_seconds * duration_seconds
    return datetime.fromtimestamp(timestamp=finish_timestamp, tz=UTC)
