""" Kraken exchange subclass """
import logging
from typing import Any, Dict

import ccxt

from freqtrade.exceptions import (DDosProtection, ExchangeError,
                                  InvalidOrderException, OperationalException,
                                  TemporaryError)
from freqtrade.exchange import Exchange
from freqtrade.exchange.common import retrier

logger = logging.getLogger(__name__)


class Phemex(Exchange):

    @retrier_async
    async def _async_get_candle_history(self, pair: str, timeframe: str,
                                        since_ms: Optional[int] = None) -> Tuple[str, str, List]:
        """
        Asynchronously get candle history data using fetch_ohlcv
        returns tuple: (pair, timeframe, ohlcv_list)
        """
        try:
            # Fetch OHLCV asynchronously
            s = '(' + arrow.get(since_ms // 1000).isoformat() + ') ' if since_ms is not None else ''
            limit = 1000
            logger.debug(
                "Fetching pair %s, interval %s, since %s %s, limit %s...",
                pair, timeframe, since_ms, s, limit
            )

            data = await self._api_async.fetch_ohlcv(pair, timeframe=timeframe,
                                                     since=since_ms,
                                                     limit=limit)
            logger.debug(
                "Getting pair %s, interval %s, data len %s",
                pair, timeframe, len(data)
            )
            # Some exchanges sort OHLCV in ASC order and others in DESC.
            # Ex: Bittrex returns the list of OHLCV in ASC order (oldest first, newest last)
            # while GDAX returns the list of OHLCV in DESC order (newest first, oldest last)
            # Only sort if necessary to save computing time
            try:
                if data and data[0][0] > data[-1][0]:
                    data = sorted(data, key=lambda x: x[0])
            except IndexError:
                logger.exception("Error loading %s. Result was %s.", pair, data)
                return pair, timeframe, []
            logger.debug("Done fetching pair %s, interval %s ...", pair, timeframe)
            return pair, timeframe, data

        except ccxt.NotSupported as e:
            raise OperationalException(
                f'Exchange {self._api.name} does not support fetching historical '
                f'candle (OHLCV) data. Message: {e}') from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            raise TemporaryError(f'Could not fetch historical candle (OHLCV) data '
                                 f'for pair {pair} due to {e.__class__.__name__}. '
                                 f'Message: {e}') from e
        except ccxt.BaseError as e:
            raise OperationalException(f'Could not fetch historical candle (OHLCV) data '
                                       f'for pair {pair}. Message: {e}') from e
