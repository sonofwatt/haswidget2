import asyncio
import logging
import time

import aiohttp

from .exceptions import SwidgetException

_LOGGER = logging.getLogger(__name__)

# Reconnect delays: 5 s after a drop, doubling on each failed attempt, capped so a
# rebooted device is picked up again within a minute.
RETRY_DELAY_MIN = 5
RETRY_DELAY_MAX = 60

STATE_CONNECTED = "connected"
STATE_DISCONNECTED = "disconnected"
STATE_STARTING = "starting"
STATE_STOPPED = "stopped"


class SwidgetWebsocket:
    """Represent a websocket connection to a Swidget Device"""

    # pylint: disable=too-many-instance-attributes

    def __init__(
        self,
        host,
        secret_key,
        callback,
        session=None,
        verify_ssl=False,
        connection_callback=None,
    ):

        self.session = session or aiohttp.ClientSession()
        self.host = host
        self.uri = self._get_uri(host, secret_key)
        self.callback = callback
        self.connection_callback = connection_callback
        self._ssl = False if verify_ssl is False else None
        self._state = None
        self.failed_attempts = 0
        self.ws_client = None
        self._connected_since = None
        self._disconnected_since = None

    @property
    def state(self):
        """Return the current state."""
        return self._state

    @state.setter
    def state(self, value):
        """Set the state."""
        self._state = value

    @property
    def connected(self):
        """Return True while the websocket is open."""
        return self._state == STATE_CONNECTED

    @staticmethod
    def _get_uri(host, secret_key):
        """Generate the websocket URI"""
        return f"wss://{host}/api/v1/sock?x-secret-key={secret_key}"

    async def running(self):
        """Open one websocket connection and act on events until it closes.

        Never logs self.uri or an aiohttp error's string form: both carry the secret key.
        """
        self.state = STATE_STARTING
        was_connected = False

        try:
            headers = {'Connection': 'Upgrade'}
            async with self.session.ws_connect(self.uri, headers=headers, ssl=self._ssl, heartbeat=30) as self.ws_client:
                was_connected = True
                self.failed_attempts = 0
                self._connected_since = time.monotonic()
                if self._disconnected_since is None:
                    _LOGGER.info("Swidget %s: websocket connected", self.host)
                else:
                    _LOGGER.warning(
                        "Swidget %s: websocket reconnected after %.0f s offline",
                        self.host, time.monotonic() - self._disconnected_since,
                    )
                self.state = STATE_CONNECTED
                await self._notify_connection(True)

                async for message in self.ws_client:
                    if self.state == STATE_STOPPED:
                        break

                    if message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            await self.callback(message.json())
                        except Exception:  # pylint: disable=broad-except
                            _LOGGER.warning("Swidget %s: could not process message: %s", self.host, message.data, exc_info=True)

                    elif message.type == aiohttp.WSMsgType.ERROR:
                        _LOGGER.warning("Swidget %s: websocket error: %s", self.host, type(self.ws_client.exception()).__name__)
                        break

                if self.state != STATE_STOPPED:
                    _LOGGER.warning(
                        "Swidget %s: websocket %s (close code %s) after %.0f s connected",
                        self.host,
                        "dropped without a close frame" if self.ws_client.close_code == aiohttp.WSCloseCode.ABNORMAL_CLOSURE else "closed by the device",
                        self.ws_client.close_code,
                        time.monotonic() - self._connected_since,
                    )

        except asyncio.CancelledError:
            raise
        except aiohttp.WSServerHandshakeError as error:
            self.failed_attempts += 1
            if error.status == 401:
                _LOGGER.error("Swidget %s: access key rejected (HTTP 401)", self.host)
            else:
                _LOGGER.warning("Swidget %s: websocket handshake failed (HTTP %s)", self.host, error.status)
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as error:
            self.failed_attempts += 1
            # Log only the first failure of a run, so a long outage is one line
            log = _LOGGER.warning if self.failed_attempts == 1 else _LOGGER.debug
            log("Swidget %s: websocket connection failed (attempt %s): %s",
                self.host, self.failed_attempts, type(error).__name__)
        except Exception:  # pylint: disable=broad-except
            self.failed_attempts += 1
            _LOGGER.exception("Swidget %s: unexpected websocket error", self.host)
        finally:
            self.ws_client = None
            if was_connected:
                self._disconnected_since = time.monotonic()
                if self.state != STATE_STOPPED:
                    self.state = STATE_DISCONNECTED
                await self._notify_connection(False)

        if self.state != STATE_STOPPED:
            self.state = STATE_DISCONNECTED
            retry_delay = min(RETRY_DELAY_MIN * 2 ** max(self.failed_attempts - 1, 0), RETRY_DELAY_MAX)
            await asyncio.sleep(retry_delay)

    async def _notify_connection(self, connected):
        if self.connection_callback is None:
            return
        try:
            await self.connection_callback(connected)
        except Exception:  # pylint: disable=broad-except
            _LOGGER.exception("Swidget %s: connection callback failed", self.host)

    async def send_str(self, message):
        _LOGGER.debug("Swidget %s: sending %s", self.host, message)
        if self.ws_client is None or self.ws_client.closed:
            raise SwidgetException(f"Swidget {self.host} is not connected")
        await self.ws_client.send_str(str(message))

    async def listen(self):
        """Keep a websocket connection open until close() is called."""
        self.failed_attempts = 0
        while self.state != STATE_STOPPED:
            await self.running()

    async def close(self):
        """Close the listening websocket."""
        self.state = STATE_STOPPED
        if self.ws_client is not None and not self.ws_client.closed:
            await self.ws_client.close()
