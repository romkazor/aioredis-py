import asyncio
import os
import re
import time

import pytest

import aioredis
from aioredis.connection import Connection, to_bool

from .compat import mock
from .conftest import REDIS_6_VERSION, skip_if_server_version_lt
from .test_pubsub import wait_for_message

pytestmark = pytest.mark.asyncio


class DummyConnection(Connection):
    description_format = "DummyConnection<>"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()

    async def connect(self):
        pass

    async def can_read(self, timeout: float = 0):
        return False


class TestConnectionPool:
    def get_pool(
        self,
        connection_kwargs=None,
        max_connections=None,
        connection_class=aioredis.Connection,
    ):
        connection_kwargs = connection_kwargs or {}
        pool = aioredis.ConnectionPool(
            connection_class=connection_class,
            max_connections=max_connections,
            **connection_kwargs
        )
        return pool

    async def test_connection_creation(self):
        connection_kwargs = {"foo": "bar", "biz": "baz"}
        pool = self.get_pool(
            connection_kwargs=connection_kwargs, connection_class=DummyConnection
        )
        connection = await pool.get_connection("_")
        assert isinstance(connection, DummyConnection)
        assert connection.kwargs == connection_kwargs

    async def test_multiple_connections(self, master_host):
        connection_kwargs = {"host": master_host}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = await pool.get_connection("_")
        c2 = await pool.get_connection("_")
        assert c1 != c2

    async def test_max_connections(self, master_host):
        connection_kwargs = {"host": master_host}
        pool = self.get_pool(max_connections=2, connection_kwargs=connection_kwargs)
        await pool.get_connection("_")
        await pool.get_connection("_")
        with pytest.raises(aioredis.ConnectionError):
            await pool.get_connection("_")

    async def test_reuse_previously_released_connection(self, master_host):
        connection_kwargs = {"host": master_host}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = await pool.get_connection("_")
        await pool.release(c1)
        c2 = await pool.get_connection("_")
        assert c1 == c2

    def test_repr_contains_db_info_tcp(self):
        connection_kwargs = {
            "host": "localhost",
            "port": 6379,
            "db": 1,
            "client_name": "test-client",
        }
        pool = self.get_pool(
            connection_kwargs=connection_kwargs, connection_class=aioredis.Connection
        )
        expected = (
            "ConnectionPool<Connection<"
            "host=localhost,port=6379,db=1,client_name=test-client>>"
        )
        assert repr(pool) == expected

    def test_repr_contains_db_info_unix(self):
        connection_kwargs = {"path": "/abc", "db": 1, "client_name": "test-client"}
        pool = self.get_pool(
            connection_kwargs=connection_kwargs,
            connection_class=aioredis.UnixDomainSocketConnection,
        )
        expected = (
            "ConnectionPool<UnixDomainSocketConnection<"
            "path=/abc,db=1,client_name=test-client>>"
        )
        assert repr(pool) == expected


class TestBlockingConnectionPool:
    def get_pool(self, connection_kwargs=None, max_connections=10, timeout=20):
        connection_kwargs = connection_kwargs or {}
        pool = aioredis.BlockingConnectionPool(
            connection_class=DummyConnection,
            max_connections=max_connections,
            timeout=timeout,
            **connection_kwargs
        )
        return pool

    async def test_connection_creation(self, master_host):
        connection_kwargs = {"foo": "bar", "biz": "baz", "host": master_host}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        connection = await pool.get_connection("_")
        assert isinstance(connection, DummyConnection)
        assert connection.kwargs == connection_kwargs

    async def test_multiple_connections(self, master_host):
        connection_kwargs = {"host": master_host}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = await pool.get_connection("_")
        c2 = await pool.get_connection("_")
        assert c1 != c2

    async def test_connection_pool_blocks_until_timeout(self, master_host):
        """When out of connections, block for timeout seconds, then raise"""
        connection_kwargs = {"host": master_host}
        pool = self.get_pool(
            max_connections=1, timeout=0.1, connection_kwargs=connection_kwargs
        )
        await pool.get_connection("_")

        start = time.time()
        with pytest.raises(aioredis.ConnectionError):
            await pool.get_connection("_")
        # we should have waited at least 0.1 seconds
        assert time.time() - start >= 0.1

    async def test_connection_pool_blocks_until_conn_available(self, master_host):
        """
        When out of connections, block until another connection is released
        to the pool
        """
        connection_kwargs = {"host": master_host}
        pool = self.get_pool(
            max_connections=1, timeout=2, connection_kwargs=connection_kwargs
        )
        c1 = await pool.get_connection("_")

        async def target():
            await asyncio.sleep(0.1)
            await pool.release(c1)

        start = time.time()
        await asyncio.gather(target(), pool.get_connection("_"))
        assert time.time() - start >= 0.1

    async def test_reuse_previously_released_connection(self, master_host):
        connection_kwargs = {"host": master_host}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = await pool.get_connection("_")
        await pool.release(c1)
        c2 = await pool.get_connection("_")
        assert c1 == c2

    def test_repr_contains_db_info_tcp(self):
        pool = aioredis.ConnectionPool(
            host="localhost", port=6379, client_name="test-client"
        )
        expected = (
            "ConnectionPool<Connection<"
            "host=localhost,port=6379,db=0,client_name=test-client>>"
        )
        assert repr(pool) == expected

    def test_repr_contains_db_info_unix(self):
        pool = aioredis.ConnectionPool(
            connection_class=aioredis.UnixDomainSocketConnection,
            path="abc",
            client_name="test-client",
        )
        expected = (
            "ConnectionPool<UnixDomainSocketConnection<"
            "path=abc,db=0,client_name=test-client>>"
        )
        assert repr(pool) == expected


class TestConnectionPoolURLParsing:
    def test_hostname(self):
        pool = aioredis.ConnectionPool.from_url("redis://my.host")
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "my.host",
        }

    def test_quoted_hostname(self):
        pool = aioredis.ConnectionPool.from_url("redis://my %2F host %2B%3D+")
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "my / host +=+",
        }

    def test_port(self):
        pool = aioredis.ConnectionPool.from_url("redis://localhost:6380")
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6380,
        }

    @skip_if_server_version_lt(REDIS_6_VERSION)
    def test_username(self):
        pool = aioredis.ConnectionPool.from_url("redis://myuser:@localhost")
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "username": "myuser",
        }

    @skip_if_server_version_lt(REDIS_6_VERSION)
    def test_quoted_username(self):
        pool = aioredis.ConnectionPool.from_url(
            "redis://%2Fmyuser%2F%2B name%3D%24+:@localhost"
        )
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "username": "/myuser/+ name=$+",
        }

    def test_password(self):
        pool = aioredis.ConnectionPool.from_url("redis://:mypassword@localhost")
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "password": "mypassword",
        }

    def test_quoted_password(self):
        pool = aioredis.ConnectionPool.from_url(
            "redis://:%2Fmypass%2F%2B word%3D%24+@localhost"
        )
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "password": "/mypass/+ word=$+",
        }

    @skip_if_server_version_lt(REDIS_6_VERSION)
    def test_username_and_password(self):
        pool = aioredis.ConnectionPool.from_url("redis://myuser:mypass@localhost")
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "username": "myuser",
            "password": "mypass",
        }

    def test_db_as_argument(self):
        pool = aioredis.ConnectionPool.from_url("redis://localhost", db=1)
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "db": 1,
        }

    def test_db_in_path(self):
        pool = aioredis.ConnectionPool.from_url("redis://localhost/2", db=1)
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "db": 2,
        }

    def test_db_in_querystring(self):
        pool = aioredis.ConnectionPool.from_url("redis://localhost/2?db=3", db=1)
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "db": 3,
        }

    def test_extra_typed_querystring_options(self):
        pool = aioredis.ConnectionPool.from_url(
            "redis://localhost/2?socket_timeout=20&socket_connect_timeout=10"
            "&socket_keepalive=&retry_on_timeout=Yes&max_connections=10"
        )

        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "db": 2,
            "socket_timeout": 20.0,
            "socket_connect_timeout": 10.0,
            "retry_on_timeout": True,
        }
        assert pool.max_connections == 10

    def test_boolean_parsing(self):
        for expected, value in (
            (None, None),
            (None, ""),
            (False, 0),
            (False, "0"),
            (False, "f"),
            (False, "F"),
            (False, "False"),
            (False, "n"),
            (False, "N"),
            (False, "No"),
            (True, 1),
            (True, "1"),
            (True, "y"),
            (True, "Y"),
            (True, "Yes"),
        ):
            assert expected is to_bool(value)

    def test_client_name_in_querystring(self):
        pool = aioredis.ConnectionPool.from_url(
            "redis://location?client_name=test-client"
        )
        assert pool.connection_kwargs["client_name"] == "test-client"

    def test_invalid_extra_typed_querystring_options(self):
        with pytest.raises(ValueError):
            aioredis.ConnectionPool.from_url(
                "redis://localhost/2?socket_timeout=_&" "socket_connect_timeout=abc"
            )

    def test_extra_querystring_options(self):
        pool = aioredis.ConnectionPool.from_url("redis://localhost?a=1&b=2")
        assert pool.connection_class == aioredis.Connection
        assert pool.connection_kwargs == {"host": "localhost", "a": "1", "b": "2"}

    def test_calling_from_subclass_returns_correct_instance(self):
        pool = aioredis.BlockingConnectionPool.from_url("redis://localhost")
        assert isinstance(pool, aioredis.BlockingConnectionPool)

    def test_client_creates_connection_pool(self):
        r = aioredis.Redis.from_url("redis://myhost")
        assert r.connection_pool.connection_class == aioredis.Connection
        assert r.connection_pool.connection_kwargs == {
            "host": "myhost",
        }

    def test_invalid_scheme_raises_error(self):
        with pytest.raises(ValueError) as cm:
            aioredis.ConnectionPool.from_url("localhost")
        assert str(cm.value) == (
            "Redis URL must specify one of the following schemes "
            "(redis://, rediss://, unix://)"
        )


class TestConnectionPoolUnixSocketURLParsing:
    def test_defaults(self):
        pool = aioredis.ConnectionPool.from_url("unix:///socket")
        assert pool.connection_class == aioredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
        }

    @skip_if_server_version_lt(REDIS_6_VERSION)
    def test_username(self):
        pool = aioredis.ConnectionPool.from_url("unix://myuser:@/socket")
        assert pool.connection_class == aioredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "username": "myuser",
        }

    @skip_if_server_version_lt(REDIS_6_VERSION)
    def test_quoted_username(self):
        pool = aioredis.ConnectionPool.from_url(
            "unix://%2Fmyuser%2F%2B name%3D%24+:@/socket"
        )
        assert pool.connection_class == aioredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "username": "/myuser/+ name=$+",
        }

    def test_password(self):
        pool = aioredis.ConnectionPool.from_url("unix://:mypassword@/socket")
        assert pool.connection_class == aioredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "password": "mypassword",
        }

    def test_quoted_password(self):
        pool = aioredis.ConnectionPool.from_url(
            "unix://:%2Fmypass%2F%2B word%3D%24+@/socket"
        )
        assert pool.connection_class == aioredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "password": "/mypass/+ word=$+",
        }

    def test_quoted_path(self):
        pool = aioredis.ConnectionPool.from_url(
            "unix://:mypassword@/my%2Fpath%2Fto%2F..%2F+_%2B%3D%24ocket"
        )
        assert pool.connection_class == aioredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/my/path/to/../+_+=$ocket",
            "password": "mypassword",
        }

    def test_db_as_argument(self):
        pool = aioredis.ConnectionPool.from_url("unix:///socket", db=1)
        assert pool.connection_class == aioredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 1,
        }

    def test_db_in_querystring(self):
        pool = aioredis.ConnectionPool.from_url("unix:///socket?db=2", db=1)
        assert pool.connection_class == aioredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 2,
        }

    def test_client_name_in_querystring(self):
        pool = aioredis.ConnectionPool.from_url(
            "redis://location?client_name=test-client"
        )
        assert pool.connection_kwargs["client_name"] == "test-client"

    def test_extra_querystring_options(self):
        pool = aioredis.ConnectionPool.from_url("unix:///socket?a=1&b=2")
        assert pool.connection_class == aioredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {"path": "/socket", "a": "1", "b": "2"}


class TestSSLConnectionURLParsing:
    def test_host(self):
        pool = aioredis.ConnectionPool.from_url("rediss://my.host")
        assert pool.connection_class == aioredis.SSLConnection
        assert pool.connection_kwargs == {
            "host": "my.host",
        }

    def test_cert_reqs_options(self):
        import ssl

        class DummyConnectionPool(aioredis.ConnectionPool):
            def get_connection(self, *args, **kwargs):
                return self.make_connection()

        pool = DummyConnectionPool.from_url("rediss://?ssl_cert_reqs=none")
        assert pool.get_connection("_").cert_reqs == ssl.CERT_NONE

        pool = DummyConnectionPool.from_url("rediss://?ssl_cert_reqs=optional")
        assert pool.get_connection("_").cert_reqs == ssl.CERT_OPTIONAL

        pool = DummyConnectionPool.from_url("rediss://?ssl_cert_reqs=required")
        assert pool.get_connection("_").cert_reqs == ssl.CERT_REQUIRED

        pool = DummyConnectionPool.from_url("rediss://?ssl_check_hostname=False")
        assert pool.get_connection("_").check_hostname is False

        pool = DummyConnectionPool.from_url("rediss://?ssl_check_hostname=True")
        assert pool.get_connection("_").check_hostname is True


class TestConnection:
    async def test_on_connect_error(self):
        """
        An error in Connection.on_connect should disconnect from the server
        see for details: https://github.com/andymccurdy/redis-py/issues/368
        """
        # this assumes the Redis server being tested against doesn't have
        # 9999 databases ;)
        bad_connection = aioredis.Redis(db=9999)
        # an error should be raised on connect
        with pytest.raises(aioredis.RedisError):
            await bad_connection.info()
        pool = bad_connection.connection_pool
        assert len(pool._available_connections) == 1
        assert not pool._available_connections[0]._reader

    @skip_if_server_version_lt("2.8.8")
    async def test_busy_loading_disconnects_socket(self, r):
        """
        If Redis raises a LOADING error, the connection should be
        disconnected and a BusyLoadingError raised
        """
        with pytest.raises(aioredis.BusyLoadingError):
            await r.execute_command("DEBUG", "ERROR", "LOADING fake message")
        if r.connection:
            assert not r.connection._reader

    @skip_if_server_version_lt("2.8.8")
    async def test_busy_loading_from_pipeline_immediate_command(self, r):
        """
        BusyLoadingErrors should raise from Pipelines that execute a
        command immediately, like WATCH does.
        """
        pipe = r.pipeline()
        with pytest.raises(aioredis.BusyLoadingError):
            await pipe.immediate_execute_command(
                "DEBUG", "ERROR", "LOADING fake message"
            )
        pool = r.connection_pool
        assert not pipe.connection
        assert len(pool._available_connections) == 1
        assert not pool._available_connections[0]._reader

    @skip_if_server_version_lt("2.8.8")
    async def test_busy_loading_from_pipeline(self, r):
        """
        BusyLoadingErrors should be raised from a pipeline execution
        regardless of the raise_on_error flag.
        """
        pipe = r.pipeline()
        pipe.execute_command("DEBUG", "ERROR", "LOADING fake message")
        with pytest.raises(aioredis.BusyLoadingError):
            await pipe.execute()
        pool = r.connection_pool
        assert not pipe.connection
        assert len(pool._available_connections) == 1
        assert not pool._available_connections[0]._reader

    @skip_if_server_version_lt("2.8.8")
    async def test_read_only_error(self, r):
        """READONLY errors get turned in ReadOnlyError exceptions"""
        with pytest.raises(aioredis.ReadOnlyError):
            await r.execute_command("DEBUG", "ERROR", "READONLY blah blah")

    def test_connect_from_url_tcp(self):
        connection = aioredis.Redis.from_url("redis://localhost")
        pool = connection.connection_pool

        assert re.match("(.*)<(.*)<(.*)>>", repr(pool)).groups() == (
            "ConnectionPool",
            "Connection",
            "host=localhost,port=6379,db=0",
        )

    def test_connect_from_url_unix(self):
        connection = aioredis.Redis.from_url("unix:///path/to/socket")
        pool = connection.connection_pool

        assert re.match("(.*)<(.*)<(.*)>>", repr(pool)).groups() == (
            "ConnectionPool",
            "UnixDomainSocketConnection",
            "path=/path/to/socket,db=0",
        )

    async def test_connect_no_auth_supplied_when_required(self, r):
        """
        AuthenticationError should be raised when the server requires a
        password but one isn't supplied.
        """
        with pytest.raises(aioredis.AuthenticationError):
            await r.execute_command(
                "DEBUG", "ERROR", "ERR Client sent AUTH, but no password is set"
            )

    async def test_connect_invalid_password_supplied(self, r):
        """AuthenticationError should be raised when sending the wrong password"""
        with pytest.raises(aioredis.AuthenticationError):
            await r.execute_command("DEBUG", "ERROR", "ERR invalid password")


class TestMultiConnectionClient:
    @pytest.fixture()
    async def r(self, create_redis, server):
        redis = await create_redis(single_connection_client=False)
        yield redis
        await redis.flushall()


class TestHealthCheck:
    interval = 60

    @pytest.fixture()
    async def r(self, create_redis):
        redis = await create_redis(health_check_interval=self.interval)
        yield redis
        await redis.flushall()

    def assert_interval_advanced(self, connection):
        diff = connection.next_health_check - time.time()
        assert self.interval > diff > (self.interval - 1)

    async def test_health_check_runs(self, r):
        if r.connection:
            r.connection.next_health_check = time.time() - 1
            await r.connection.check_health()
            self.assert_interval_advanced(r.connection)

    async def test_arbitrary_command_invokes_health_check(self, r):
        # invoke a command to make sure the connection is entirely setup
        if r.connection:
            await r.get("foo")
            r.connection.next_health_check = time.time()
            with mock.patch.object(
                r.connection, "send_command", wraps=r.connection.send_command
            ) as m:
                await r.get("foo")
                m.assert_called_with("PING", check_health=False)

            self.assert_interval_advanced(r.connection)

    async def test_arbitrary_command_advances_next_health_check(self, r):
        if r.connection:
            await r.get("foo")
            next_health_check = r.connection.next_health_check
            await r.get("foo")
            assert next_health_check < r.connection.next_health_check

    async def test_health_check_not_invoked_within_interval(self, r):
        if r.connection:
            await r.get("foo")
            with mock.patch.object(
                r.connection, "send_command", wraps=r.connection.send_command
            ) as m:
                await r.get("foo")
                ping_call_spec = (("PING",), {"check_health": False})
                assert ping_call_spec not in m.call_args_list

    async def test_health_check_in_pipeline(self, r):
        async with r.pipeline(transaction=False) as pipe:
            pipe.connection = await pipe.connection_pool.get_connection("_")
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                responses = await pipe.set("foo", "bar").get("foo").execute()
                m.assert_any_call("PING", check_health=False)
                assert responses == [True, b"bar"]

    async def test_health_check_in_transaction(self, r):
        async with r.pipeline(transaction=True) as pipe:
            pipe.connection = await pipe.connection_pool.get_connection("_")
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                responses = await pipe.set("foo", "bar").get("foo").execute()
                m.assert_any_call("PING", check_health=False)
                assert responses == [True, b"bar"]

    async def test_health_check_in_watched_pipeline(self, r):
        await r.set("foo", "bar")
        async with r.pipeline(transaction=False) as pipe:
            pipe.connection = await pipe.connection_pool.get_connection("_")
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                await pipe.watch("foo")
                # the health check should be called when watching
                m.assert_called_with("PING", check_health=False)
                self.assert_interval_advanced(pipe.connection)
                assert await pipe.get("foo") == b"bar"

                # reset the mock to clear the call list and schedule another
                # health check
                m.reset_mock()
                pipe.connection.next_health_check = 0

                pipe.multi()
                responses = await pipe.set("foo", "not-bar").get("foo").execute()
                assert responses == [True, b"not-bar"]
                m.assert_any_call("PING", check_health=False)

    async def test_health_check_in_pubsub_before_subscribe(self, r):
        """A health check happens before the first [p]subscribe"""
        p = r.pubsub()
        p.connection = await p.connection_pool.get_connection("_")
        p.connection.next_health_check = 0
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            assert not p.subscribed
            await p.subscribe("foo")
            # the connection is not yet in pubsub mode, so the normal
            # ping/pong within connection.send_command should check
            # the health of the connection
            m.assert_any_call("PING", check_health=False)
            self.assert_interval_advanced(p.connection)

            subscribe_message = await wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"

    async def test_health_check_in_pubsub_after_subscribed(self, r):
        """
        Pubsub can handle a new subscribe when it's time to check the
        connection health
        """
        p = r.pubsub()
        p.connection = await p.connection_pool.get_connection("_")
        p.connection.next_health_check = 0
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            await p.subscribe("foo")
            subscribe_message = await wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            self.assert_interval_advanced(p.connection)
            # because we weren't subscribed when sending the subscribe
            # message to 'foo', the connection's standard check_health ran
            # prior to subscribing.
            m.assert_any_call("PING", check_health=False)

            p.connection.next_health_check = 0
            m.reset_mock()

            await p.subscribe("bar")
            # the second subscribe issues exactly only command (the subscribe)
            # and the health check is not invoked
            m.assert_called_once_with("SUBSCRIBE", "bar", check_health=False)

            # since no message has been read since the health check was
            # reset, it should still be 0
            assert p.connection.next_health_check == 0

            subscribe_message = await wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            assert await wait_for_message(p) is None
            # now that the connection is subscribed, the pubsub health
            # check should have taken over and include the HEALTH_CHECK_MESSAGE
            m.assert_any_call("PING", p.HEALTH_CHECK_MESSAGE, check_health=False)
            self.assert_interval_advanced(p.connection)

    async def test_health_check_in_pubsub_poll(self, r):
        """
        Polling a pubsub connection that's subscribed will regularly
        check the connection's health.
        """
        p = r.pubsub()
        p.connection = await p.connection_pool.get_connection("_")
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            await p.subscribe("foo")
            subscribe_message = await wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            self.assert_interval_advanced(p.connection)

            # polling the connection before the health check interval
            # doesn't result in another health check
            m.reset_mock()
            next_health_check = p.connection.next_health_check
            assert await wait_for_message(p) is None
            assert p.connection.next_health_check == next_health_check
            m.assert_not_called()

            # reset the health check and poll again
            # we should not receive a pong message, but the next_health_check
            # should be advanced
            p.connection.next_health_check = 0
            assert await wait_for_message(p) is None
            m.assert_called_with("PING", p.HEALTH_CHECK_MESSAGE, check_health=False)
            self.assert_interval_advanced(p.connection)
