"""PGSync RedisQueue."""

import json
import logging
import os
import typing as t

from redis import Redis
from redis.exceptions import ConnectionError

from .settings import REDIS_READ_CHUNK_SIZE, REDIS_SOCKET_TIMEOUT, REDIS_SSL, REDIS_SSL_CA_CERT_PATH
from .urls import get_redis_url

logger = logging.getLogger(__name__)


class RedisQueue(object):
    """Simple Queue with Redis Backend."""

    def __init__(self, name: str, namespace: str = "queue", **kwargs):
        """Init Simple Queue with Redis Backend."""
        url: str = get_redis_url(**kwargs)
        self.key: str = f"{namespace}:{name}"

        ssl_ca_certs = REDIS_SSL_CA_CERT_PATH
        if ssl_ca_certs and not os.path.exists(ssl_ca_certs):
            raise IOError(
                f'"{ssl_ca_certs}" not found.\n'
                f"Provide a valid file containing SSL certificate "
                f"authority (CA) certificate(s)."
            )
        ssl_options = dict(
            ssl=True,
            ssl_cert_reqs="required",
            ssl_ca_certs=ssl_ca_certs,
        )
        try:
            self.__db: Redis = Redis.from_url(
                url,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
                **ssl_options if REDIS_SSL == True else {},
            )
            self.__db.ping()
        except ConnectionError as e:
            logger.exception(f"Redis server is not running: {e}")
            raise

    @property
    def qsize(self) -> int:
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def pop(self, chunk_size: t.Optional[int] = None) -> t.List[dict]:
        """Remove and return multiple items from the queue."""
        chunk_size = chunk_size or REDIS_READ_CHUNK_SIZE
        if self.qsize > 0:
            pipeline = self.__db.pipeline()
            pipeline.lrange(self.key, 0, chunk_size - 1)
            pipeline.ltrim(self.key, chunk_size, -1)
            items: t.List = pipeline.execute()
            logger.debug(f"pop size: {len(items[0])}")
            return list(map(lambda value: json.loads(value), items[0]))

    def push(self, items: t.List) -> None:
        """Push multiple items onto the queue."""
        self.__db.rpush(self.key, *map(json.dumps, items))

    def delete(self) -> None:
        """Delete all items from the named queue."""
        logger.info(f"Deleting redis key: {self.key}")
        self.__db.delete(self.key)
