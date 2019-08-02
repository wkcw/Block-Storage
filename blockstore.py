import rpyc
import sys
import logging


logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr
)
logger = logging.getLogger(__name__)


"""
The BlockStore service is an in-memory data store that stores blocks of data,
indexed by the hash value.  Thus it is a key-value store. It supports basic
get() and put() operations. It does not need to support deleting blocks of
data-we just let unused blocks remain in the store. The BlockStore service only
knows about blocks-it doesn't know anything about how blocks relate to files.
"""
class BlockStore(rpyc.Service):


    """
    Initialize any datastructures you may need.
    """
    def __init__(self):
        self._hash_block_map = {}

    """
        store_block(h, b) : Stores block b in the key-value store, indexed by
        hash value h

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    """
    def exposed_store_block(self, h, block):
        logger.info('current size {}, store {}'.format(len(self._hash_block_map), h))
        self._hash_block_map[h] = block


    """
    b = get_block(h) : Retrieves a block indexed by hash value h

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    """
    def exposed_get_block(self, h):
        logger.info('current size {}, get {}'.format(len(self._hash_block_map), h))
        return self._hash_block_map[h]

    """
        rue/False = has_block(h) : Signals whether block indexed by h exists
        in the BlockStore service

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    """
    def exposed_has_block(self, h):
        logger.info('current size {}, check {}'.format(len(self._hash_block_map), h))
        return h in self._hash_block_map

if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer
    port = int(sys.argv[1])
    server = ThreadedServer(BlockStore(), port=port)
    server.start()
