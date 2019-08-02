import rpyc
import sys
import logging
from threading import Lock


logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr
)
logger = logging.getLogger(__name__)


mutex = Lock()


'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues -
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''
class ErrorResponse(Exception):
    def __init__(self, message):
        super(ErrorResponse, self).__init__(message)
        self.error = message

    def missing_blocks(self, hashlist):
        self.error_type = 1
        self.missing_blocks = hashlist

    def wrong_version_error(self, version):
        self.error_type = 2
        self.current_version = version

    def file_not_found(self):
        self.error_type = 3



'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''
class MetadataStore(rpyc.Service):



    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """
    def __init__(self, config):
        self._name_file_map = {}
        self._blockstore_conn_list = []
        with open(config, 'r') as f:
            block_line = f.readline()
            self.num_block_stores = int(block_line.split(": ")[1])
            _ = f.readline()
            for i in range(self.num_block_stores):
                blockstore_host_line = f.readline()
                host, port = blockstore_host_line.split(": ")[1].split(":")
                port = int(port)
                new_blockstore_conn = rpyc.connect(host, port)
                self._blockstore_conn_list.append(new_blockstore_conn)


    '''
        ModifyFile(f,v,hl): Modifies file f so that it now contains the
        contents refered to by the hashlist hl.  The version provided, v, must
        be exactly one larger than the current version that the MetadataStore
        maintains.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    '''
        :param filename: uploaded file's name
        :param version: version number from clients
        :return: no return value. If version not valid, raise wrong_version_error. If missing blocks, raise missing_blocks
    '''
    def exposed_modify_file(self, filename, version, hashlist):
        mutex.acquire()
        if filename in self._name_file_map:
            stored_file = self._name_file_map[filename]
        else:
            stored_file = FileMetaData()
            self._name_file_map[filename] = stored_file

        if stored_file.ver + 1 != version:
            version_error_response = ErrorResponse("Requires version {0}.".format(stored_file.ver+1))
            version_error_response.wrong_version_error(stored_file.ver)
            mutex.release()
            raise version_error_response
        ret_hashlist = []
        for new_hashval in hashlist:
            if new_hashval in stored_file.hashlist:
                continue
            else:
                if not self._check_blockstore(new_hashval):
                    ret_hashlist.append(new_hashval)
        if len(ret_hashlist) != 0:
            missing_error_response = ErrorResponse("The file being modified has missing blocks in the block store.")
            missing_error_response.missing_blocks(tuple(ret_hashlist))
            mutex.release()
            raise missing_error_response
        stored_file.hashlist = tuple(hashlist)
        stored_file.tombstone = False
        stored_file.ver = version
        logging.info('modify succeed. version: {}'.format(version))
        logging.debug('current name_file_map = {}'.format(self._name_file_map))
        mutex.release()

    '''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    '''
        :param filename: uploaded file's name
        :param y: version number from clients
        :return: no return value. If file does not exist, raise file_not_found. If version is invalid, raise wrong_version_error
    '''
    def exposed_delete_file(self, filename, version):
        mutex.acquire()
        stored_file = self._name_file_map.get(filename, None)
        if stored_file is None:
            notfound_error_mesaage = ErrorResponse("Deleting file not found.")
            notfound_error_mesaage.file_not_found()
            mutex.release()
            raise notfound_error_mesaage
        if stored_file.ver + 1 != version:
            version_error_response = ErrorResponse("Requires version {0}.".format(stored_file.ver+1))
            version_error_response.wrong_version_error(stored_file.ver)
            mutex.release()
            raise version_error_response
        stored_file.ver = version
        stored_file.tombstone = True
        logger.info('delete succeed. version: {}'.format(version))
        mutex.release()

    '''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    '''
        :param filename: uploaded file's name
        :return: If read succeeded, return (file_version, hashlist)
                 If file does not exist, return (0, [])
                 If file has been deleted, return (file_version, [])
    '''
    def exposed_read_file(self, filename):
        mutex.acquire()
        logger.info('read file {}'.format(filename))
        stored_file = self._name_file_map.get(filename, None)
        if stored_file is None:
            logger.error('file does not exist')
            mutex.release()
            return 0, tuple()
        elif stored_file.tombstone:
            logger.error('file has been deleted')
            version = stored_file.ver
            mutex.release()
            return version, tuple()
        else:
            logger.info('read succeed, version: {}'.format(stored_file.ver))
            version = stored_file.ver
            hashlist = stored_file.hashlist
            mutex.release()
            return version, hashlist


    def _check_blockstore(self, hashval):
        blockstore_no = self._find_server(hashval)
        if not self._blockstore_conn_list[blockstore_no].root.has_block(hashval):
            return False
        return True

    def _find_server(self, h):
        return int(h, 16) % self.num_block_stores


class FileMetaData:

    def __init__(self, ):
        self.ver = 0
        self.hashlist = tuple()
        self.tombstone = False





if __name__ == '__main__':
    with open(sys.argv[1], 'r') as f:
        _ = f.readline()
        meta_info = f.readline()
        host, port = meta_info.split(": ")[1].split(":")
        port = int(port)
    from rpyc.utils.server import ThreadedServer
    server = ThreadedServer(MetadataStore(sys.argv[1]), port = port)
    server.start()
