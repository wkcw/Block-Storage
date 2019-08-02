import rpyc
import sys


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
        self.blockstore_conn_list = []
        with open(config) as f:
            block_line = f.readline()
            self.num_block_stores = int(block_line.split(": ")[1])
            _ = f.readline()
            for i in range(self.num_block_stores):
                blockstore_host_line = f.readline()
                host, port = blockstore_host_line.split(": ")[1].split(":")
                new_blockstore_conn = rpyc.connect(host, port)
                self.blockstore_conn_list.append(new_blockstore_conn)


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
        :return: a tuple (x, y). If x is 0 or 1, y is a list of hashval. If x is 2, y is the demanded version number
    '''
    def exposed_modify_file(self, filename, version, hashlist):
        stored_file = self._name_file_map.get(filename, None)
        if stored_file is None:
            self._name_file_map[filename] = FileMetaData()
            return 0, hashlist
        else:
            try:
                if(stored_file.ver>=version):
                    version_error_response = ErrorResponse("Requires version >= {0}.".format(stored_file.ver+1))
                    version_error_response.wrong_version_error(stored_file.ver)
                    raise version_error_response
                ret_hashlist = []
                for new_hashval in hashlist:
                    if new_hashval in stored_file:
                        continue
                    else:
                        if not self._check_blockstore(new_hashval):
                            ret_hashlist.append(new_hashval)
                if len(ret_hashlist) != 0:
                    missing_error_response = ErrorResponse("The file being modified has missing blocks in the block store.")
                    missing_error_response.missing_blocks(ret_hashlist)
                    raise missing_error_response
            except ErrorResponse as e:
                if e.error_type==1:
                    return e.error_type, e.missing_blocks
                else:
                    return e.error_type, e.current_version
        return (0, None)

    '''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    '''
        :param filename: uploaded file's name
        :param y: version number from clients
        :return: If deletion succeeded, return None, otherwise return error message.
    '''
    def exposed_delete_file(self, filename, version):
        stored_file = self._name_file_map.get(filename, None)
        try:
            if stored_file is None:
                notfound_error_mesaage = ErrorResponse("Deleting file not found.")
                notfound_error_mesaage.file_not_found()
                raise notfound_error_mesaage
            elif stored_file.ver>=version:
                version_error_response = ErrorResponse("File already deleted.")
                raise version_error_response
            else:
                stored_file.tombstone = True
                return None
        except ErrorResponse as e:
            return e.error

    '''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    '''
        :param filename: uploaded file's name
        :return: If read succeeded, return (fileVersion, hashlist), otherwise return (-1, []).
    '''
    def exposed_read_file(self, filename):
        stored_file = self._name_file_map.get(filename, None)
        try:
            if stored_file is None:
                notfound_error_mesaage = ErrorResponse("Requested file not found.")
                notfound_error_mesaage.file_not_found()
                raise notfound_error_mesaage
            else:
                return stored_file.ver, stored_file.hashList
        except ErrorResponse as e:
            return -1, []




    def _check_blockstore(self, hashval):
        blockstore_no = self._find_server(hashval)
        if not self.blockstore_conn_list[blockstore_no].has_block(hashval):
            return False
        return True

    def _find_server(self, h):
        return int(h, 16) % self.num_block_stores


class FileMetaData:

    def __init__(self, ):
        self.ver = 0
        self.hashList = []
        self.tombstone = False





if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer
    server = ThreadPoolServer(MetadataStore(sys.argv[1]), port = 6000)
    server.start()

