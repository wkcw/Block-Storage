import rpyc
import hashlib
import os
import sys
import logging
import hashlib
import time


logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr
)
logger = logging.getLogger(__name__)


"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""

class ServerInfo:
    def __init__(self, host, port):
        self.host = host
        self.port = port


class SurfStoreClient():

    """
    Initialize the client and set up connections to the block stores and
    metadata store using the config file
    """
    def __init__(self, config):
        with open(config, 'r') as f:
            config_file_data = f.read()
            config_file_data_lines = config_file_data.split('\n')
            self._block_server_info_list = []
            self._num_block_stores = int(config_file_data_lines[0].split(': ')[1])
            for i in range(1, self._num_block_stores + 2):
                current_line = config_file_data_lines[i]
                colon_index = current_line.find(': ')
                key = current_line[:colon_index]
                value = current_line[(colon_index + 2):]
                colon_index = value.rfind(':')  # find the last ':' in case there are ':'s in ipv6 address
                server_host = value[:colon_index]
                server_port = int(value[(colon_index + 1):])
                server_info = ServerInfo(server_host, server_port)
                if i == 1:
                    self._metadata_server_info = server_info
                else :
                    self._block_server_info_list.append(server_info)
        self._metadata_server_conn = rpyc.connect(self._metadata_server_info.host, self._metadata_server_info.port)
        self._init_nearest_server()

    def _init_nearest_server(self):
        # set self._nearest_server_id to the nearest server id
        self._nearest_server_id = _block_server_info_list[0]
        shortest_ping_time = 10  # some large number. ping will timeout in 5 seconds.
        for i in range(self._num_block_stores):
            server_info = self._block_server_info_list[i]
            block_server_conn = rpyc.connect(server_info.host, server_info.port)
            check_time = time.time()
            block_server_conn.pint()
            ping_time = time.time() - check_time
            if ping_time < shortest_ping_time:
                shortest_ping_time = ping_time
                self._nearest_server_id = i

    def _find_server(self, h):
        return int(h, 16) % self._num_block_stores

    def _find_server_nearest(self):
        return self._nearest_server_id

    """
    upload(filepath) : Reads the local file, creates a set of
    hashed blocks and uploads them onto the MetadataStore
    (and potentially the BlockStore if they were not already present there).
    """
    def upload(self, filepath):
        if not os.path.isfile(filepath):
            logger.error('{} is not a file on disk'.format(filepath))
            print('Not Found')
            return
        file_block_list = []
        file_block_hash_list = []
        with open(filepath, 'rb') as f:
            while True:
                data_4096 = f.read(4096)
                if not data_4096:
                    break
                file_block_list.append(data_4096)
                file_block_hash_list.append(hashlib.sha256(data_4096).hexdigest())
        block_number = len(file_block_list)
        logger.info('find {} blocks for file {} on local disk'.format(block_number, filepath))
        filename = os.path.basename(filepath)
        version_number, _ = self._metadata_server_conn.root.read_file(filename)
        version_number = version_number + 1
        modify_succeed = False
        while not modify_succeed:
            try:
                self._metadata_server_conn.root.modify_file(filename, version_number, file_block_hash_list)
                modify_succeed = True
                print('OK')
            except Exception as e:
                try:
                    logger.exception(e)
                    error_type = e.error_type
                    if error_type == 1:  # missing_blocks
                        logger.info('need to upload missing blocks')
                        self._upload_missing_blocks(file_block_list, file_block_hash_list, e.missing_blocks)
                    elif error_type == 2:  # version_error
                        logger.info('need to update version number')
                        version_number = e.current_version + 1
                except Exception as e:
                    logger.exception(e)
                    break

    def _upload_missing_blocks(self, file_block_list, file_block_hash_list, missing_blocks):
        server_upload_list = [{} for i in range(self._num_block_stores)]
        for i in range(len(file_block_list)):
            block_hash = file_block_hash_list[i]
            if block_hash in missing_blocks:
                server_id = self._find_server(block_hash)
                server_upload_list[server_id][block_hash] = file_block_list[i]
        for i in range(self._num_block_stores):
            server_i_upload_dict = server_upload_list[i]
            if server_i_upload_dict:
                server_info = self._block_server_info_list[i]
                block_server_conn = rpyc.connect(server_info.host, server_info.port)
                for block_hash in server_i_upload_dict:
                    block_server_conn.root.store_block(block_hash, server_i_upload_dict[block_hash])


    """
    delete(filename) : Signals the MetadataStore to delete a file.
    """
    def delete(self, filename):
        version_number, _ = self._metadata_server_conn.root.read_file(filename)
        delete_succeed = False
        version_number = version_number + 1
        while not delete_succeed:
            try:
                self._metadata_server_conn.root.delete_file(filename, version_number)
                delete_succeed = True
                print('OK')
            except Exception as e:
                try:
                    logger.exception(e)
                    error_type = e.error_type
                    if error_type == 2:  # version_error
                        logger.info('need to update version number')
                        version_number = e.current_version + 1
                    elif error_type == 3:  # file_not_found
                        logger.error('file {} not found on server'.format(filename))
                        print('Not Found')
                        return
                except Exception as e:
                    logger.exception(e)
                    break


    """
        download(filename, dst) : Downloads a file (f) from SurfStore and saves
        it to (dst) folder. Ensures not to download unnecessary blocks.
    """
    def download(self, filename, location):
        _, file_block_hash_list = self._metadata_server_conn.root.read_file(filename)
        if not file_block_hash_list:
            logger.error('{} is not found on server'.format(filename))
            print('Not Found')
            return
        hash_block_dict = {}
        for file_name_in_location in os.listdir(location):
            file_path = os.path.join(location, file_name_in_location)
            if os.path.isfile(file_path):
                try:
                    with open(file_path, 'rb') as f:
                        while True:
                            data_4096 = f.read(4096)
                            if not data_4096:
                                break
                            data_4096_hash_value = hashlib.sha256(data_4096).hexdigest()
                            if data_4096_hash_value in file_block_hash_list:
                                hash_block_dict[data_4096_hash_value] = data_4096
                except Exception as e:
                    logger.exception(e)
        self._download_missing_blocks(file_block_hash_list, hash_block_dict)
        try:
            file_path = os.path.join(location, filename)
            with open(file_path, 'wb') as f:
                for hash_value in file_block_hash_list:
                    f.write(hash_block_dict[hash_value])
            print('OK')
        except Exception as e:
            logger.exception(e)

    def _download_missing_blocks(self, file_block_hash_list, hash_block_dict):
        server_download_list = [[] for i in range(self._num_block_stores)]
        for block_hash in file_block_hash_list:
            if block_hash not in hash_block_dict:
                server_id = self._find_server(block_hash)
                server_download_list[server_id].append(block_hash)
        for i in range(self._num_block_stores):
            server_i_download_list = server_download_list[i]
            if server_i_download_list:
                server_info = self._block_server_info_list[i]
                block_server_conn = rpyc.connect(server_info.host, server_info.port)
                for block_hash in server_i_download_list:
                    hash_block_dict[block_hash] = block_server_conn.root.get_block(block_hash)


    """
     Use eprint to print debug messages to stderr
     E.g -
     self.eprint("This is a debug message")
    """
    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)



if __name__ == '__main__':
    client = SurfStoreClient(sys.argv[1])
    operation = sys.argv[2]
    if operation == 'upload':
        client.upload(sys.argv[3])
    elif operation == 'download':
        client.download(sys.argv[3], sys.argv[4])
    elif operation == 'delete':
        client.delete(sys.argv[3])
    else:
        print("Invalid operation")
