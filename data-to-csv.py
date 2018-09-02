import atexit
import os
import struct
from datetime import datetime


class PrOD:
    data = {}

    def __init__(self, path: str):
        print("Reading PrOD file...")

        if os.path.exists(path):
            with open(path, 'rb') as prod_file:
                if prod_file.read(0x04) != b'PrOD':
                    print('\033[31mFile {0} does not appear to be a prod file...\033[0m'.format(path))
                    return

                count, length, filesize, cluster_count, string_table_offset = struct.unpack('>4x5I4x',
                                                                                            prod_file.read(0x1c))

                pos = prod_file.tell()

                while prod_file.tell() < string_table_offset:
                    cluster_size, element_count, cluster_string_offset = struct.unpack('>3I4x', prod_file.read(0x10))

                    name = self.get_name(prod_file, cluster_string_offset, string_table_offset)

                    if prod_file.tell() + 28 < filesize:
                        self.data[name] = {}
                        for element in range(element_count):
                            self.data[name]['x'], self.data[name]['y'], self.data[name]['z'], self.data[name]['rot_x'], \
                            self.data[name]['rot_y'], self.data[name]['rot_z'], self.data[name]['scale '] = \
                                struct.unpack('>7f', prod_file.read(28))

                    pos += cluster_size + 0x10  # cluster size + header size
                    prod_file.seek(pos)

    def get_name(self, prod_file, string_table_offset, cluster_string_offset):
        pos = prod_file.tell()
        prod_file.seek(string_table_offset + cluster_string_offset)
        name = prod_file.read().split(b'\0')[0].decode('utf-8')
        prod_file.seek(pos)
        return name


class BYML:
    NodeType = {
        'String': 0xa0,
        'Array': 0xc0,
        'Dictionary': 0xc1,
        'StringTable': 0xc2,
        'Boolean': 0xd0,
        'Integer': 0xd1,
        'Float': 0xd2,
        'Int2': 0xd3
    }
    data = {}

    def __init__(self, path: str):
        print("Reading BYML file...")

        if os.path.exists(path):
            with open(path, 'rb') as self.byml_file:
                if self.byml_file.read(0x02) != b'BY':
                    print('\033[31mFile {0} does not appear to be a byml file...\033[0m'.format(path))
                    return

                version, node_names_table_offset, strings_table_offset, root_node \
                    = struct.unpack('>H3I', self.byml_file.read(0x0e))
                
                self.node_names_table = self.get_node(node_names_table_offset)

    def get_node(self, pos, node_type=None):
        if node_type is None:
            node_type = struct.unpack('<B', self.byml_file.read(0x01))[0]

        # if node_type == self.NodeType['StringTable']:
        #     return self.get_string_table()


class FileCrawler:
    def __init__(self, path: str, outfilename='outfile.csv'):
        self.start_time = datetime.now()
        self.outfilename = outfilename
        self.on_start()

        self.scan_dir(path)

    def on_start(self):
        print("Scan started at {0}".format(self.start_time.strftime("%Y-%m-%d %H:%M:%S.%f")))
        atexit.register(self.on_stop)
        self.outfile = open(self.outfilename, 'w')

    def on_stop(self):
        end_time = datetime.now()
        delta = end_time - self.start_time
        print("Scan took {0} seconds".format(delta.total_seconds()))

    def scan_dir(self, path):
        print('Scanning...')

        for (dirpath, dirnames, filenames) in os.walk(path):
            dir_path = dirpath.replace('\\', '/') + '/'

            for filename in filenames:
                print('Reading {0}...'.format(dir_path + filename))

                signature = self.get_file_signature(dir_path + filename)

                if signature == b'PrOD':
                    prod = PrOD(dir_path + filename)
                    self.write_prod(prod.data)
                elif signature == b'BY\x00\x02':
                    byml = BYML(dir_path + filename)
                    exit()
                elif signature == b'Yaz0':
                    pass

    def get_file_signature(self, path):
        with open(path, 'rb') as infile:
            return infile.read(0x04)

    def write_prod(self, data):
        for entry in data:
            x = None
            y = None
            z = None
            name = entry
            print(data[entry])

            if 'x' in data[entry]:
                x = data[entry]['x']
            if 'y' in data[entry]:
                y = data[entry]['y']
            if 'z' in data[entry]:
                z = data[entry]['z']

            self.outfile.write("{0}, {1}, {2}, {3}\n".format(name, x, y, z))



def main():
    FileCrawler("C:\\botw-data\\decompressed\\content\\Map\\MainField")


if __name__ == "__main__":
    main()
