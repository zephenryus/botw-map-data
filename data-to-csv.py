import atexit
import os
import struct
import zlib
from datetime import datetime


class HashTable:
    hash_table = {}

    def generate_hash_table(self):
        with open("C:\\botw-data\\src\\extractors\\hashed_names.txt", 'r', encoding='utf-8') as hash_str_file:
            data = hash_str_file.read().split('\n')

            for index in range(len(data)):
                str_hash = str(zlib.crc32(bytearray(data[index], 'utf-8')))
                self.hash_table[str_hash] = data[index]


hash_table = HashTable()
hash_table.generate_hash_table()


class PrOD:
    data = {}

    def __init__(self, path: str):
        # print("Reading PrOD file...")

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

    @staticmethod
    def get_name(prod_file, string_table_offset, cluster_string_offset):
        pos = prod_file.tell()
        prod_file.seek(string_table_offset + cluster_string_offset)
        name = prod_file.read().split(b'\0')[0].decode('utf-8')
        prod_file.seek(pos)
        return name


class NodeType:
    String = 0xa0
    Array = 0xc0
    Dictionary = 0xc1
    StringTable = 0xc2
    Boolean = 0xd0
    Integer = 0xd1
    Float = 0xd2
    Int2 = 0xd3

    Values = [
        0xa0,
        0xd0,
        0xd1,
        0xd2,
        0xd3
    ]

    Reference = [
        0xc0,
        0xc1,
        0xc2
    ]


class BYML:
    node_names_table = []
    strings_table = []
    data_object = []
    hash_table = {}

    def __init__(self, path, temp_data=None):
        # print("Parsing Binary YAML file...")

        filename = os.path.basename(path)
        print("Reading " + filename + "...")

        try:
            file = open(path, 'rb')
            self.data = file.read()
        except PermissionError:
            self.data = temp_data

        global hash_table
        self.hash_table = hash_table.hash_table

        signature = self.data[0x00:0x02]
        version, node_names_table_offset, strings_table_offset, root_node \
            = struct.unpack('>HIII', self.data[0x02:0x10])

        # Check file signature
        if signature != b'BY':
            print('\033[31mQuitting: {0} is not a binary YAML file\033[0m'.format(filename))
            print('\033[31mExpected b\'BY\' but saw {0}\033[0m'.format(signature))
            exit(0)

        # BotW uses version 2 of BYML
        if version != 2:
            print('\033[31mQuitting: {0} is not the correct binary YAML version\033[0m'.format(filename))
            print('\033[31mExpected 2 but saw {0}\033[0m'.format(version))
            exit(0)

        self.node_names_table = self.get_node(node_names_table_offset)

        # Make sure there is a node names table
        if len(self.node_names_table) <= 0:
            print('\033[31mQuitting: {0} does not contain a node name table\033[0m'.format(filename))
            exit(0)

        self.strings_table = self.get_node(strings_table_offset)

        # Make sure there is a strings table
        if len(self.node_names_table) <= 0:
            print('\033[31mQuitting: {0} does not contain a string table\033[0m'.format(filename))
            exit(0)

        # Get hashed names
        # self.get_hash_table()

        self.data_object.append(self.get_node(root_node))

    def get_node(self, pos, node_type=None):
        if node_type is None:
            node_type = struct.unpack('<B', self.data[pos:pos + 0x01])[0]

        if node_type == NodeType.StringTable:
            return self.get_string_table(pos)

        elif node_type == NodeType.Dictionary:
            return self.get_dictionary(pos)

        elif node_type == NodeType.Array:
            return self.get_array(pos)

        elif node_type == NodeType.String:
            return self.strings_table[struct.unpack('>I', self.data[pos:pos + 0x04])[0]]

        elif node_type == NodeType.Boolean:
            boolValue = struct.unpack('>I', self.data[pos:pos + 0x04])[0]
            return True if boolValue == 1 else False

        elif node_type == NodeType.Integer:
            return struct.unpack('>I', self.data[pos:pos + 0x04])[0]

        elif node_type == NodeType.Int2:
            return struct.unpack('>L', self.data[pos:pos + 0x04])[0]

        elif node_type == NodeType.Float:
            return struct.unpack('>f', self.data[pos:pos + 0x04])[0]

    def get_string_table(self, pos):
        table = []
        length = struct.unpack('>H', self.data[pos + 0x02:pos + 0x04])[0]
        next_node = pos + 0x04

        for index in range(0, length):
            string_offset = struct.unpack('>I', self.data[next_node:next_node + 0x04])[0] + pos
            table.append(self.data[string_offset:].split(b'\x00', 1)[0].decode("utf_8", 'strict'))
            next_node += 0x04
        return table

    def get_dictionary(self, pos):
        dictionary = {}

        length = struct.unpack('>H', self.data[pos + 0x02:pos + 0x04])[0]
        next_node = pos + 0x04

        for index in range(0, length):
            node_name = struct.unpack('>I', b'\x00' + self.data[next_node:next_node + 0x03])[0]
            key = self.node_names_table[node_name]
            value_type = struct.unpack('>B', self.data[next_node + 0x03:next_node + 0x04])[0]
            value = 0

            if value_type in NodeType.Values:
                value = self.get_node(next_node + 0x04, value_type)

            elif value_type in NodeType.Reference:
                offset = struct.unpack('>I', self.data[next_node + 0x04:next_node + 0x08])[0]
                value = self.get_node(offset)

            # if key == 'HashId' or key == 'SRTHash':
            if isinstance(value, int):
                value = str(value)
                if value in self.hash_table:
                    print('that value is a hash!')
                    print("matched {0} to {1}".format(value, self.hash_table[value]))
                    value = self.hash_table[value]

            dictionary[key] = value
            next_node += 0x08

        return dictionary

    def get_array(self, pos):
        array = []
        node_types = []
        if struct.unpack('>B', self.data[pos:pos + 0x01])[0] != NodeType.Array:
            return []

        length = struct.unpack('>I', b'\x00' + self.data[pos + 0x01:pos + 0x04])[0]
        next_node = pos + 0x04

        for index in range(0, length):
            node_type = struct.unpack('>B', self.data[next_node:next_node + 0x01])[0]
            node_types.append(node_type)
            next_node += 0x01

        alignment_padding = (0x04 - (pos + 0x04 + length)) % 4
        first_node = pos + 0x04 + length + alignment_padding

        next_node = first_node

        for index in range(0, length):
            if node_types[index] in NodeType.Reference:
                offset = struct.unpack('>I', self.data[next_node:next_node + 0x04])[0]
                node = self.get_node(offset)
                # print(node)
                array.append(node)
            else:
                node = self.get_node(next_node, node_types[index])
                # print(node)
                array.append(node)
            next_node += 0x04

        return array


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
                    self.write_byml(byml.data_object)
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

            if 'x' in data[entry]:
                x = data[entry]['x']
            if 'y' in data[entry]:
                y = data[entry]['y']
            if 'z' in data[entry]:
                z = data[entry]['z']

            self.outfile.write("{0}, {1}, {2}, {3}\n".format(name, x, y, z))

    def write_byml(self, byml):
        for entry in range(len(byml)):
            if 'Objs' in byml[entry]:
                for location in byml[entry]['Objs']:
                    x = None
                    y = None
                    z = None
                    name = None

                    if 'Translate' in location:
                        x = location['Translate'][0]
                        y = location['Translate'][1]
                        z = location['Translate'][2]

                    if 'UnitConfigName' in location:
                        name = location['UnitConfigName']

                    self.outfile.write("{0}, {1}, {2}, {3}\n".format(name, x, y, z))


def main():
    FileCrawler("C:\\botw-data\\decompressed\\content\\Map\\MainField", "output/mainfield.csv")
    FileCrawler("C:\\botw-data\\decompressed\\content\\Pack\\TitleBG\\Map\\MainField", "output/titlebg.csv")
    FileCrawler("C:\\botw-data\\decompressed\\aoc0005000c101c9400\\0010\\Map\\MainField", "output/dlc.csv")


if __name__ == "__main__":
    main()
