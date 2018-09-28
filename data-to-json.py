import codecs
import json
import os
from datetime import datetime

from byml.byaml import BYML
from prod.prod import PrOD


class FileCrawler:
    data = []

    def __init__(self, path: str, path_mask=0):
        self.start_time = datetime.now()
        self.path_mask = path_mask
        self.scan_dir(path)

    def scan_dir(self, path):
        print('Scanning {0}/'.format(path))

        for (dirpath, dirnames, filenames) in os.walk(path):
            directory = dirpath.replace('\\', '/') + '/'

            for filename in filenames:
                current_file = directory + filename
                print('Reading {0}...'.format(current_file))

                signature = self.get_file_signature(current_file)
                data = {}

                if signature == b'PrOD':
                    data = PrOD(current_file)

                if signature == b'BY\x00\x02':
                    buffer = BYML(current_file)
                    data = buffer.data_object

                self.save_as_json(data, "output/json/{0}".format(current_file[self.path_mask:]))

    @staticmethod
    def get_file_signature(path):
        with open(path, 'rb') as infile:
            return infile.read(0x04)

    @staticmethod
    def save_as_json(data, outfile):
        print('Saving {0}.json...'.format(outfile))

        if not os.path.exists(os.path.abspath(os.path.join(outfile, os.pardir))):
            os.makedirs(os.path.abspath(os.path.join(outfile, os.pardir)))

        with codecs.open('{0}.json'.format(outfile), 'w', 'utf-8') as outfile:
            json.dump(data, outfile, ensure_ascii=False, sort_keys=True, indent=4, separators=(',', ': '))


def get_file_signature(path):
    with open(path, 'rb') as infile:
        return infile.read(0x04)


def crawl_file(path, path_mask=0):
    print('Scanning {0}/'.format(path))

    for (dirpath, dirnames, filenames) in os.walk(path):
        directory = dirpath.replace('\\', '/') + '/'

        for filename in filenames:
            current_file = directory + filename
            print('Reading {0}...'.format(current_file))

            signature = get_file_signature(current_file)
            data = []

            if signature == b'PrOD':
                data = PrOD(current_file)

            if signature == b'BY\x00\x02':
                buffer = BYML(current_file)
                data = buffer.data_object

            save_as_json(data, "output/json/{0}".format(current_file[path_mask:]))

def save_as_json(data, outfile):
    print('Saving {0}.json...'.format(outfile))

    if not os.path.exists(os.path.abspath(os.path.join(outfile, os.pardir))):
        os.makedirs(os.path.abspath(os.path.join(outfile, os.pardir)))

    with codecs.open('{0}.json'.format(outfile), 'w', 'utf-8') as outfile:
        json.dump(data, outfile, ensure_ascii=False, sort_keys=True, indent=4, separators=(',', ': '))

def main():
    # C:\botw-data\decompressed\aoc0005000c101c9400\0010\Map\MainField
    crawl_file("C:\\botw-data\\decompressed\\aoc0005000c101c9400\\0010\\Map\\MainField", 46)
    # fc = FileCrawler("C:\\botw-data\\decompressed\\aoc0005000c101c9400\\0010\\Map\\MainField", 46)
    # C:\botw-data\decompressed\content\Map\MainField
    # fc = FileCrawler("C:\\botw-data\\decompressed\\content\\Map\\MainField", 34)
    # C:\botw-data\decompressed\content\Pack\TitleBG\Map\MainField"
    # fc = FileCrawler("C:\\botw-data\\decompressed\\content\\Pack\\TitleBG\\Map\\MainField", 34)


if __name__ == "__main__":
    main()
