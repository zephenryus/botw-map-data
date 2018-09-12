import json
import os


def get_unique_names(path, outfile_resource):
    print('Reading {0}...'.format(path))

    with open(path, 'r') as infile:
        data = json.loads(infile.read())

        if isinstance(data, list):
            for index in range(len(data)):
                if isinstance(data[index], dict):
                    if 'Objs' in data[index]:
                        for entry in data[index]['Objs']:
                            outfile_resource.write("{0}\n".format(entry['UnitConfigName']))

        if isinstance(data, dict):
            for entry in data:
                outfile_resource.write("{0}\n".format(entry))


def main():
    for (dirpath, dirnames, filenames) in os.walk("C:\\Users\\zephe\\PycharmProjects\\botw-map-data\\output\\json"):
        directory = dirpath.replace('\\', '/') + '/'

        names = []
        with open('output/unique_names.txt', 'a') as outfile:
            for filename in filenames:
                current_file = directory + filename
                get_unique_names(current_file, outfile)


if __name__ == "__main__":
    main()
