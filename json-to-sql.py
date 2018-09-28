import atexit
import json
import math
import os
import subprocess
from datetime import datetime
import pymysql.cursors
from pymysql import IntegrityError, InternalError


class Markers:
    data = {}

    def __init__(self):
        with open('output/marker-types/markers-renamed.csv', 'r') as infile:
            for line in infile:
                data_buffer = line.split(',')

                for index in range(len(data_buffer)):
                    data_buffer[index] = data_buffer[index].strip().strip('\"')

                self.data[data_buffer[2]] = data_buffer


class ConvertJsonToSQL:
    connection = None
    object_data = {}
    verbose_output = False

    def __init__(self, path: str, refresh_db=False, marker_data=None):
        # self.start_time = datetime.now()
        # self.on_start()
        # self.loadObjectData()

        self.marker_data = marker_data

        if refresh_db:
            self.refresh_database()

        self.database_connect()

        if self.connection.open:
            self.query("ALTER TABLE `markers` ADD UNIQUE `unique_index`(`marker_name`, `marker_type_id`, `x`, `z`);")
            self.convert_file(path)

        else:
            print('\033[31mNot connected to database\033[0m')

    def on_start(self):
        print("Scan started at {0}".format(self.start_time.strftime("%Y-%m-%d %H:%M:%S.%f")))
        atexit.register(self.on_stop)

    def on_stop(self):
        end_time = datetime.now()
        delta = end_time - self.start_time
        print("Scan took {0} seconds".format(delta.total_seconds()))

    def convert_file(self, path, skip_to=0):
        print('Converting {0}...'.format(path))
        lines_read = 0

        with open(path, 'r') as infile:
            data = read_json(json.loads(infile.read()))
            source_file = path.split('/')
            keep = False
            sourcefile = ''
            for index in range(len(source_file)):
                if keep:
                    sourcefile = sourcefile + source_file[index] + '/'

                if source_file[index] == 'json':
                    keep = True

            source_file = sourcefile[:-6]

        for index in range(len(data)):
            lines_read += 1

            if lines_read % 100 == 0:
                print("{0} lines read...".format(lines_read))

            map_region_x = math.floor(float(data[index]['location']['x']) / 1000) + 5
            map_region_z = math.floor(float(data[index]['location']['z']) / 1000) + 4
            map_region = (map_region_z * 10 + map_region_x) + 1

            # Objects placed outside of the playable area
            if map_region < 1:
                map_region = 1
            if map_region > 121:
                map_region = 1

            marker_type = self.query(
                "SELECT * FROM `botwmap`.`marker_types` WHERE `marker_types`.`marker_type_name` = '{0}'".format(
                    data[index]['type']))

            if len(marker_type) == 0:
                name = data[index]['type']
                icon = 'images/icons/markers/default.png'
                description = ''

                if data[index]['type'] in self.marker_data:
                    name = self.marker_data[data[index]['type']][1]
                    icon = self.marker_data[data[index]['type']][3]
                    description = self.marker_data[data[index]['type']][4]

                result = self.insert(
                    "INSERT INTO `botwmap`.`marker_types` (`id`, `marker_type_name`, `marker_type_slug`, `icon`, `marker_type_description`, `created_at`, `updated_at`) VALUES (NULL, '{0}', '{1}', '{2}', '{3}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);".format(
                        name,
                        data[index]['type'],
                        icon,
                        description
                    ))

                marker_type = self.query(
                    "SELECT * FROM `botwmap`.`marker_types` WHERE `marker_types`.`marker_type_slug` = '{0}'".format(
                        data[index]['type']))

            marker_type_id = marker_type[0]['id']

            name = data[index]['name']
            description = data[index]['description']
            x = float(data[index]['location']['x'])
            y = float(data[index]['location']['y'])
            z = float(data[index]['location']['z'])
            # hash = "{0}_Name".format(name)
            # print(hash)
            # if hash in self.object_data:
            #     name = self.object_data[hash]['text']

            self.insert(
                "INSERT INTO `markers` (`id`, `map_region_id`, `marker_type_id`, `marker_name`, `marker_description`, `x`, `y`, `z`, `source`, `created_at`, `updated_at`) VALUES (NULL, '{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);".format(
                    map_region, marker_type_id, name, data[index]['description'], float(data[index]['location']['x']),
                    float(data[index]['location']['y']), float(data[index]['location']['z']), source_file
                ))

        # for line in infile:
        #     lines_read += 1
        #
        #     if lines_read % 100 == 0:
        #         print("{0} lines read...".format(lines_read))
        #
        #     if lines_read < skip_to:
        #         continue
        #
        #     current_line = line.split(',')
        #
        #     for index in range(len(current_line)):
        #         current_line[index] = current_line[index].strip(' \n')
        #
        #     map_region_x = math.floor(float(current_line[1]) / 1000) + 5
        #     map_region_z = math.floor(float(current_line[3]) / 1000) + 4
        #     map_region = (map_region_z * 10 + map_region_x) + 1
        #
        #     # Objects placed outside of the playable area
        #     if map_region < 0:
        #         map_region = 0
        #     if map_region > 80:
        #         map_region = 0
        #
        #     marker_type = self.query(
        #         "SELECT * FROM `marker_types` WHERE `marker_types`.`marker_type_name` = '{0}'".format(
        #             data[index]['type']))
        #
        #     if len(marker_type) == 0:
        #         self.query(
        #             "INSERT INTO `marker_types` (`id`, `marker_type_name`, `marker_type_slug`, `icon`, `marker_type_description`, `created_at`, `updated_at`) VALUES (NULL, '{0}', '{1}', 'images/icons/markers/default.png', '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);".format(
        #                 data[index]['type'],
        #                 data[index]['type']
        #             ))
        #         marker_type = self.query(
        #             "SELECT * FROM `marker_types` WHERE `marker_types`.`marker_type_name` = '{0}'".format(
        #                 data[index]['type']))
        #
        #     marker_type_id = marker_type[0]['id']
        #
        #     name = data[index]['type']
        # hash = "{0}_Name".format(name)
        # print(hash)
        # if hash in self.object_data:
        #     name = self.object_data[hash]['text']

        # self.query(
        #     "INSERT INTO `markers` (`id`, `map_region_id`, `marker_name`, `x`, `y`, `z`, `source`, `marker_type_id`, `marker_category_id`, `marker_sub_category_id`, `created_at`, `updated_at`) VALUES (NULL, '{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);".format(
        #         map_region, name, float(current_line[1]), float(current_line[2]),
        #         float(current_line[3]), current_line[4], marker_type_id, 1, 1))

    def query(self, query: str):
        result = []

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                cursor.execute('COMMIT;')
        except IntegrityError as error:
            if self.verbose_output:
                print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))
            # except pymysql.InternalError as e:
            #     print('\033[31merror!\033[0m')
        except InternalError as error:
            if self.verbose_output:
                print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))

        # finally:
        #     self.connection.close()

        return result

    def insert(self, query: str):
        result = False

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.execute('COMMIT;')
        except IntegrityError as error:
            if self.verbose_output:
                print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))
            # except pymysql.InternalError as e:
            #     print('\033[31merror!\033[0m')
        except InternalError as error:
            if self.verbose_output:
                print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))

        # finally:
        #     self.connection.close()

        return result

    def refresh_database(self) -> None:
        print('Refreshing database...')

        subprocess.run([
            "php",
            "C:\\wamp\\www\\botwmap\\artisan",
            "migrate:refresh"
            # "--seed"
        ])

    def database_connect(self):
        self.connection = pymysql.connect(host='localhost',
                                          user='root',
                                          password='',
                                          db='botwmap',
                                          # charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

    def loadObjectData(self):
        files = [
            "C:\\Users\\zephe\\PycharmProjects\\botw-map-data\\msbt\\object_strings.json",
        ]

        for file in files:
            with open(file, 'r') as infile:
                data = json.loads(infile.read())
                for line in data:
                    self.object_data[line['label']] = {
                        'label': line['label'],
                        'text': line['text']
                    }


def read_json(data):
    return_data = []

    if isinstance(data, list):
        for index in range(len(data)):
            # print("index: {0}".format(data[index]))
            if isinstance(data[index], dict):
                if 'Objs' in data[index]:
                    for entry in range(len(data[index]['Objs'])):
                        if isinstance(data[index]['Objs'][entry], dict):
                            subentry = data[index]['Objs'][entry]
                            # print(subentry)
                            current_entry = {
                                'type': '',
                                'name': '',
                                'description': '',
                                'location': {
                                    'x': 0,
                                    'y': 0,
                                    'z': 0
                                }
                            }

                            if 'UnitConfigName' in subentry:
                                current_entry['type'] = subentry['UnitConfigName']
                                current_entry['name'] = subentry['UnitConfigName']

                            if 'Translate' in subentry:
                                # print("translate: {0}".format(subentry['Translate']))
                                current_entry['location']['x'] = subentry['Translate'][0]
                                current_entry['location']['y'] = subentry['Translate'][1]
                                current_entry['location']['z'] = subentry['Translate'][2]

                            if 'HashId' in subentry:
                                current_entry['description'] = current_entry['description'] + "<HashId: {0}> ".format(
                                    subentry['HashId'].strip('\0\'\"\b\n\r\t\\%<>'))
                                # current_entry['name'] = subentry['UnitConfigName'] + '_' + subentry['HashId']

                            if 'SRTHash' in subentry:
                                current_entry['description'] = current_entry['description'] + "<SRTHash: {0}> ".format(
                                    subentry['SRTHash'].strip('\0\'\"\b\n\r\t\\%<>'))

                            return_data.append(current_entry)

                else:
                    for entry in data[index]:
                        # print("entry: {0}".format(entry))
                        if isinstance(data[index][entry], list):
                            for subentry in data[index][entry]:
                                current_entry = {
                                    'type': entry,
                                    'name': entry,
                                    'description': '',
                                    'location': {
                                        'x': 0,
                                        'y': 0,
                                        'z': 0
                                    }
                                }
                                # print("\nentry: {0}".format(entry))
                                # print("subentry: {0}".format(subentry))
                                if 'Translate' in subentry:
                                    # print("translate: {0}".format(subentry['Translate']))
                                    if 'X' in subentry['Translate']:
                                        current_entry['location']['x'] = subentry['Translate']['X']
                                    if 'Y' in subentry['Translate']:
                                        current_entry['location']['y'] = subentry['Translate']['Y']
                                    if 'Z' in subentry['Translate']:
                                        current_entry['location']['z'] = subentry['Translate']['Z']
                                if 'UniqueName' in subentry:
                                    # print("UniqueName: {0}".format(subentry['UniqueName']))
                                    current_entry['name'] = subentry['UniqueName']

                                return_data.append(current_entry)

    # print(return_data)
    return return_data


def main():
    path = "C:\\Users\\zephe\\PycharmProjects\\botw-map-data\\output\\json"
    refresh_db = True
    markers = Markers()

    for (dirpath, dirnames, filenames) in os.walk(path):
        directory = dirpath.replace('\\', '/') + '/'
        for filename in filenames:
            ConvertJsonToSQL(directory + filename, refresh_db, markers.data)
            refresh_db = False


if __name__ == "__main__":
    main()
