import json
import math
import os
import re
import zlib

import pymysql.cursors
from pymysql import IntegrityError, InternalError

source_files_id_cache = [
    ''
]

log_file = open('log.txt', 'w+')


def get_map_region_id(x, z):
    x = math.floor(float(x) / 1000) + 5
    z = math.floor(float(z) / 1000) + 4
    map_region = (z * 10 + x) + 1

    # Objects placed outside of the playable area
    if map_region < 1:
        map_region = 1
    if map_region > 121:
        map_region = 1

    return map_region


def get_marker_icon_id(connection, icon_url):
    icon_id = query(
        connection,
        "SELECT `id` FROM `botwmap`.`marker_icons` WHERE `icon_url` = '{0}'".format(icon_url),
        True
    )

    if len(icon_id) <= 0:
        insert(
            connection,
            "INSERT INTO `botwmap`.`marker_icons` (`id`, `icon_url`, `created_at`, `updated_at`) VALUES (NULL, '{0}', NOW(), NOW());".format(
                icon_url
            ),
            True
        )
        return get_marker_icon_id(connection, icon_url)
    else:
        return icon_id[0]['id']


def get_marker_type_id(connection, marker_type_name, icon_id='NULL', marker_type_description='NULL'):
    marker_type_id = query(
        connection,
        "SELECT `id` FROM `botwmap`.`marker_types` WHERE `marker_type_name` = '{0}'".format(marker_type_name),
        True
    )

    if len(marker_type_id) <= 0:
        insert(
            connection,
            "INSERT INTO `botwmap`.`marker_types` (`id`, `marker_type_name`, `marker_type_slug`, `marker_icon_id`, `marker_type_description`, `created_at`, `updated_at`) VALUES (NULL, '{0}', '{1}', {2}, {3}, NOW(), NOW());".format(
                marker_type_name,
                marker_type_name,
                icon_id,
                marker_type_description
            ),
            True
        )
        return get_marker_type_id(connection, marker_type_name, icon_id, marker_type_description)
    else:
        return marker_type_id[0]['id']


def add_blwp_to_db(connection, path: str):
    with open(path, encoding="utf8") as infile:
        print("Reading {0}...".format(path))
        raw_data = json.loads(infile.read())

        # query(connection, "ALTER TABLE `markers` ADD UNIQUE `unique_index`(`marker_name`, `marker_type_id`, `x`, `z`);")

        source_file = get_source_file_id(connection, get_source_file(path))

        for marker_type in raw_data:
            location = raw_data[marker_type]

            x = float(location['X'])
            y = float(location['Y'])
            z = float(location['Z'])
            map_region_id = get_map_region_id(x, z)
            icon_id = get_marker_icon_id(connection, 'default.png')
            marker_type_id = get_marker_type_id(connection, marker_type, icon_id)
            marker_name = marker_type

            add_marker(
                connection,
                map_region_id,
                marker_type_id,
                source_file,
                0,
                0,
                0,
                marker_name,
                '',
                x,
                y,
                z
            )


def add_mubin_to_db(connection, path: str):
    with open(path, encoding="utf8") as infile:
        print("Reading {0}...".format(path))
        raw_data = json.loads(infile.read(), encoding='utf-8')
        pattern_hash_int = re.compile(r"[\d]+")

        source_file_id = get_source_file_id(connection, get_source_file(path))

        # for data_index in raw_data[0]:
        #     print(data_index)

        # for data_index in raw_data[1]:
        #     print(data_index)

        for marker_data in raw_data['Objs']:
            map_region_id = 1
            marker_type_id = 0
            source_file_id = source_file_id
            has_item = 0
            hash_id = 0
            srt_hash = 0
            marker_name = ''
            marker_description = ''
            x = 0
            y = 0
            z = 0

            map_region_id = get_map_region_id(marker_data['Translate'][0], marker_data['Translate'][2])
            marker_icon_id = get_marker_icon_id(connection, 'default.png')
            marker_type_id = get_marker_type_id(connection, marker_data['UnitConfigName'], marker_icon_id)
            hash_id = marker_data['HashId'] if 'HashId' in marker_data else 0
            srt_hash = marker_data['SRTHash'] if 'SRTHash' in marker_data else 0
            marker_name = marker_data['UnitConfigName'] if 'UnitConfigName' in marker_data else ''
            marker_description = ''
            x = marker_data['Translate'][0] if 'Translate' in marker_data else 0
            y = marker_data['Translate'][1] if 'Translate' in marker_data else 0
            z = marker_data['Translate'][2] if 'Translate' in marker_data else 0

            has_item = 0
            if '!Parameters' in marker_data:
                if 'DropActor' in marker_data['!Parameters']:
                    has_item = 1

            match = pattern_hash_int.search(hash_id)
            if match is not None:
                match = match.group()
                if int(match) > 0:
                    hash_id = int(match)
            else:
                log_file.write("Invalid hash_id: {0}\n".format(marker_data))
                marker_description = "< HashId: {0} >".format(hash_id.replace('%', ''))
                hash_id = zlib.crc32(bytearray(connection.escape_string(hash_id), 'utf-8'))

            add_marker(
                connection,
                map_region_id,
                marker_type_id,
                source_file_id,
                has_item,
                hash_id,
                srt_hash,
                connection.escape_string(marker_name),
                connection.escape_string(marker_description),
                x,
                y,
                z
            )

            marker_id = get_marker_id(connection, map_region_id, marker_type_id, source_file_id, hash_id, srt_hash,
                                marker_name)

            if '!Parameters' in marker_data:
                if 'DropActor' in marker_data['!Parameters']:
                    if 'id' in marker_id[0]:
                        marker_id = marker_id[0]['id']

                    item = get_item(connection, marker_data['!Parameters']['DropActor'])
                    if len(item) > 0:
                        if 'id' in item[0]:
                            item_id = item[0]['id']
                            link_marker_to_item(connection, marker_id, item_id)
                    else:
                        print(marker_data)
                        log_file.write("Unable to add marker-item link: {0}\n".format(marker_data))


def link_marker_to_item(connection, marker_id, item_id):
    print("link_marker_to_item", marker_id, item_id)
    return insert(
        connection,
        "INSERT INTO `item_marker` (`id`, `item_id`, `marker_id`, `created_at`, `updated_at`) VALUES (NULL, {0}, {1}, NOW(), NOW());".format(
            item_id,
            marker_id
        ),
        True
    )


def get_marker_id(connection, map_region_id, marker_type_id, source_file_id, hash_id, srt_hash, marker_name):
    return query(
        connection,
        "SELECT `id` FROM `markers` WHERE `map_region_id` = {0} AND  `marker_type_id` = {1} AND `source_file_id` = {2} AND `hash_id` = {3} AND `srt_hash` = {4} AND `marker_name` = '{5}'".format(
            map_region_id,
            marker_type_id,
            source_file_id,
            hash_id,
            srt_hash,
            marker_name
        ),
        True
    )


def get_item(connection, item_name):
    return query(
        connection,
        "SELECT * FROM `items` WHERE `item_game_name` = '{0}'".format(item_name),
        True
    )


def add_marker(connection, map_region_id, marker_type_id, source_file_id, has_item, hash_id, srt_hash, marker_name,
               marker_description, x, y, z):
    return insert(
        connection,
        "INSERT INTO `markers` (`id`, `map_region_id`, `marker_type_id`, `source_file_id`, `has_item`, `hash_id`, `srt_hash`, `marker_name`, `marker_description`, `x`, `y`, `z`, `created_at`, `updated_at`) VALUES (NULL, '{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', {10}, NOW(), NOW());".format(
            map_region_id, marker_type_id, source_file_id, has_item, hash_id, srt_hash, marker_name, marker_description, x, y, z
        ),
        True)


def get_source_file(path):
    source_file = path.split('/')
    keep = False
    sourcefile = ''

    for index in range(len(source_file)):
        if keep:
            sourcefile = sourcefile + source_file[index] + '/'

        if source_file[index] == 'json':
            keep = True

    return sourcefile[:-6]


def get_source_file_id(connection, source_file_name):
    source_file = query(
        connection,
        "SELECT `id` FROM `botwmap`.`source_files` WHERE `source_file_name` = '{0}'".format(source_file_name),
        True
    )

    if len(source_file) <= 0:
        insert(
            connection,
            "INSERT INTO `botwmap`.`source_files` (`id`, `source_file_name`, `created_at`, `updated_at`) VALUES (NULL, '{0}',  NOW(), NOW());".format(
                source_file_name
            ),
            True
        )
        return get_source_file_id(connection, source_file)
    else:
        return source_file[0]['id']


def get_marker_type_id(connection, marker_type_name, icon_id='NULL', marker_type_description='NULL'):
    marker_type_id = query(
        connection,
        "SELECT `id` FROM `botwmap`.`marker_types` WHERE `marker_type_name` = '{0}'".format(marker_type_name),
        True
    )

    if len(marker_type_id) <= 0:
        insert(
            connection,
            "INSERT INTO `botwmap`.`marker_types` (`id`, `marker_type_name`, `marker_type_slug`, `marker_icon_id`, `marker_type_description`, `created_at`, `updated_at`) VALUES (NULL, '{0}', '{1}', {2}, {3}, NOW(), NOW());".format(
                marker_type_name,
                marker_type_name,
                icon_id,
                marker_type_description
            ),
            True
        )
        return get_marker_type_id(connection, marker_type_name, icon_id, marker_type_description)
    else:
        return marker_type_id[0]['id']


def loadObjectData(self):
    files = [
        "C:\\Users\\zephe\\PycharmProjects\\botw-map-data\\msbt\\object_strings.json",
    ]

    for file in files:
        with open(file, 'r', encoding="utf8") as infile:
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


def database_connect():
    return pymysql.connect(host='localhost',
                           user='root',
                           password='',
                           db='botwmap',
                           # charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)


def insert(connection, query: str, verbose_output=False):
    result = False

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.execute('COMMIT;')
    except IntegrityError as error:
        if verbose_output:
            print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))
        # except pymysql.InternalError as e:
        #     print('\033[31merror!\033[0m')
    except InternalError as error:
        if verbose_output:
            print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))

    # finally:
    #     connection.close()

    return result


def query(connection, query: str, verbose_output=False):
    # print(query)
    result = []

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.execute('COMMIT;')
    except IntegrityError as error:
        if verbose_output:
            print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))
        # except pymysql.InternalError as e:
        #     print('\033[31merror!\033[0m')
    except InternalError as error:
        if verbose_output:
            print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))

    # finally:
    #     connection.close()

    return result


def main():
    path = "C:\\Users\\zephe\\PycharmProjects\\botw-map-data\\output\\json"
    connection = database_connect()

    query(connection, "ALTER TABLE `botwmap`.`item_marker` ADD UNIQUE `item_marker_item_id_marker_id_unique` (`item_id`, `marker_id`);", True)

    skip_files = [
        "LazyTraverseList.mubin.json",
        "Static.mubin.json",
        "DebugDemoArea.mubin.json"
    ]

    pattern_blwp = re.compile(r"\.blwp.json", re.IGNORECASE)
    pattern_mubin = re.compile(r"\.mubin.json", re.IGNORECASE)

    for (dirpath, dirnames, filenames) in os.walk(path):
        directory = dirpath.replace('\\', '/') + '/'

        for filename in filenames:
            if filename not in skip_files:
                if pattern_blwp.search(filename) is not None:
                    add_blwp_to_db(connection, directory + filename)
                if pattern_mubin.search(filename) is not None:
                    add_mubin_to_db(connection, directory + filename)


if __name__ == "__main__":
    main()
