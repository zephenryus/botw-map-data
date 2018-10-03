import json
import math
import re

import pymysql
from pymysql import IntegrityError, InternalError


location_marker_icon = {
    'Dungeon': 'shrine.png',
    'CheckPoint': 'default.png',
    'RemainsWind': 'vah-medoh.png',
    'RemainsElectric': 'vah-naboris.png',
    'RemainsFire': 'vah-rudania.png',
    'RemainsWater': 'vah-ruta.png',
    'Labo': 'lab.png',
    'ShopColor': 'shop-dye.png',
    'Village': 'town.png',
    'Hatago': 'shop-stable.png',
    'ShopYorozu': 'shop-item.png',
    'ShopYadoya': 'default.png',
    'ShopBougu': 'default.png',
    'Tower': 'tower.png',
    'ShopJewel': 'shop-jewler.png',
    'StartPoint': 'shrine-of-resurrection.png',
    'Castle': 'hyrule-castle.png'
}

location_marker_type = {
    'Dungeon': 'Shrine',
    'CheckPoint': 'CheckPoint',
    'RemainsWind': 'DevineBeast',
    'RemainsElectric': 'DevineBeast',
    'RemainsFire': 'DevineBeast',
    'RemainsWater': 'DevineBeast',
    'Labo': 'Lab',
    'ShopColor': 'Shop',
    'Village': 'Villiage',
    'Hatago': 'Stable',
    'ShopYorozu': 'Shop',
    'ShopYadoya': 'Shop',
    'ShopBougu': 'Shop',
    'Tower': 'Tower',
    'ShopJewel': 'Shop',
    'StartPoint': 'Shrine',
    'Castle': 'HyruleCastle'
}


def database_connect():
    return pymysql.connect(host='localhost',
                           user='root',
                           password='',
                           db='botwmap',
                           # charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)


def insert(connection, query: str, verbose_output=False):
    # print(query)
    result = False

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.execute('COMMIT;')
    except IntegrityError as error:
        if verbose_output:
            print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))
    except InternalError as error:
        if verbose_output:
            print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))

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
    except InternalError as error:
        if verbose_output:
            print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))

    return result


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


def add_marker(connection, map_region_id, marker_type_id, source_file_id, has_item, hash_id, srt_hash, marker_name,
               marker_description, x, y, z):
    return insert(
        connection,
        "INSERT INTO `markers` (`id`, `map_region_id`, `marker_type_id`, `source_file_id`, `has_item`, `hash_id`, `srt_hash`, `marker_name`, `marker_description`, `x`, `y`, `z`, `created_at`, `updated_at`) VALUES (NULL, '{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', {10}, NOW(), NOW());".format(
            map_region_id, marker_type_id, source_file_id, has_item, hash_id, srt_hash, marker_name, marker_description, x, y, z
        ),
        True)


def get_message_by_id(connection, message_label):
    return query(connection, "SELECT * FROM `messages` WHERE `label` = '{0}'".format(message_label))



def main():
    connection = database_connect()
    path = r"C:\Users\zephe\PycharmProjects\botw-map-data\output\json\0010\Map\MainField\Static.mubin.json"
    source_file_id = get_source_file_id(connection, 'Map/MainField/Static.mubin')

    with open(path) as infile:
        raw_data = json.loads(infile.read())
        print(raw_data)
        for static_array_type in raw_data:
            for location in raw_data[static_array_type]:
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

                if static_array_type == 'DLCRestartPos':
                    x = float(location['Translate']['X'])
                    y = float(location['Translate']['Y'])
                    z = float(location['Translate']['Z'])
                    map_region_id = get_map_region_id(x, z)
                    icon_id = get_marker_icon_id(connection, 'default.png')
                    marker_type_id = get_marker_type_id(connection, static_array_type, icon_id)
                    marker_name = location['UniqueName']

                elif static_array_type == 'KorokLocation':
                    hash_id = int(location['Flag'][32:])
                    x = float(location['Translate']['X'])
                    y = float(location['Translate']['Y'])
                    z = float(location['Translate']['Z'])
                    map_region_id = get_map_region_id(x, z)
                    icon_id = get_marker_icon_id(connection, 'korok-seed.png')
                    marker_type_id = get_marker_type_id(connection, 'KorokLocation', icon_id)
                    marker_name = static_array_type

                elif static_array_type == 'FldObj_DLC_ShootingStarCollaborationAnchor':
                    x = float(location['Translate']['X'])
                    y = float(location['Translate']['Y'])
                    z = float(location['Translate']['Z'])
                    map_region_id = get_map_region_id(x, z)
                    icon_id = get_marker_icon_id(connection, 'default.png')
                    marker_type_id = get_marker_type_id(connection, static_array_type, icon_id)
                    marker_name = location['collaboSSFalloutFlagName']

                elif static_array_type == 'LocationMarker':
                    x = float(location['Translate']['X'])
                    y = float(location['Translate']['Y'])
                    z = float(location['Translate']['Z'])
                    map_region_id = get_map_region_id(x, z)

                    if 'Icon' in location:
                        icon_id = get_marker_icon_id(connection, location_marker_icon[location['Icon']])
                        marker_type_id = get_marker_type_id(connection, location_marker_type[location['Icon']], icon_id)

                    else:
                        if location['SaveFlag'] == 'HyruleGround':
                            # Maybe add this later?
                            continue

                    if 'MessageID' in location:
                        marker_name = get_message_by_id(connection, location['MessageID'])[0]['message']

                    elif 'Icon' in location:
                        pattern_shop = re.compile(r"Shop", re.IGNORECASE)
                        pattern_location = re.compile(r"Location_", re.IGNORECASE)
                        pattern_yadoya = re.compile(r"Yadoya", re.IGNORECASE)

                        pattern_shop.match(location['Icon'])
                        if pattern_shop.search(location['Icon']) is not None:
                            message_id = pattern_shop.sub('Shop_', location['Icon'])
                            message_id = pattern_location.sub('', location['SaveFlag']) + message_id
                            message_id = pattern_yadoya.sub('Yado', message_id)

                            message = get_message_by_id(connection, message_id)

                            if len(message) > 0:
                                marker_name = message[0]['message']
                            else:
                                marker_name = "{0}_{1}".format(location['SaveFlag'], location['Icon'])

                elif static_array_type == 'LocationPointer':
                    x = float(location['Translate']['X'])
                    y = float(location['Translate']['Y'])
                    z = float(location['Translate']['Z'])
                    map_region_id = get_map_region_id(x, z)
                    icon_id = get_marker_icon_id(connection, 'default.png')
                    marker_type_id = get_marker_type_id(connection, static_array_type, icon_id)
                    if 'MessageID' in location:
                        marker_name = get_message_by_id(connection, location['MessageID'])[0]['message']

                    else:
                        marker_name = "Unnamed Location Pointer"

                elif static_array_type == 'NonAutoGenArea':
                    # Add Later
                    continue

                elif static_array_type == 'NonAutoPlacement':
                    # Add Later
                    continue

                elif static_array_type == 'RoadNpcRestStation':
                    # Add Later
                    continue

                elif static_array_type == 'StartPos':
                    # Add Later
                    continue

                elif static_array_type == 'StaticGrudgeLocation':
                    x = float(location['Translate']['X'])
                    y = float(location['Translate']['Y'])
                    z = float(location['Translate']['Z'])
                    map_region_id = get_map_region_id(x, z)
                    icon_id = get_marker_icon_id(connection, 'default.png')
                    marker_type_id = get_marker_type_id(connection, static_array_type, icon_id)
                    marker_name = static_array_type

                elif static_array_type == 'TargetPosMarker':
                    x = float(location['Translate']['X'])
                    y = float(location['Translate']['Y'])
                    z = float(location['Translate']['Z'])
                    map_region_id = get_map_region_id(x, z)
                    icon_id = get_marker_icon_id(connection, 'default.png')
                    marker_type_id = get_marker_type_id(connection, static_array_type, icon_id)
                    if 'UniqueName' in location:
                        marker_name = location['UniqueName']
                    else:
                        marker_name = "Unnamed TargetPosMarker"

                elif static_array_type == 'TeraWaterDisable':
                    # Add Later
                    continue

                elif static_array_type == 'TerrainHideCenterTag':
                    # Add Later
                    continue

                else:
                    continue

                print(
                    map_region_id,
                    marker_type_id,
                    source_file_id,
                    has_item,
                    hash_id,
                    srt_hash,
                    marker_name.replace("'", "''"),
                    marker_description.replace("'", "''"),
                    x,
                    y,
                    z
                )

                add_marker(
                    connection,
                    map_region_id,
                    marker_type_id,
                    source_file_id,
                    has_item,
                    hash_id,
                    srt_hash,
                    marker_name.replace("'", "''"),
                    marker_description.replace("'", "''"),
                    x,
                    y,
                    z
                )


if __name__ == '__main__':
    main()
