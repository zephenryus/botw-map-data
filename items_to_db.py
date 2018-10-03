import json
import os
import re

import pymysql
from pymysql import IntegrityError, InternalError


def items_to_db(path):
    with open(path) as infile:
        print("Reading {0}...".format(path))

        items_json = json.loads(infile.read())

        items = {}
        pattern = re.compile(r"((_Name)|(_Desc)|(_PictureBook))", re.IGNORECASE)
        pattern_name = re.compile(r"_Name", re.IGNORECASE)
        pattern_desc = re.compile(r"_Desc", re.IGNORECASE)
        pattern_pict = re.compile(r"_PictureBook", re.IGNORECASE)
        pattern_basename = re.compile(r"_BaseName", re.IGNORECASE)
        pattern_spec = re.compile(r"_Spec", re.IGNORECASE)
        pattern_menudesc = re.compile(r"_MenuDesc", re.IGNORECASE)

        for item in items_json:
            text = items_json[item]['text']
            key = pattern.sub("", item).strip()

            if pattern_basename.search(item) is not None:
                continue
            if pattern_spec.search(item) is not None:
                continue
            if pattern_menudesc.search(item) is not None:
                continue

            if key not in items:
                items[key] = {
                    'name': '',
                    'description': '',
                    'picture_book': ''
                }

            if pattern_name.search(item) is not None:
                items[key]['name'] = text
            if pattern_desc.search(item) is not None:
                items[key]['description'] = text
            if pattern_pict.search(item) is not None:
                items[key]['picture_book'] = text

        add_to_db(items)


def add_to_db(items):
    connection = database_connect()

    for index in items:
        if len(query(connection, "SELECT * FROM `items` WHERE `item_game_name` = '{0}'".format(index))) == 0:
            icon = 'NoImage.png'
            icon_path = r"C:\wamp\www\botwmap\public\images\icons\items\{0}.png".format(index)
            if os.path.isfile(icon_path):
                icon = "{0}.png".format(index)

            insert(connection,
                         "INSERT INTO `items` (`id`, `item_game_name`, `item_type_id`, `item_icon`, `item_name`, `item_description`, `sale_price`, `created_at`, `updated_at`) VALUES (NULL, '{0}', 1, '{1}', '{2}', '{3}', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)".format(
                             index,
                             items[index]['name'].replace("'", "''"),
                             icon.replace("'", "''"),
                             items[index]['description'].replace("'", "''")
                         ),
                         True)


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
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\Item.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\ArmorHead.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\ArmorLower.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\ArmorUpper.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\Bullet.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\CookResult.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\HorseReins.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\HorseSaddle.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\PlayerItem.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\WeaponBow.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\WeaponLargeSword.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\WeaponShield.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\WeaponSmallSword.msbt.json"
    items_to_db(path)
    path = r"C:\Users\zephe\PycharmProjects\msbt\output\json\Pack\Bootup_USen\Message\Msg_USen.product\ActorType\WeaponSpear.msbt.json"
    items_to_db(path)


if __name__ == '__main__':
    main()
