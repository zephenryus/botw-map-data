import os

import pymysql
from pymysql import IntegrityError, InternalError

import msbt_to_db
import items_to_db
import static_mubin_to_db
import markers_json_to_db


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


def seed_database():
    connection = database_connect()
    query(
        connection,
        "INSERT INTO `marker_icons` (`id`, `icon_url`, `created_at`, `updated_at`) VALUES (NULL, 'default.png', CURRENT_TIME(), CURRENT_TIME());",
        True
    )
    query(
        connection,
        "ALTER TABLE `botwmap`.`item_marker` ADD UNIQUE `item_marker_item_id_marker_id_unique` (`item_id`, `marker_id`);",
        True
    )


def main():
    os.system('php "C:/wamp/www/botwmap/artisan" migrate:refresh')
    seed_database()

    msbt_to_db.main()
    items_to_db.main()
    static_mubin_to_db.main()
    markers_json_to_db.main()


if __name__ == '__main__':
    main()
