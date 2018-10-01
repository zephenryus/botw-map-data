import json
import os

import pymysql
from pymysql import IntegrityError, InternalError

source_files_id_cache = [
    ''
]


def parse_msbt(path):
    with open(path, 'r') as infile:
        data = json.loads(infile.read())
        return data


def get_source_file_id(connection, source_file):
    if source_file in source_files_id_cache:
        return source_files_id_cache.index(source_file)

    response = query(
        connection,
        "SELECT `id` FROM `botwmap`.`source_files` WHERE `source_file_name` = '{0}'".format(source_file),
        True
    )

    if len(response) <= 0:
        insert_file_source(connection, source_file)
        response = query(
            connection,
            "SELECT `id` FROM `botwmap`.`source_files` WHERE `source_file_name` = '{0}'".format(source_file),
            True
        )

    return response[0]['id']


def insert_file_source(connection, source_file, verbose=False):
    insert(
        connection,
        "INSERT INTO `botwmap`.`source_files` (`id`, `source_file_name`, `created_at`, `updated_at`) VALUES (NULL, '{0}',  CURRENT_TIME(), CURRENT_TIME());".format(
            source_file
        ),
        verbose
    )


def compile_msbt(data, source_file=''):
    return_data = []

    for key in data:
        label = key.replace("'", "''")
        message = data[key]['text'].replace("'", "''")
        attribute = data[key]['attributes'].replace("'", "''")
        meta = json.dumps(data[key]['meta']).replace("'", "''")
        return_data.append({
            'label': label,
            'message': message,
            'attribute': attribute,
            'meta': meta,
            'source': source_file
        })

    return return_data


def insert_msbt(connection, data, verbose=False):
    for row in data:
        source_file_id = get_source_file_id(connection, row['source'])

        insert(
            connection,
            "INSERT INTO `botwmap`.`messages` (`id`, `label`, `message`, `attribute`, `meta`, `source_file_id`, `created_at`, `updated_at`) VALUES (NULL, '{0}', '{1}', '{2}', '{3}', '{4}', CURRENT_TIME(), CURRENT_TIME());".format(
                row['label'],
                row['message'],
                row['attribute'],
                row['meta'],
                source_file_id
            ),
            verbose
        )


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


def database_connect():
    return pymysql.connect(host='localhost',
                           user='root',
                           password='',
                           db='botwmap',
                           # charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)


def msbt_to_db(path):
    print('Reading {0}/'.format(path))

    connection = database_connect()

    for (dirpath, dirnames, filenames) in os.walk(path):
        directory = dirpath.replace('\\', '/') + '/'

        for filename in filenames:
            current_file = directory + filename
            print('Reading {0}...'.format(current_file))

            data = parse_msbt(current_file)
            data = compile_msbt(data, current_file[50:])
            insert_msbt(connection, data, True)


def main():
    msbt_to_db("C:\\Users\\zephe\\PycharmProjects\\botw-map-data\\msbt")


if __name__ == "__main__":
    main()
