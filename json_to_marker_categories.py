import json

import pymysql
from pymysql import IntegrityError, InternalError


def get_parent_category_id(connection, parent_category_name):
    if parent_category_name is None:
        return 'NULL'

    parent_category = query(
        connection,
        "SELECT `id` FROM `botwmap`.`marker_categories` WHERE `marker_category_name` = '{0}';".format(
              parent_category_name
        )
    )

    if len(parent_category) > 0:
        if 'id' in parent_category[0]:
            return parent_category[0]['id']

    return 'NULL'


def get_marker_type_id(connection, marker_type_name):
    if marker_type_name is None:
        return 'NULL'

    marker_type = query(
        connection,
        "SELECT `id` FROM `botwmap`.`marker_types` WHERE `marker_type_name` = '{0}';".format(
              marker_type_name
        )
    )

    if len(marker_type) > 0:
        if 'id' in marker_type[0]:
            return marker_type[0]['id']
    else:
        print('Marker type not found: {0}'.format(marker_type_name))
        exit(-2)


def main():
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='',
                                 db='botwmap',
                                 # charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    categories = {}

    with open(r"C:\wamp\www\botwmap\.notes\categories.json") as categories_json:
        data = json.loads(categories_json.read())

        for entry in data:
            print("entry: {0}".format(entry))
            marker_category_name = entry['name']
            parent_category_id = get_parent_category_id(connection, entry['parent'])
            marker_type_id = get_marker_type_id(connection, entry['type'])

            insert(
                connection,
                "INSERT INTO `botwmap`.`marker_categories` (`id`, `parent_category_id`, `marker_category_name`, `created_at`, `updated_at`) VALUES (NULL, {0}, '{1}', NOW(), NOW());".format(
                    parent_category_id,
                    marker_category_name.replace("'", "''")
                )
            )

            last_category = query(
                connection,
                "SELECT `id` FROM `botwmap`.`marker_categories` WHERE `marker_category_name` = '{0}';".format(
                    marker_category_name.replace("'", "''")
                )
            )[0]['id']
            print("last category:", last_category)

            if entry['type'] is not None:
                insert(connection,
                    "INSERT INTO `botwmap`.`marker_category_marker_type` (`id`, `marker_type_id`, `marker_category_id`, `created_at`, `updated_at`) VALUES (NULL, {0}, {1}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);".format(
                        marker_type_id, last_category))

def query(connection, query: str):
    print(query)
    result = []

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.execute('COMMIT;')
    except IntegrityError as error:
        print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))
        # except pymysql.InternalError as e:
        #     print('\033[31merror!\033[0m')
    except InternalError as error:
        print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))

    return result


def insert(connection, query: str):
    result = False

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.execute('COMMIT;')
    except IntegrityError as error:
        print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))
    # except pymysql.InternalError as e:
    #     print('\033[31merror!\033[0m')
    except InternalError as error:
        print('\033[31mERROR {0}: {1}\033[0m'.format(error.args[0], error.args[1]))

    # finally:
    #     self.connection.close()

    return result


if __name__ == "__main__":
    main()
