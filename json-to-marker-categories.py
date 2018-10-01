import json

import pymysql
from pymysql import IntegrityError, InternalError


def main():
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='',
                                 db='botwmap',
                                 # charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    categories = {}

    with open("C:\\wamp\\www\\botwmap\\.notes\\categories.json") as categories_json:
        data = json.loads(categories_json.read())

        for entry in data:
            parent_category_id = entry['parent']
            marker_category_name = entry['name']
            marker_type_slug = entry['type']

            if parent_category_id is None:
                parent_category_id = 'NULL'
            else:
                parent_category_id = "'{0}'".format(parent_category_id.replace("'", '\\\''))

            if parent_category_id != 'NULL':
                marker_category = query(connection,
                            "SELECT `id` FROM `botwmap`.`marker_categories` WHERE `marker_category_name` = {0};".format(
                                parent_category_id))
                parent_category_id = marker_category[0]['id']

            if marker_type_slug is not None:
                marker_type = query(connection,
                            "SELECT * FROM `botwmap`.`marker_types` WHERE `marker_type_slug` = '{0}';".format(
                                marker_type_slug.replace("'", '\\\'')))
                print("Marker type: {0}".format(marker_type))

            insert(connection,
                "INSERT INTO `botwmap`.`marker_categories` (`id`, `parent_category_id`, `marker_category_name`, `created_at`, `updated_at`) VALUES (NULL, {0}, '{1}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);".format(
                    parent_category_id, marker_category_name.replace("'", "\\'")))

            last_category = query(connection,
                        "SELECT * FROM `botwmap`.`marker_categories` WHERE `marker_category_name` = \"{0}\";".format(
                            marker_category_name.replace("'", "\\'")))

            if marker_type_slug is not None:
                insert(connection,
                    "INSERT INTO `botwmap`.`marker_category_marker_type` (`id`, `marker_type_id`, `marker_category_id`, `created_at`, `updated_at`) VALUES (NULL, {0}, {1}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);".format(
                        marker_type[0]['id'], last_category[0]['id']))

def query(connection, query: str):
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
