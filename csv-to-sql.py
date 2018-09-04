import atexit
import math
import subprocess
from datetime import datetime
import pymysql.cursors

class ConvertCSVtoSQL:
    connection = None

    def __init__(self, path: str, refresh_db=False):
        self.start_time = datetime.now()
        self.on_start()

        if refresh_db:
            self.refresh_database()

        self.database_connect()

        self.convert_file(path)

    def on_start(self):
        print("Scan started at {0}".format(self.start_time.strftime("%Y-%m-%d %H:%M:%S.%f")))
        atexit.register(self.on_stop)

    def on_stop(self):
        end_time = datetime.now()
        delta = end_time - self.start_time
        print("Scan took {0} seconds".format(delta.total_seconds()))

    def convert_file(self, path):
        print('Converting {0}...'.format(path))
        lines_read = 0

        with open(path, 'r') as infile:
            for line in infile:
                lines_read += 1

                if lines_read % 100 == 0:
                    print("{0} lines read...".format(lines_read))

                current_line = line.split(',')

                for index in range(len(current_line)):
                    current_line[index] = current_line[index].strip(' \n')

                map_region_x = math.floor(float(current_line[1]) / 1000) + 5
                map_region_z = math.floor(float(current_line[3]) / 1000) + 4
                map_region = (map_region_z * 10 + map_region_x) + 1

                self.query("INSERT INTO `markers` (`id`, `map_region_id`, `marker_name`, `x`, `y`, `z`, `source`, `marker_type_id`, `marker_category_id`, `marker_sub_category_id`, `created_at`, `updated_at`) VALUES (NULL, '{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', NULL, NULL);".format(map_region, current_line[0], float(current_line[1]), float(current_line[2]), float(current_line[3]), path, 1, 1, 1))

    def query(self, query: str, data=()) -> tuple:
        result = []

        # try:
        with self.connection.cursor() as cursor:
            cursor.execute(query, data)
            result = cursor.fetchall()
        # finally:
        #     self.connection.close()

        return result

    def insert(self, query: str, data: tuple) -> bool:
        try:
            with self.connection.cursor() as cursor:
                result = cursor.execute(query, data)
        finally:
            self.connection.close()

        return True if result > 0 else False

    def refresh_database(self) -> None:
        print('Refreshing database...')

        subprocess.run([
            "php",
            "C:\\wamp\\www\\botwmap\\artisan",
            "migrate:refresh",
            "--seed"
        ])

    def database_connect(self):
        self.connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='',
                                     db='botwmap',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)


def main():
    ConvertCSVtoSQL("output/dlc.csv", True)


if __name__ == "__main__":
    main()