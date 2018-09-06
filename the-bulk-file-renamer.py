import math
import os
import re


def main():
    zoom = 6
    read_dir = "{0}{1}".format("C:\\wamp\\www\\botwmap\\public\\hyrule\\", zoom)
    width = 2 ** zoom
    dryrun = False

    for (dirpath, dirnames, filenames) in os.walk(read_dir):
        dirpath = dirpath.replace('\\', '/') + '/'
        for filename in filenames:
            file, ext = os.path.splitext(filename)
            matches = re.search('(?:MapTex\d-)([0-3])(?:_)(\d{1,4})(?:\.png)', filename)
            tile_vertical_offset = int(matches.group(1))
            tile_horizontal_offset = int(matches.group(2))
            tile_y_offset = (math.floor(tile_vertical_offset / 2)) * 32
            tile_x_offset = (tile_vertical_offset % 2) * 32

            index = (math.floor(tile_horizontal_offset / 32) + tile_y_offset) * 64 + (tile_horizontal_offset % 32 + tile_x_offset)
            y = int(math.floor(index / width))
            x = index % width

            print('Moving {0} to {1}{2}/{3}.png...'.format(dirpath + filename, dirpath, y, x))

            if not dryrun:
                if not os.path.exists("{0}{1}".format(dirpath, y)):
                    os.makedirs("{0}{1}".format(dirpath, y))

                os.rename(
                    "{0}".format(dirpath + filename),
                    "{0}{1}/{2}.png".format(dirpath, y, x)
                )


if __name__ == "__main__":
    main()
