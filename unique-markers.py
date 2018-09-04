def get_unique_names(path, outfile_resource, unique_names=None) -> list:
    print('Reading {0}...'.format(path))

    if unique_names is None:
        unique_names = []
    unique_names = unique_names

    lines_read = 0

    with open(path, 'r') as infile:
        for line in infile:
            lines_read += 1

            if lines_read % 100 == 0:
                print("{0} lines read...".format(lines_read))

            current_line = line.split(',')
            if current_line[0] not in unique_names:
                unique_names.append(current_line[0])
                outfile_resource.write("{0}\n".format(current_line[0]))

    return unique_names


def main():
    with open('output/unique_names.txt', 'w') as outfile:
        names = get_unique_names("output/dlc.csv", outfile)
        names = get_unique_names("output/mainfield.csv", outfile, names)
        get_unique_names("output/titlebg.csv", outfile, names)


if __name__ == "__main__":
    main()