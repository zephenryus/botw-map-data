import json


def main():
    with open('output/marker-types/markers-renamed.csv', 'w') as outfile:
        with open('items.json', 'r') as items_infile:
            item_descriptions = json.loads(items_infile.read())

            with open('marker-types.csv', 'r') as infile:
                for line in infile:
                    id, marker_type_name, marker_type_slug, icon, marker_type_description, created_at, updated_at = \
                        line.split(',')

                    for key in item_descriptions:
                        if key == "{0}_Name".format(marker_type_slug):
                            marker_type_name = item_descriptions[key]

                        if key == "{0}_Desc".format(marker_type_slug):
                            marker_type_description = item_descriptions[key]

                        marker_type_name = marker_type_name.strip()
                        marker_type_description = marker_type_description.strip()
                        updated_at = updated_at.strip()

                    outfile.write('"{0}", "{1}", "{2}", "{3}", "{4}", "{5}", "{6}"\n'.format(id, marker_type_name, marker_type_slug, icon, marker_type_description, created_at, updated_at))

if __name__ == "__main__":
    main();