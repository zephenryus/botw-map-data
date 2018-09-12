import os
import struct


# class PrOD:
    # data_object = {}

    # def __init__(self, path, temp_data=None):
        # print("Parsing PrOD file...")

def PrOD(path):
    filename = os.path.basename(path)
    data_object = {}
    # print("Reading {0}...".format(filename))

    file = open(path, 'rb')
    data = file.read()
    signature = data[0x00:0x04]
    count, length = struct.unpack('>II', data[0x08:0x10])

    filesize, cluster_count, strings_ptr = \
        struct.unpack('>III', data[0x10:0x1c])

    pos = 0x20
    for _ in range(cluster_count):
        cluster_size, element_count, cluster_strptr = \
            struct.unpack('>III', data[pos:pos + 0x0c])
        name = data[strings_ptr + cluster_strptr:].split(b'\0')[0].decode('utf-8')
        name = str(name)

        data_object[name] = {}
        for j in range(element_count):
            # Model/%s.bfres
            # Model/%s.Tex2.bfres
            x, y, z, rot_x, rot_y, rot_z, scale = \
                struct.unpack('>fffffff', data[pos + 0x10 + j * 0x20:pos + 0x2c + j * 0x20])
            data_object[name]['X'] = x
            data_object[name]['Y'] = y
            data_object[name]['Z'] = z
            data_object[name]['RotX'] = rot_x
            data_object[name]['RotY'] = rot_y
            data_object[name]['RotZ'] = rot_z
            data_object[name]['Scale'] = scale
        pos += 0x10 + cluster_size

    file.close()
    return data_object