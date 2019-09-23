import os
import sys

# This script is to help you merge all the docs into one mardown file, then you can convert it into one pdf file and down load.


def get_all_md_file_path(file_path):
    if not os.path.isdir(file_path):
        print(file_path+" is not a dir")
    file_list_in_md = os.listdir(file_path)
    file_list_md = []
    for path_md in file_list_in_md:
        if '.md' in path_md:
            file_list_md.append(file_path + '/' + path_md)
    # print(file_list_md)
    return file_list_md


def read_md_file(md_file_path):
    with open(md_file_path) as f:
        lines = f.readlines()
    return lines


def creat_md_file(file_name):
    f = open(file_name, 'w')
    f.write('\n')
    f.close()


def write_file(source_file_dir, dst_md_file):
    with open(dst_md_file, 'a') as md_writor:
        md_file_list = get_all_md_file_path(source_file_dir)
        md_file_list.sort()
        for file in md_file_list:
            print('writing ' + file)
            lines = read_md_file(file)
            for line in lines:
                md_writor.write(line)
            md_writor.write('\n\n\n')


def write_all(source_dir, dst_md_file):
    file_list = os.listdir(source_dir)
    dir_list = []
    for path in file_list:
        file_path = source_dir + '/' + path
        if os.path.isdir(file_path):
            dir_list.append(file_path)
        elif os.path.isfile(path):
            pass
    write_file(source_dir, dst_md_file)
    dir_list.sort()
    for sub_dir in dir_list:
        write_all(sub_dir, dst_md_file)


if __name__ == "__main__":

    # define these constants for reusability without commanline arguments
    UNDEFINED = 'UNDEFINED'

    # Output file
    target_file = UNDEFINED
    # Source file to be merged
    source_dir = UNDEFINED

    # argument 1 will be the source, argument 2 will be the target
    if len(sys.argv) == 1:
        sys.exit(
            'usage: merge-all [target_file] [source_dir]'
        )

    elif len(sys.argv) != 3:
        if target_file == UNDEFINED or source_dir == UNDEFINED:
            sys.exit(
                'error: target_file and source_dir not given in either scripts or commandline'
            )
    else:
        target_file = sys.argv[1]
        source_dir = sys.argv[2]

    creat_md_file(target_file)
    write_all(source_dir, target_file)
