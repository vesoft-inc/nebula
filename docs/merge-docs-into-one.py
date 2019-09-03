import os
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

def get_all_dirs(file_dir):
    file_list = os.listdir(file_dir)
    # print(file_list)
    dir_list=[]
    for path in file_list:
        file_path = file_dir + '/' + path
        if os.path.isdir(file_path):
            print(file_path + ":it's a directory")
            dir_list.append(file_path)
        elif os.path.isfile(path):
            print(file_path + ":it's a normal file")
        else:
            print(file_path + ":it's a special file(.md)")
    return dir_list

def read_md_file(md_file_path):
    with open(md_file_path) as f:
        lines = f.readlines()
    return lines

def creat_md_file(file_name):
    f = open(file_name,'w')
    #f.write('the new file\n')
    #f.write('##########################\n\n')
    f.write('\n')
    f.close()

def write_file(source_file_dir,dst_md_file):
    with open(dst_md_file, 'a') as md_writor:
        md_file_list = get_all_md_file_path(source_file_dir)
        md_file_list.sort()
        for file in md_file_list:
            print('writing ' + file)
            lines = read_md_file(file)
            # print(lines)
            for line in lines:
                md_writor.write(line)
            md_writor.write('\n\n\n')

def write_all(source_dir, dst_md_file):
    file_list = os.listdir(source_dir)
    # print(file_list)
    dir_list=[]
    for path in file_list:
        file_path = source_dir + '/' + path
        if os.path.isdir(file_path):
            # print(file_path + ":it's a directory")
            dir_list.append(file_path)
        elif os.path.isfile(path):
            pass
            # print(file_path + ":it's a normal file")
    write_file(source_dir, dst_md_file)
    dir_list.sort()
    for sub_dir in dir_list:
        write_all(sub_dir, dst_md_file)

if __name__ == "__main__":
    file = '/Users/zhangying/Desktop/output.md' # Output file
    creat_md_file(file)

    source_dir = '/Users/zhangying/Desktop/en-docs'  # Source file to be merged
    write_all(source_dir, file)

   

    # with open(file_name, 'a') as md_writor:
    #     md_file_list = get_all_md_file_path(file_dir1)
    #     for file in md_file_list:
    #         lines = read_md_file(file)
    #         print(lines)
    #         for line in lines:
    #             md_writor.write(line)
    #         md_writor.write('\n\n\n')
