import os
import zipfile


def zipfile_name(file_dir):
    # 读取文件夹下面的文件名.zip
    file_name_list = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            if os.path.splitext(file)[1] == '.zip':  # 读取带zip 文件
                file_name_list.append(os.path.join(root, file))
        return file_name_list


# 将zip文件解压处理，并放到指定的文件夹里面去
def unzip_file(zip_file_name, destination_path):
    archive = zipfile.ZipFile(zip_file_name, mode='r')
    for file in archive.namelist():
        archive.extract(file, destination_path)


def main():
    source_path = "../data/"
    target_path = "../data/"
    files = zipfile_name(source_path)
    for file in files:
        unzip_file(file, target_path)


if __name__ == "__main__":
    main()
    print("done")
