sudo umount /home/vagrant/wrapperfs/tmp/mount_dir
rm -rf ./tmp/mount_dir
rm -rf ./tmp/data_dir
rm -rf ./tmp/db_dir
rm ./tmp/log_file.txt


mkdir -p ./tmp/mount_dir
mkdir -p ./tmp/data_dir
mkdir -p ./tmp/db_dir
touch ./tmp/log_file.txt



./build/tools/mkfs  ./tmp/db_dir 