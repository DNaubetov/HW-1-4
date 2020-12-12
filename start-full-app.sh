# Скачивание входных файлов из гугл диска
echo 'Используем магию вне Хогвартса'
echo 'Скачивание архива'
file_id='1PY_7TF19FCwaFbuZv6K31l71NXmALYvH'
file_name='hw1 4.zip'
curl -sc /tmp/cookie "https://drive.google.com/uc?export=download&id=${file_id}" > /dev/null
code="$(awk '/_warning_/ {print $NF}' /tmp/cookie)"
curl -Lb /tmp/cookie "https://drive.google.com/uc?export=download&confirm=${code}&id=${file_id}" -o ${file_name}

# Распаковка архива
echo 'Распаковка архива'
unzip hw1
mkdir input
mkdir cache
cp ./hw1\ 4/imp* input/
cp ./hw1\ 4/city* cache/

# удаление данных с hdfs
echo 'Удаленние данных с hdfs'
hdfs dfs -rm -r -f /user/root/output
hdfs dfs -rm -r -f /user/root/input
hdfs dfs -rm -r -f /user/root/cache

echo 'Создание папок для данных'
#Если запускаете в первый раз,то нужно откомментировать две нижней строчки 
#hdfs dfs -mkdir /user
#hdfs dfs -mkdir /user/root
hdfs dfs -mkdir /user/root/input
hdfs dfs -mkdir /user/root/cache

echo 'Копирование данных в hdfs'
hdfs dfs -put -f /root/Desktop/hw-1-4/input /user/root
hdfs dfs -put -f /root/Desktop/hw-1-4/cache /user/root

# Удаление локальных данных
echo 'Удаление локальных данных'
rm -rf ./__MACOSX/
rm -rf ./hw1\ 4/
rm -f hw1
rm -rf ./input
rm -rf ./cache

#
echo 'Пере-сборка проекта'
mvn clean package

echo 'Запуск программы'
#/opt/hadoop-2.10.0/bin/hadoop jar  "указать путь до jar файла и 3 аргумента:
#    1.папку с файлами img
#    2.папка куда будет записан результат
#    2.папку и файл с городами"

/opt/hadoop-2.10.0/bin/hadoop jar /root/Desktop/hw-1-4/target/hw-1-4-1.0.jar input output cache/city.en.txt

echo 'Вывод результата'
hdfs dfs -text output/part-00000

