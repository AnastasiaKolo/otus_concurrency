# ДЗ-9: MemcLoad

## Описание/Пошаговая инструкция выполнения домашнего задания:
Реализуем конкурентную заливку данных в memcache'ы

## Задание
: нужно переделать однопоточную версию memc_load.py в болеепроизводительный вариант. Сам скрипт парсит и заливает в мемкеш поминутную выгрузку логов трекера установленных приложений. Ключом является тип иидентификатор устройства через двоеточие, значением являет protobuf сообщение.
$ ls -lh /Users/s.stupnikov/Coding/otus/*.tsv.gz
-rw-r--r-- 1 s.stupnikov staff 506M 29 сен 12:09 /Users/s.stupnikov/Codin
-rw-r--r-- 1 s.stupnikov staff 506M 29 сен 12:17 /Users/s.stupnikov/Codin
-rw-r--r-- 1 s.stupnikov staff 506M 29 сен 12:25 /Users/s.stupnikov/Codin
$ gunzip -c /Users/s.stupnikov/Coding/otus/20170929000000.tsv.gz | head -3
idfae7e1a50c0ec2747ca56cd9e1558c0d7c67.7835424444-22.80440054
idfaf5ae5fe6122bb20d08ff2c2ec43fb4c4-104.68583244-51.24448376g
aid3261cf44cbe6a00839c574336fdf49f6137.79083956756.840367524

Ссылки на tsv.gz файлы:
https://cloud.mail.ru/public/2hZL/Ko9s8R9TA
https://cloud.mail.ru/public/DzSX/oj8RxGX1A
https://cloud.mail.ru/public/LoDo/SfsPEzoGc

Пример запуска
$ python memc_load.py --pattern=/Users/s.stupnikov/Coding/otus/*.tsv.gz --dry
[2017.09.29 12:39:52] I Memc loader started with options: {'dry': True, 'log'
[2017.09.29 12:39:52] I Processing /Users/s.stupnikov/Coding/otus/20170929000
[2017.09.29 12:39:52] D 127.0.0.1:33013 - idfa:e7e1a50c0ec2747ca56cd9e1558c0d
[2017.09.29 12:39:52] D 127.0.0.1:33013 - idfa:f5ae5fe6122bb20d08ff2c2ec43fb4
[2017.09.29 12:39:52] D 127.0.0.1:33014 - gaid:3261cf44cbe6a00839c574336fdf49

## Запуск приложения
Установите зависимости: pip install -r requirements.txt
Запустите memcached в docker-контейнере, выполнив команду: docker-compose up -d
Запустите приложение: 
- однопоточная версия memc_load.py
- многопоточная версия memc_load_parallel.py

Укажите параметры конфигурации в командной строке. 

python source/memc_load.py --pattern pattern --log log

| Name        | Description                           | Default value          |
|-------------|---------------------------------------|------------------------|
| pattern     | Шаблон имени файлов для обработки     | ./data/appsinstalled/*.tsv.gz                   |
| log         | Имя файла лога работы данного скрипта | None (вывод в консоль) |


## Реализовано в рамках задачи 
(исходный скрипт source/memc_load.py, новыe скрипты source/memcload_parallel.py и source/memc_client.py):

- persistent connection to memcached
- retry and timeouts
- запуск загрузки в параллельных потоках

Ускорение работы замеряла на файле 200 тыс строк:
memc_load.py - 15 мин
memc_load_parallel.py - 1 мин 52 сек
