# coding=utf-8
"""
из документа about configurate

1) Первая - заголовочная структура
    Поля:
    0) number (пока нет)
    1) cmd
    2) sender
    3) data
    4) size_next_mess
    5) общий объем сообщения

Максимальный размер сообщения - 1024 байта
Все поля сообщения заполняются по 4 байта.
Один тип сообщений

"""

from collections import namedtuple, deque
import Commands as CMD

# типы пользователей
PP = 'pp'
CLIENT = 'clients'

# размеры полей в байтах
SIZE_FIELD     = 4 # универсальный размер поля для всех полей

SIZE_ID        = 4 # размер поля под id - номер сообщения в пакете
SIZE_CMD       = 4 # размер поля под команду
SIZE_SENDER    = 4 # размер поля SENDER
SIZE_RECV      = 4 # размер поля для RECEIVER
SIZE_DATA      = 4 # размер под DATA в ЗС
SIZE_NEXT_PACK = 4 # сколько байт в конце посылки отвечают за размер следующего сообщения
COUNT_FIELDS   = 4 # cmd, sender, data, size_next_mess

SIZE_HEADER    = SIZE_ID + SIZE_CMD + SIZE_SENDER + SIZE_RECV + SIZE_DATA + SIZE_NEXT_PACK # размер первого сообщения
MIN_SIZE_MESS  = SIZE_FIELD * COUNT_FIELDS

SIZE_MAX_LEN   = 1024  # максимальный размер сообщения

ntuple_data_message = namedtuple("msg",         ['id', 'cmd', 'sender', 'recv', 'size_next', 'data'])
ntuple_main_header  = namedtuple("main_header", ['id', 'cmd', 'sender', 'recv', 'size_next'        ])
ntuple_queue        = namedtuple("queue",       ['name', 'addr', 'conn', 'chanl', 'prop'           ])

# слайсы для распаривания
id_slise             = slice(0, SIZE_ID)
cmd_slice            = slice(SIZE_ID ,
                             SIZE_ID + SIZE_CMD)
sender_slice         = slice(SIZE_ID + SIZE_CMD,
                             SIZE_ID + SIZE_CMD + SIZE_SENDER)
receiver_slice       = slice(SIZE_ID + SIZE_CMD + SIZE_SENDER,
                             SIZE_ID + SIZE_CMD + SIZE_SENDER + SIZE_RECV)
size_next_mess_slice = slice(SIZE_ID + SIZE_CMD + SIZE_SENDER + SIZE_RECV,
                             SIZE_ID + SIZE_CMD + SIZE_SENDER + SIZE_RECV + SIZE_NEXT_PACK)
data_slice           = slice(SIZE_ID + SIZE_CMD + SIZE_SENDER + SIZE_RECV + SIZE_NEXT_PACK, None)

#коды ошибок
ERR_SEND_TO_ARM = 1
ERR_RECV = 2
ERR_     = 3

# константы для соединения (продублированы в globals_variables как поля класа)
PORT = 27000
server_address = 'localhost'
MAX_CONNECT = 10
BUFFER_SIZE = 30

# кодировка сообщений
TYPE_CODING = dict(utf_8='utf-8', ackii='ackii')

# для очереди
queue_name = "Global_Queue"
queue_addr = 'localhost'

log_queue_name = "Log_Queue"
log_queue_addr = 'localhost'

key_recv   = "key_recv"
key_send   = "key_send"
password   = "6EQUJ511_1"

# для базы данных
DB_DEVICE = namedtuple("DATa_BASE_DEVICES", ['name', 'address', 'user', 'password', 'fields'])
db = DB_DEVICE('LC_device', 'localhost', 'LC', '12345', ['id', 'type', 'permissions'])

ntuple_db_device = namedtuple("db_device_", ['id','type', 'rights'])
ntuple_db_operation = namedtuple("db_operation_", ['id','sender', 'reciver', 'status'])
STATUS = {'ok':0, 'waiting':1, 'not_answer':2}
user = 'LC'

def to_bytes_from_data_message(data, size_field=4, end_symbol ='0', charset="utf-8", more=False):
    """

    :param data: ntuple_data_message
    :param size_field:
    :param end_symbol:
    :param charset:
    :param more:
    :return: bytes(cmd_ + sender_ + data_ + size_next_mess_)
    """

    #if(not more and len(data) >= size_field):  #???? что это????
     #   raise Exception("Переполнение поля")

    id_             = str(data.id).rjust(size_field, end_symbol).       encode(charset)
    cmd_            = str(data.cmd).rjust(size_field, end_symbol).      encode(charset)
    sender_         = str(data.sender).rjust(size_field, end_symbol).   encode(charset)
    receiver_       = str(data.recv).rjust(size_field, end_symbol).     encode(charset)
    size_next_mess_ = str(data.size_next).rjust(size_field, end_symbol).encode(charset)
    data_           = str(data.data).rjust(size_field, end_symbol).     encode(charset)

    return bytes(id_ + cmd_ + sender_ + receiver_ + size_next_mess_ + data_)


def from_bytes_get_data_message(by_data, size_field=4, charset="utf-8"):
    """
     проверяет:
     - длинна больше минимально возможной
     - тип входных данных

    :param by_data: bytes
    :param size_field:
    :param charset:
    :return: ntuple_data_message
    """

    #if(type(by_data) != bytes):
     #   raise Exception("Неверный тип")

#    if(len(by_data) < MIN_SIZE_MESS):

 #       raise Exception("Размер меньше размера минимального размера сообщения")

    str_res = str(by_data.decode(charset))

    id_        = int(str_res[id_slise])
    cmd_       = int(str_res[cmd_slice])
    sender_    = int(str_res[sender_slice])
    receiver_  = int(str_res[receiver_slice])
    size_next_ = int(str_res[size_next_mess_slice])
    data_      = int(str_res[data_slice])


    return ntuple_data_message(id_, cmd_, sender_, receiver_, size_next_, data_)

def init_ntuple_data_message():
    return ntuple_data_message(None, None, None, None, None, None)

def is_clients_cmd(cmd):
    if 0 < cmd < CMD.CLIENTs_CMD:
        return True
    else:
        return False


def is_pps_cmd(cmd):
    if CMD.CLIENTs_CMD < cmd < CMD.PPs_CMD:
        return True
    else:
        return False
