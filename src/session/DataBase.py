# coding=utf-8
from threading import Thread
import psycopg2
import configurate as cnf

class Data_Base:
    """
    интерфейс БД на серваке
    """
    data_base_devices = None
    ER_MESSADE_ISNOT_SEND = "Сообщение не отправлено"
    def __init__(self):
        pass

    def mess_to_log(self, mess='some_error', data=''):
      help = 1
        # m = "Sv--> " + mess + '\n'
        # print(m, data)

    def connect(self):

        connect = psycopg2.connect(database=cnf.db.name, user=cnf.db.user, host=cnf.db.address, password=cnf.db.password)
        cursor = connect.cursor()

        cursor.execute("CREATE TABLE tbl(id_device INT, type JSON, permissions INT);")


