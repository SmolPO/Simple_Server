# coding=utf-8
from threading import Thread
import psycopg2
import Configurate as cnf
from Configurate import _log_db_, _dvcs_db_, _op_db_, postgresql_data_base, user, password_for_DB, address_db

class Data_Bases:
    """
    интерфейс БД на серваке
    базы данных
    log_DB (sender, msg, date)
    devices_DB (id, type, position)
    operation_DB (msg (bytes), verify, date)
    """

    def __init__(self):
        self.init_postgressql()
        self.init_devices_DB()
        self.init_log_DB()
        self.init_operations_DB()
        pass

    def init_postgressql(self):
        self.postgresql_connect = psycopg2.connect(database=postgresql_data_base,
                                   user=user,
                                   host=address_db,
                                   password=password_for_DB)
        self.cursor = self.postgresql_connect.cursor()
        self.cursor.execute("CREATE TABLE log_db (sender INT, message JSON, date DATE);")
        self.cursor.execute("CREATE TABLE devices_db(id_device INT, type JSON, location POINT);")
        self.cursor.execute("CREATE TABLE operations_db(message BYTES, verify INT, date DATE);")

    def to_log_DB(self, msg):
        """
        :param msg: (sender, message, date)
        :return:
        """
        pass

    def to_devices_DB(self, msg):
        """

        :param msg: (id_device, type, location)
        :return:
        """
        pass

    def to_operation_DB(self, msg):
        """
        :param msg: (message, verify, date)
        :return:
        """
        pass

    def set_verify_for_msg(self, id):
        """
        найти по id посылки запись и изменить флаг на 'исполнено'
        :param id: id посылки
        :return:
        """
        pass

    def add_to_datebases(self, body):
        """
        проанализировать сообщение и определить в какие базы его нужно добавить
        :return:
        """
        pass

