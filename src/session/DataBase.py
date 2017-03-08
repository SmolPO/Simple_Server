# coding=utf-8
from threading import Thread
import psycopg2

import Configurate as cnf
from Configurate import nt_log_db_, nt_devices_db_, nt_operation_db_, postgresql_data_base, user, password_for_DB, address_db, FL_DEVICES_DB, FL_OPERATION_DB, FL_LOG_DB, STATUS
from Commands import step_comands
import datetime
class Data_Bases:
    """
    интерфейс БД на серваке
    базы данных
    """

    def __init__(self):
        self.init_postgressql()
        pass

    def init_postgressql(self):
        return
        self.postgresql_connect = psycopg2.connect(database=postgresql_data_base,
                                   user=user,
                                   host=address_db,
                                   password=password_for_DB)
        self.cursor = self.postgresql_connect.cursor()
        self.cursor.execute("CREATE TABLE log_db (sender INT, message JSON, date DATE);")
        self.cursor.execute("CREATE TABLE devices_db (id_device INT, type JSON, location POINT, date DATE);")
        self.cursor.execute("CREATE TABLE operations_db (id INT, sender INT, message BYTES, verify INT, date DATE);")

    def add_to_datebases(self, message):
        """
        проанализировать сообщение и определить в какие базы его нужно добавить
        1) определить, сообщение - ответ или это новая команда
            ответ - найти в базе, изменить статус
            новая команда - добавить
        2) определить куда добавлять
        :return:
        """
        if self.is_find_it_message_in_operation_DB(message):
            status = self.get_status_operation(message)
            self.set_verify_for_msg(message, status)  # TODO не всегда ответное сообщение означает, что команда выполнена!!!
            # еще какая то логика...
            pass
            return
        if self.is_update_device(message):
            self._to_operation_DB_(message)

        if self.is_log_message(message):
            self._to_log_DB_(message)

    ### --- Добавить в таблицу --- ###
    def _to_log_DB_(self, message):
        """
        :param message: (sender, message, date)
        :return:
        """
        date = self.get_time()
        execute = "INSERT INTO {0} ({1}, {2}, {3}) values({4}, {5}, {6})".format(
            nt_log_db_.name,
            FL_LOG_DB.sender,
            FL_LOG_DB.msg,
            FL_LOG_DB.date,
            message.sender,
            message.data, # текст сообщения передается в поле data
            date
            )
        self.cursor.execute(execute)
        pass

    def _to_devices_DB_(self, message):
        """
        добавить в базу данных
        :param message: (id_device, type, location)
        :return:
        """
        date = self.get_time()
        type_device = self.get_type_device(message)
        execute = "INSERT INTO {0} ({1}, {2}, {3}, {4}) values({5}, {6}, {7}, {8})".format(
            nt_devices_db_.name,
            FL_DEVICES_DB.sender,
            FL_DEVICES_DB.type,
            FL_DEVICES_DB.position,
            FL_DEVICES_DB.date,
            message.sender,
            type_device,
            message.data,
            date
            )
        self.cursor.execute(execute)
        pass

    def _to_operation_DB_(self, message):
        """
        :param message: (id, sender, message, verify, date)
        :return:
        """
        type_device = self.get_type_device(message)
        execute = "INSERT INTO {0} ({1}, {2}, {3}, {4}, {5}) values({6}, {7}, {8}, {9}, {10})".format(
            nt_operation_db_.name,
            FL_OPERATION_DB.id,
            FL_OPERATION_DB.sender,
            FL_OPERATION_DB.message,
            FL_OPERATION_DB.verify,
            FL_OPERATION_DB.date,
            message.id,
            message.sender,
            message,
            STATUS['waiting'],
            message.data,
            )
        self.cursor.execute(execute)
        pass


    ### --- Получить данные --- ####
    def get_type_device(self, message):
        """
        определет тип устройства
        :param message:
        :return:
        """
        return message.cmd // step_comands

    def get_time(self):
        return datetime.datetime.now()

    def get_status_operation(self, message):
        """
        определяет статус операции
        :param message:
        :return: STATUS
        """
        return STATUS['ok']

    def is_update_device(self, message):
        """
        надо ли внести изменения в таблицу устройств (появилось новое, изменился статус, что-то еще)
        :param message:
        :return:
        """
        return True

    def is_log_message(self, message):
        if message.cmd == 400:
            return True
        return False


    ### ---- Запроосы --- ###
    def is_find_it_message_in_operation_DB(self, message):
        execute = "SELECT {0} FROM {1} WHERE {0} = {2}".format(
            FL_OPERATION_DB.id,
            nt_operation_db_.name,
            message.id
        )
        result = self.cursor.execute(execute)
        return bool(result)


    def set_verify_for_msg(self, id_message, status='ok'):
        """
        найти по id посылки запись и изменить флаг на 'исполнено'
        :param id_message: id посылки
        :return:
        """
        execute = "UPDATE {0} SET {1} = {2} WHERE {2} = {3}".format(
            nt_devices_db_.name,
            FL_OPERATION_DB.id,
            STATUS[status],
            FL_OPERATION_DB.id,
            id_message
        )
        pass
