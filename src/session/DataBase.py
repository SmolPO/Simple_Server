# coding=utf-8
import psycopg2
from threading import Thread
from Configurate import nt_log_db_, nt_devices_db_, nt_operation_db_, postgresql_data_base, user, password_for_DB, address_db, FL_DEVICES_DB, FL_OPERATION_DB, FL_LOG_DB, STATUS
from Commands import step_comands
import datetime
class Data_Bases(Thread):
    """
    интерфейс БД на серваке
    базы данных
    """

    def __init__(self):
        Thread.__init__(self)
        self.init_postgressql()
        pass

    def run(self):
        """
        проверяет базы данных на неотвечанные сообщения
        проверяет БД раз в t секунд (или может это можно сделать как то не так)
        :return:
        """
        ### проверка базы данных операций
        # найти все записи со статусом waiting и определить давность операции
        # если время больше таймаута, то изменить на 'not_answer' и что-нибудь сделать (например, послать ':(' отправителю)))
        execute = "SELECT * FROM {0} WHERE {1} == {2}".format(
            nt_operation_db_.name,
            FL_OPERATION_DB.verify,
            STATUS['waiting']
        )
        self.cursor.execute(execute)
        TIMEOUT = 10000
        rows = self.cursor.fetchall()
        for row in rows:
      #      if datetime.timedelta(row[4], datetime.datetime.now()) > TIMEOUT:
             print (str(row))
             pass

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
        if self._is_find_it_message_in_operation_DB_(message):
            status = self._get_status_operation_(message)
            self._set_verify_for_msg_(message, status)  # TODO не всегда ответное сообщение означает, что команда выполнена!!!
            # еще какая то логика...
            pass
            return
        if self.is_update_device(message):
            self._add_to_operation_DB_(message)

        if self._is_log_message_(message):
            self._add_to_log_DB_(message)

    ### --- Добавить в таблицу --- ###
    def _add_to_log_DB_(self, message):
        """
        :param message: (sender, message, date)
        :return:
        """
        date = self._get_time_()
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

    def _add_to_devices_DB_(self, message):
        """
        добавить в базу данных
        :param message: (id_device, type, location)
        :return:
        """
        date = self._get_time_()
        type_device = self._get_type_device_(message)
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

    def _add_to_operation_DB_(self, message):
        """
        :param message: (id, sender, message, verify, date)
        :return:
        """
        type_device = self._get_type_device_(message)
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
    def _get_type_device_(self, message):
        """
        определет тип устройства
        :param message:
        :return:
        """
        return message.cmd // step_comands

    def _get_time_(self):
        return datetime.datetime.now()

    def _get_status_operation_(self, message):
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

    def _is_log_message_(self, message):
        if message.cmd == 400:
            return True
        return False


    ### ---- Запроосы --- ###
    def _is_find_it_message_in_operation_DB_(self, message):
        execute = "SELECT {0} FROM {1} WHERE {0} = {2}".format(
            FL_OPERATION_DB.id,
            nt_operation_db_.name,
            message.id
        )
        result = self.cursor.execute(execute)
        return bool(result)

    def _set_verify_for_msg_(self, id_message, status='ok'):
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
# формировние запроса
# fields = ', '.join(my_dict.keys())
# values = ', '.join(['%%(%s)s' % x for x in my_dict])
# query = 'INSERT INTO some_table (%s) VALUES (%s)' % (fields, values)
# cursor.execute(query, my_dict)