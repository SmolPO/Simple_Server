# coding=utf-8
import psycopg2
from threading import Thread
from Configurate import nt_log_db_, nt_devices_db_, nt_operation_db_, \
    postgresql_data_base, user, password_for_DB, address_db, \
    _FIELDS_DEVICES_DB, _FIELDS_JOURNAL_DB, _FIELDS_LOG_DB, STATUS, TIMEOUT_WAIT_ANSWER, ntuple_data_message

from Commands import step_comands, Commands
import datetime
class Data_Bases(Thread):
    """
    интерфейс БД на серваке
    базы данных
    """

    def __init__(self):
        Thread.__init__(self)
        self.init_postgressql()
        self.is_checking_db = True
        pass

    def run(self):
        """
        проверяет базы данных на неотвечанные сообщения
        проверяет БД раз в t секунд (или может это можно сделать как то не так)
        лучше добавлять сообщения в какой-то буфер (RabbitMQ) и поставить обработчик,
        который получает сообщение, только если оно находится там N секунд
        :return:
        """
        ### проверка базы данных операций
        # найти все записи со статусом waiting и определить давность операции
        # если время больше таймаута, то изменить на 'not_answer' и что-нибудь сделать (например, послать ':(' отправителю)))
        # TODO есть ли такой функционал в RabbitMQ
        # подключится

    def init_postgressql(self):

        self.postgresql_connect = psycopg2.connect(database=postgresql_data_base,
                                   user=user,
                                   host=address_db,
                                   password=password_for_DB)
        self.cursor = self.postgresql_connect.cursor()
        self._test_()

    def _test_(self):
        # self._add_to_devices_DB_(message)
        # self._add_to_log_DB_(message)
        # self._add_to_operation_DB_(message)
        # self.cursor.execute("SELECT * FROM log_db")
        # result = self.cursor.fetchall()
        # self.cursor.execute("SELECT * FROM devices_db")
        # result = self.cursor.fetchall()
        # self.cursor.execute("SELECT * FROM operations_db")
        # result = self.cursor.fetchall()
        # print(result)
        pass

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
        elif self.is_update_device(message):
            self._add_to_devices_DB_(message)
            self._add_to_journal_DB_(message)

        elif self._is_log_message_(message):
            self._add_to_log_DB_(message)

    ### --- Добавить в таблицу --- ###
    def _add_to_log_DB_(self, message):
        """
        :param message: (sender, message, date)
        :return:
        """
        fields = ', '.join(['%s' % x for x in _FIELDS_LOG_DB])
        list_values = [message.sender, message]
        values = ', '.join(['%s' % x for x in list_values])
        query = "INSERT INTO {0} ({1}) VALUES ({2}, '{3}')".format(nt_log_db_.name, fields, values, self._get_time_())
        self.cursor.execute(query)
        pass

    def _add_to_devices_DB_(self, message):
        """
        добавить в базу данных
        :param message: (id_device, type, location, date)
        :return:
        """
        fields = ', '.join(['%s' % x for x in _FIELDS_DEVICES_DB])
        list_values = [message.sender, self._get_type_device_(message), self.get_location(message)]
        values = ', '.join(['%s' % x for x in list_values])
        query = "INSERT INTO {0} ({1}) VALUES ({2}, '{3}')".format(nt_devices_db_.name, fields, values, self._get_time_())
        self.cursor.execute(query)
        pass

    def _add_to_journal_DB_(self, message):
        """
        :param message: (id, sender, message, verify, date)
        :return:
        """

        fields = ', '.join(['%s' % x for x in _FIELDS_JOURNAL_DB])
        list_values = [message.sender, message, STATUS['waiting']]
        values = ', '.join(['%s' % x for x in list_values])
        query = "INSERT INTO {0} ({1}) VALUES ({2}, '{3}')".format(nt_operation_db_.name, fields, values, self._get_time_())
        self.cursor.execute(query)
        pass

    ### --- Получить данные --- ####
    def _get_type_device_(self, message):
        """
        определет тип устройства
        :param message:
        :return:
        """
        return "client"
    #    return message.cmd // step_comands

    def _get_time_(self):
         return "2017-03-11-20-45-59"
        # return str(datetime.datetime.now()).replace(' ', '-')

    def _get_location_(self, message):
        """
        пока пусть будет такой способ 0000, где первые два числа - широта, вторые - долгота
        :param message:
        :return:
        """
        return int(message.date, 10) // 100, int(message.date) % 100

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
        if message.cmd == Commands().ClnCmd().CONNECT_CLIENT:
            return True
        return False

    def _is_log_message_(self, message):
        if message.cmd == 400:
            return True
        return False


    ### ---- Запроосы --- ###
    def _is_find_it_message_in_operation_DB_(self, message):
        execute = "SELECT {0} FROM {1} WHERE {0} = {2} AND ".format(
            _FIELDS_JOURNAL_DB,
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

        pass
