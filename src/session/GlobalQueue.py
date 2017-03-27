# coding=utf-8
# интерфейс очереди

from itertools import count
import pika

import configurate.Configurate as cnf
from configurate.Configurate import *
from DataBase import *

id_queue = count()
# генераторы
def _log_():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=address_rbbmq))
    channel = connection.channel()
    channel.queue_declare(queue=log_queue_name)
    exchange = ''
    while True:
        msg = yield
        if not msg:
            continue
        channel.basic_publish(
            exchange='',  # точка обмена
            routing_key=log_queue_name,  # имя очереди
            body=msg
        )

# инизиализация логирования  # TODO изучить способы логирования в БД

g_my_log = _log_()
next(g_my_log)

def g_to_main_exchange():
    """
    добавляет в очередь сообщение в байтах
    :param point:
    :return:
    """
    print("init g_to_main_exchange")
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=cnf.server_global_queue_name)
    exchange = ''

    while True:
        packet = yield
        try:
            packet = cnf.to_bytes_from_data_message(packet)
            channel.basic_publish(
                exchange=exchange,  # точка обмена
                routing_key=server_global_queue_name,  # имя очереди
                body=packet
            )
            print("to queue is ok")
        except:
            g_my_log.send("to_queue: exit..." + str(packet))
            break
    return

def create_queue(name=cnf.server_global_queue_name, address=cnf.queue_addr, delivery_mode=2):
    connect = pika.BlockingConnection(pika.ConnectionParameters(address))
    channel = connect.channel()
    queue_ = cnf.ntuple_queue(
        name,
        address,
        connect,
        channel,
        pika.BasicProperties(delivery_mode=delivery_mode)
    )
    queue_.chanl.queue_declare(queue=name)
    print("Connect to queue...", (name, address))
    return queue_


def init_rabbitmq(address=cnf.queue_addr):
    """
    запускается при if "__name__" == __main__:
    :param address:
    :return:
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(address))
    channel = connection.channel()

    channel.exchange_declare(exchange=exchange_cl, type='direct')
    channel.exchange_declare(exchange=exchange_pp, type='direct')

    # для отладки TODO
    channel.queue_declare(queue=cnf.server_global_queue_name)
    channel.queue_declare(queue=cnf.log_queue_name)

    return connection, channel


class Server_Thread(Thread):
    """
    подключится к глобальной очереди
    получать из нее сообщения
    анализировать их
        передать в точку доступа клиентов
        передать в точку достпуа ПП
    """
    def __init__(self):
        Thread.__init__(self)
        # подключение к базам данных
        self.DB = Data_Bases()
        self.connection, self.channel = init_rabbitmq()

    def run(self):
        self.channel.basic_consume(
            self._callback_,
            queue=server_global_queue_name,
            no_ack=True
        )
        self.channel.start_consuming()
        pass

###--- отправки сообщения в очереди ---###
    # отправка в очередь для ПП
    def _add_to_queue_pp_(self, message, rout_key):
        """

        :param message: сообщение
        :param rout_key:
        :return:
        """
        print ("to queue pp")
        name_queue_pp = "pp" + "_" + str(rout_key)
        print ("to queue pp")
        self.channel.basic_publish(
            exchange=exchange_pp,  # точка обмена
            routing_key=name_queue_pp,  # имя очереди
            body=message
        )
        print ("->> to queue client" + str(name_queue_pp) + "_" + str(message))
        return

    # отправка в очередь для клиентов
    def _add_to_queue_clients(self, message, rout_key):

        name_queue_client = "client" + "_" + str(rout_key)
        self.channel.basic_publish(
            exchange=exchange_cl,  # точка обмена
            routing_key=name_queue_client,  # имя очереди
            body=message
        )
        print ("->> to queue client: " + str(name_queue_client) + " " + str(message))
        pass

###--- получение данных из сообщения ---###
    # получение из сообщений команды
    def _get_cmd_(self, message):
        return message.cmd

    def _get_rout_key_(self, mess):
        """
        узнать получателя сообщения и вернуть имя его очереди
        :param mess:
        :return: rout_key из сообщения mess
        """
        receiver = mess.recv # TODO возможн здесь будет другой способ получения ключа из recv
        rout_key = receiver
        return rout_key

    def get_type_receiver(self, body):
        """
        возвращает тип устройства (сервер, клиент, ПП)
        :param cmd:
        :return:
        """
        return cnf.type_receivers['pp'] if body.recv > 100 else cnf.type_receivers['client']
        # cmd = body.cmd
        # if cmd // CMD.step_comands == CMD.Commands().ClnCmd().index_commands:
        #     return cnf.type_receivers['client']
        # elif cmd // CMD.step_comands == CMD.Commands().PPCmd().index_commands:
        #     return cnf.type_receivers['pp']
        # elif cmd // CMD.step_comands == CMD.Commands().SrvCmd().index_commands:
        #     return cnf.type_receivers['server']
        # return 'Did not find type!!!!'

###--- прочие ---###
    def _callback_(self, ch, method, properties, body):
        """
        анализ сообщения
        :param message: сообщение в кодировке
        :return:

        """
        print ("Global queue callback")
        # добавить в базы данных
        self.DB.add_to_databases(body) # добавление в БД, скрыта вся логика добавления и фиксирования ответа в БД
        message = cnf.to_data_message_from_bytes_(body)
        cmd, rout_key, who_type_receiver = self._get_cmd_(message), self._get_rout_key_(message), self.get_type_receiver(message)
        if who_type_receiver == cnf.type_receivers['client']:
            print ("add to queue client")
            self._add_to_queue_clients(body, rout_key)
            self._basic_ask()
            return

        elif who_type_receiver == cnf.type_receivers['pp']:
            print ("add to queue pp")
            self._add_to_queue_pp_(body, rout_key)
            self._basic_ask()
            return

        elif who_type_receiver == cnf.type_receivers['server']:
            print ("for server")
            self._basic_ask()

    def _basic_ask(self):
        print ("message is receive....")
        pass

    def _close_session_(self):
        self.channel.queue_delete(queue=cnf.server_global_queue_name)
        self.channel.close()
        print ("global queue is remove..")

        pass

if __name__ == '__main__':
    init_rabbitmq()