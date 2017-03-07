# coding=utf-8
# интерфейс очереди

from itertools import count
from threading import Thread
import logging
import pika

import Commands as CMD
import Configurate as cnf

from Configurate import *
from DataBase import *

id_queue = count()

def __log__():

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
    return

# инизиализация логирования  # TODO изучить способы логирования в БД
                             # TODO кто слушает __log__ ???
my_log = __log__()
my_log.next()

def to_queue(point=None, flag=False):
    """
    добавляет в очередь сообщение в байтах
    генератор!!!
    :param packet:
    :return:
    """

    print("to queue...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=cnf.glob_queue_name)
    exchange = ''

    while True:
        packet = yield

        try:
            if not flag:
                packet = cnf.to_bytes_from_data_message(packet)

            if point:
                exchange = point

            channel.basic_publish(
                exchange=exchange,  # точка обмена
                routing_key=glob_queue_name,  # имя очереди
                body=packet
            )
            print("to queue is ok")
        except:
            my_log.send("to_queue: exit..." + str(packet))
            break
    return

def create_queue(name=cnf.glob_queue_name, address=cnf.queue_addr, delivery_mode=2):
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
    conn = pika.BlockingConnection(pika.ConnectionParameters(address))
    chanl = conn.channel()

    chanl.exchange_declare(exchange=exchange_cl, type='direct')
    chanl.exchange_declare(exchange=exchange_pp, type='direct')

    chanl.queue_declare(queue="client_0")
    chanl.queue_declare(queue=cnf.glob_queue_name)
    chanl.queue_declare(queue=cnf.log_queue_name)
    chanl.queue_bind("client_0", "clients")

    return conn, chanl


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
        # подключится к очереди
        # создать две точки обмена

        # создать точки обмена
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=glob_queue_name)
        self.channel.queue_declare(queue=log_queue_name)
        self.channel.queue_declare(queue='client_0') # TODO жесткое задание имени

        # точки обмена
        self.channel.exchange_declare(exchange='clients', type='direct')
        self.channel.exchange_declare(exchange='pp'     , type='direct')

        # привязать очередь к точке обмена
        self.channel.queue_bind("client_0", "clients")

    def run(self):
        self.channel.basic_consume(
            self._callback_,
            queue=glob_queue_name,
            no_ack=True
        )
        self.channel.start_consuming()
        pass


    # отправка в очередь для ПП
    def _to_queue_pp(self, mess, rout_key):
        """
        возможно нужны какие-то преобразования сообщения
        :param mess: [<байты>]
        :return: true or false
        """
        print ("to queue pp")
        self.channel.basic_publish(
            exchange=exchange_pp,  # точка обмена
            routing_key=rout_key,  # имя очереди
            body=mess
        )
        return

    # отправка в очередь для клиентов
    def _to_queue_clients(self, mess, rout_key):
        print ("to queue client")
        self.channel.basic_publish(
            exchange=exchange_cl,  # точка обмена
            routing_key=rout_key,  # имя очереди
            body=mess
        )
        pass

    # получение из сообщений команды
    def _get_cmd(self, mess):
        message = from_bytes_get_data_message(mess)
        print ("return cmd = " + str(message.cmd))
        return message.cmd

    def _get_rout_key(self, mess):
        """
        узнать получателя сообщения и вернуть имя его очереди
        :param mess:
        :return: rout_key из сообщения mess
        """
        message = from_bytes_get_data_message(mess)
        reciever = message.recv # TODO возможно здесь будет другой способ получения ключа из recv
        rout_key = reciever
        return rout_key

    # не используется
    def _is_private_message(self, mess):
        """
        сообщение для сервера (лог, подтверждение команды или еще что-то)
        определеятся по номеру команды
        :param mess:
        :return:
        """
        return False

    def _callback_(self, ch, method, properties, body):
        """
        анализ сообщения
        :param message: сообщение в кодировке
        :return:

        """
        print ("Global queue callback")
        # добавить в базы данных
        self.DB.add_to_datebases(body)

        if self._is_private_message(body):
            return

        cmd, rout_key = self._get_cmd(body), self._get_rout_key(body)
        if is_clients_cmd(cmd):
            print ("add to queue client")
            self._to_queue_clients(body, rout_key)
            self._basic_ask() # TODO зачем???
            return

        elif is_pps_cmd(cmd):
            print ("add to queue pp")
            self._to_queue_pp(body, rout_key)
            self._basic_ask() # TODO зачем???
            return

        return

    def _basic_ask(self):
        print ("message is receive....")
        pass

    def close_session(self):
        # TODO остановить свой поток
        self.channel.queue_delete(queue=cnf.glob_queue_name)
        print ("global queue is remove..")

        pass

if __name__ == '__main__':
    init_rabbitmq()