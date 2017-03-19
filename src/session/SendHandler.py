# coding=utf-8
"""
Принимает сообщение в виде словаря из очереди по требуемому ключу
Собирает пакет
Отправляет

"""

import socket
import pika
from threading import Thread

import Configurate as cnf
import GlobalQueue as glb_queue

from GlobalQueue import id_queue

class Send_Handler(Thread):

    handler = None
    sock = None
    type_handler = None # client or PP

    def __init__(self, handler):
        Thread.__init__(self)

        self.handler = handler or None
        self.sock    = handler.socket or None
        self.type_handler = handler.type_ or None

        self.name_queue = self._create_name_queue_()

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.name_queue)
        self.channel.basic_consume(
            self.callback,
            queue=self.name_queue,
            no_ack=True
        )

    def run(self):
        self.channel.start_consuming()
        print ("stop consumer")
        # создать очереди и привязать ее к точке обмена
        pass

    def send_mess(self, mess):
        """
        возможно нужны какие-то преобразования сообщения
        :param mess: [<байты>]
        :return: true or false
        """
        print("sending...")
        print(str(self.sock.send(mess)))
        return True

    def callback(self, ch, method, properties, body):
        """
        :param message: сообщение в кодировке
        :return:

        """
        print("callback send handler")
        if not self.send_mess(body):
            # ошибка отправки
            print("reset connect callback")
            self.close_session()
            return
        print("callback is ok")
        pass

    def basic_ask(self):
        print("basic_ask")
        pass

    # внутренние функции
    def _create_name_queue_(self):
        """
        :return:
        """
        return str(self.handler.type_) + "_" + str(next(id_queue))

    def close_session(self):
       # self.handler.connect.list_handler.remove(self.handler)
        self.channel.queue_delete(queue=self.name_queue)
        self.sock.close()
        self.channel.close()

        print("reset connect. send heandler")

if __name__ == '__main__':
    snd = Send_Handler(1)
