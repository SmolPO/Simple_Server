# coding=utf-8
"""
Принимает байты
Преобразует к словарю
Кидает в очередь


"""

from threading import Thread
from collections import namedtuple
import pika

import GlobalQueue as glb_queue
from GlobalQueue import create_queue
import Configurate as cnf # экземпляр класса Config


class Recv_Handler(Thread):
    """
    прием сообщений с АРМ и добавление их в очередь.
    """
    is_connect = True # есть ли связь с клиентом
    sock       = None
    _log_ = None
    global_queue = None
    handler = None

    def __init__(self, handler):

        Thread.__init__(self)
        self.handler = handler

        self.sock    = self.handler.socket
        self.typr_handler  = self.handler.type_
        self.packet = cnf.init_ntuple_data_message()
        pass

    # преорабразование сообщения
    def parse_message(self, mess):
        """
        аналог функции from_bytes_to_data_mesage(), только разбивает байты
        не используется
        :param mess: bytes
        :return: ntuple_data_message, size_next_mess
        """
        id_ = int(mess[cnf.id_slise])
        cmd = int(mess[cnf.cmd_slice])
        sender = int(mess[cnf.sender_slice])
        receiver = int(mess[cnf.receiver_slice])
        size_next_mess = int(mess[cnf.size_next_mess_slice])
        data = int(mess[cnf.data_slice])


        return cnf.ntuple_data_message(id_, cmd, sender, receiver, data, size_next_mess), size_next_mess

    def parse_data(self, mess):
        """
        парсит последующие после ЗС сообщения
        не используется
        :param mess:
        :return: data, size
        """
        data = bytes(mess[cnf.data_slice])
        size_next = int(mess[cnf.size_next_mess_slice])

        return data, size_next

    # добавление в переменные класса
    def add_to_packet_from_main_header(self, main_hdr):

        self.packet = self.packet._replace (
                                            id        =main_hdr.id,
                                            cmd       =main_hdr.cmd,
                                            sender    =main_hdr.sender,
                                            recv      =main_hdr.recv,
                                            size_next =main_hdr.size_next,
                                            data      =main_hdr.data
                                            )

    def _add_datas_in_packet(self, data):

        if not data:
            data_to_str = ''
        else:
            data_to_str = ''.join(data) # преобразование списка в строку
        self.packet = self.packet._replace(data=(str(self.packet.data) + data_to_str))

        self.packet = cnf.ntuple_data_message(
            self.packet.id,
            self.packet.cmd,
            self.packet.sender,
            self.packet.recv,
            self.packet.size_next,
            str(self.packet.data) + data_to_str
        )

    # проверка сообщения
    def _check_mess(self, mess):
        """
        проверяет, что поля cmd и sender соответствуют данным из текущего пакета
        :param mess:
        :return:
        """
        if mess[0] != self.packet.id:
            print("dont match id")
            return False

        elif mess[1] != self.packet.cmd:
            print("dont match cmd")
            return False

        elif mess[2] != self.packet.sender:
            print("dont match sender")
            return False
        elif mess[3] != self.packet.recv:
            print("dont match receiver")
            return False

        return True

    def run(self):
        print (cnf.address_db)
        sending_to_queue = glb_queue.g_to_main_exchange()
        sending_to_queue.next()

        while self.is_connect:
            # инициализируем и чистим значения при последующих циклах
            pack = []  # сборка цепочки сообщений
            size_next_mess = 0
            current_message = cnf.init_ntuple_data_message()
            try:
                print ("waiting message...")
                recvd_msg = self.sock.recv(cnf.SIZE_HEADER)
            except:
                print ("reset connect")
                self.is_connect = False
                continue

            if not recvd_msg:
                print("reset connect")
                self.is_connect = False
                continue

            current_message = cnf.from_bytes_get_data_message(bytes(recvd_msg))

            if not current_message:
                print("reset connect")
                self.is_connect = False
                continue
            self.add_to_packet_from_main_header(current_message)
            while current_message.size_next > 0 and self.is_connect:
                current_message = cnf.from_bytes_get_data_message(bytes(self.sock.recv(size_next_mess)))
                if not current_message or self._check_mess(current_message):
                    print("dont have data or is not good. Close session...\n -->> ")
                    self.is_connect = False
                    continue
                else:
                    pack.append(current_message.data)

            # прием закончен, добавить в CacheClient

            self._add_datas_in_packet(pack)

            sending_to_queue.send(self.packet)
            print("Message is receiving ad add to queue....")
        if not self.is_connect:
            self._close_session_()
        else: return
        pass

    def _close_session_(self):
        self.handler.send_handler._close_session_()
        self.handler.connect.list_handlers.pop(self.handler.index_handler)
        self.sock.close()




