# coding=utf-8
"""
подключение клиентов и устройств к серверу
"""
import socket

from threading import Thread
from itertools import count

import configurate.Configurate as cnf
import Commands as CMD
from Handler import Handler

class Connect(Thread):
    app          = None
    sock         = None
    connect = None
    channel = None
    self_ID = 100
    list_handlers = {} # список всех подключенных пользователей # ??? добавить maxlen, так как у нас не может быть больше MAX_CONNECT
    id_clients = None

    def __init__(self, app):
        Thread.__init__(self)
        self.app = app

        # инициализация генераторов
        self.id_clients = count(1) # начальное значение
        self.id_PP = count(101)

    def run(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.bind(('', cnf.PORT))
            self.sock.listen(cnf.MAX_CONNECT)
        except:
            print ("connect except...")
            return False

        try:
             while 1:
                # Проверка подключения. Первый пакет должен содержать пароль и логин.
                # Только после этого создается класс Client и весь последующий интерфейс
                print ("Start...")
                print (str(self.list_handlers))
                conn, addr = self.sock.accept()
                print('Connection address:' + str(addr))
                if self.authentication_and_create_handler(conn):
                    print ("Это наш клиент!!!")
                else:
                    conn.close()
                    print ("Плохой клиент!!!")
                    continue
                print("check connect is good")
        finally:
            print ("sock.close....")
            self.sock.close()

    def authentication_and_create_handler(self, conn, next_id=None):
        # принимает один байт для подтверждения
        # отправляет следующий номер либо из генератора id_clients, либо из id_PP
        bytes_message = conn.recv(cnf.SIZE_HEADER)
        if not bytes_message:
            print("disconnect")
            return False
        try:
            message = cnf.to_data_message_from_bytes_2(bytes_message)
        except:
            return False

        if message.cmd:
            print("пришло что-то странное.... ")
            print (message)
            return False

        elif message.cmd == CMD.NEW_CLIENT:  # заглушка, замена аутотификации на сервере. Не забыть изменить!!!
            print("Client!")
            next_id = self.id_clients.next()
            self.create_client_handler(conn, next_id)

        elif message.cmd == CMD.NEW_PP:
            print('PP')
            next_id = self.id_PP.next()
            self.create_PP_handler(conn, next_id)

        answer = cnf.ntuple_data_message(0, CMD.GET_SELF_ID, self.self_ID, message.sender, 0, next_id)
        bytes_answer = cnf.to_bytes_from_data_message_2(answer)
        conn.send(bytes_answer) # TODO !!! ВНИМАНИЕ!!!
        return True

    def send_list_handler(self, conn):
        c = count()
        next(c)
        cnt = len(self.list_handlers)
        for k in self.list_handlers:
            if next(c) < cnt:
                conn.send(1)
            conn.send(k)

    def create_client_handler(self, conn, id_client):
        key_ =  "client_" + str(id_client)
        handler = Handler(self, conn, cnf.type_receivers['client'], id_client)  # тип возвращается функцией аутотификации
        self.list_handlers[key_] = handler
        handler.start()

    def create_PP_handler(self, conn, id_pp):
        key_ = str(id_pp) + "_pp"
        handler = Handler(self, conn, cnf.type_receivers['pp'], id_pp)  # тип возвращается функцией аутотификации
        self.list_handlers[key_] = handler
        handler.start()