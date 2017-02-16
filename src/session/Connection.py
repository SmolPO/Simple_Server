# coding=utf-8
"""
подключение клиентов и устройств к серверу
"""

from threading import Thread
import socket
from itertools import count

import Configurate as cnf

from Handler import Handler

class Connect(Thread):
    app          = None
    sock         = None
    connect = None
    channel = None
    list_handler = {} # список всех подключенных пользователей # ??? добавить maxlen, так как у нас не может быть больше MAX_CONNECT
    id_clients = None

    def __init__(self, app):
        Thread.__init__(self)
        self.app         = app

        # инициализация генераторов
        self.id_clients = count()
        self.id_PP = count()
        self.id_clients.next()
        self.id_PP.next()

    def run(self):

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.bind(('', cnf.PORT))
            self.sock.listen(cnf.MAX_CONNECT)
        except:
            print ("connect except...")
            return

        try:
             while 1:
                # Проверка подключения. Первый пакет должен содержать пароль и логин.
                # Только после этого создается класс Client и весь последующий интерфейс
                print ("Start...")
                print (str(self.list_handler))
                conn, addr = self.sock.accept()
                print('Connection address:' + str(addr))
                # TODO проверка подключения
                if self.authentication(conn):
                    print ("Это наш клиент!!!")
                else:
                    print ("Плохой клиент!!!")
                    break
                print("check connect is good")
                handler = Handler(self, conn, cnf.CLIENT) # тип возвращается функцией аутотификации
                self.list_handler[next(self.id_clients)] = handler
                handler.start()
        finally:
            print ("sock.close....")
            self.sock.close()

    def authentication(self, conn, next_id=None):
        # принимает один байт для подтверждения
        # отправляет следующий номер либо из генератора id_clients, либо из id_PP

        data = conn.recv(2)
        if not data:
            return False

        if data == 'Cl':  # заглушка, замена аутотификации на сервере. Не забыть изменить!!!
            print("Client!")
            next_id = self.id_clients.next()

        elif data == 'PP':
            print('PP')
            next_id = self.id_PP.next()
            return True
        elif data:
            print("пришло что-то странное.... ")
            print (data)
            return False

        conn.send(bytes(next_id))
        return True

    def send_list_handler(self, conn):
        c = count()
        next(c)
        cnt = len(self.list_handler)
        for k in self.list_handler:
            if next(c) < cnt:
                conn.send(1)
            conn.send(k)
