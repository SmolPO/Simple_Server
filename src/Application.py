# coding=utf-8
from threading import Thread

from Connection import Connect as Conn
from GlobalQueue import Server_Thread as svr_thread
# TODO убрать import * или import as , заменить на полный импорт

class Application(Thread):
    connect_class = None
    server_thread = None

    def __init__(self):
        """
        создает класс Connect и очередь сервера
        передает в класс Connect себя
        """
        Thread.__init__(self)

        self.connect_class = Conn(self)
        self.server_thread = svr_thread()

    def run(self):
        self.server_thread.start()
        self.connect_class.start()

        self.connect_class.join()

if __name__ == "__main__":
    App = Application()
    App.start()


