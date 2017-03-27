# coding=utf-8
from threading import Thread

from Connection import Connect
from GlobalQueue import Server_Thread

class Application(Thread):
    connect_class = None
    server_thread = None

    def __init__(self):
        """
        создает класс Connect и очередь сервера
        передает в класс Connect себя
        """
        Thread.__init__(self)

        self.connect_class = Connect(self)
        self.server_thread = Server_Thread()

    def run(self):
        self.server_thread.start()
        self.connect_class.start()

        self.connect_class.join()
        return

if __name__ == "__main__":
    App = Application()
    App.start()


