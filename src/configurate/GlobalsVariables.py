# coding=utf-8
class Global_variables:

    def __init__(self):
        self.__PORT__ = 27000
        self.__server_ip__ = 'localhost'
        self.__MAX_CONNECT__ = 10
        self.__BUFFER_SIZE__ = 30
        self.localhost = 'localhost'
        self.address_rbbmq = 'localhost'
        self.glob_queue_name = "Global_Queue"
        self.queue_addr = 'localhost'

    def set_server_ip(self, val):
        self.__server_ip__ = val

    def set_port(self, val):
        self.__PORT__ = val

    def get_server_ip(self):
        return self.__server_ip__

    def get_port(self):
        return self.__PORT__

global_data = Global_variables()

