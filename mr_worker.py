# python2.7 mr_worker.py 0.0.0.0:10000 0.0.0.0:10001
import sys
import importlib
import os
import cPickle as pickle

import zerorpc
import gevent
from gevent.queue import Queue

class Worker(object):
    def __init__(self, master_addr, worker_addr):
        self.state = 'ready'
        self.master_addr = master_addr
        self.worker_addr = worker_addr
        self.map_object = None
        self.reduce_object = None
        self.unit = None
        self.data_dir = None
        self.reducer_map_queue = Queue()
        self.master = None
        gevent.spawn(self.controller)

    def controller(self):
        while True:
            print('Worker: %s: %s' % (self.worker_addr, self.state))
            gevent.sleep(1)

    def ping(self):
        print('Worker: ' + self.worker_addr +' pinging Master')
        return True

    def do_map(self, split_mapper, test_name, test_code, mapper, mapper_count, reducer_count, task_name, info, file_split_size):
        gevent.spawn(self.do_map_async, split_mapper,
                     test_name, test_code, mapper, mapper_count, reducer_count, task_name, info, file_split_size)

    def do_map_async(self, split_mapper,
                     test_name, test_code, mapper, mapper_count, reducer_count, task_name, info, file_split_size):
        print "Start Mapping" 
        module = importlib.import_module(test_name[:-3])
        self.unit = module.unit
        self.map_object = module.Map(mapper, mapper_count, reducer_count, task_name, file_split_size)
        data = self.read_input(split_mapper, mapper, mapper_count, info, file_split_size)
        gevent.sleep(0)

        self.map_object.map(test_name, data)
        self.map_object.combine()
        gevent.sleep(0)

        self.map_object.write_to_file()
        self.master.mapper_finish(True, task_name, mapper, self.worker_addr)

    def do_reduce(self, test_name, test_code, mapper_count, reducer, output_file, task_name):
        gevent.spawn(self.do_reduce_async, test_name, test_code, mapper_count, reducer, output_file, task_name)

    def do_reduce_async(self,test_name, test_code, mapper_count, reducer, output_file, task_name):
        module = importlib.import_module(test_name[:-3])
        self.reduce_object = module.Reduce(reducer, output_file)

        data_finished = {}
        while len(data_finished) != mapper_count:
            gevent.sleep(0)
            mapper, mapper_ip_port = self.reducer_map_queue.get()
            if mapper_ip_port == self.worker_addr:
                data = pickle.load(open(task_name + "_m" + str(mapper) +
                                            '_r' + str(reducer), 'rb'))
            else:
                c = zerorpc.Client()
                c.connect("tcp://" + mapper_ip_port)
                data = pickle.load(open(task_name + "_m" + str(mapper) + "_r" + str(reducer), 'rb'))
                c.close()
            data_finished[mapper] = data
        self.reduce_object.reduce(task_name, data_finished)
        self.reduce_object.write_to_file()
        self.master.reducer_finish(True, task_name, reducer, self.worker_addr)

    def read_input(self, split_mapper, mapper, mapper_count, info, file_split_size):
        data = ""
        filename = ""
        start = 0
        read_size = 0
        count =0 
        filename = split_mapper[mapper][0][0]
        start = split_mapper[mapper][0][1]
        read_size = split_mapper[mapper][0][2] - split_mapper[mapper][0][1]
        #data = self.read_length_from_file(filename, start, read_size)
        # read data from the split for this mapper
        # filename = split[0][0]
        # start = split[0][1]
        # read_size = split[0][2] - split[0][1]
        # data = self.read_length_from_file(filename, start, read_size)
        #start = start + read_size
        # # If unit is str, then get more data until we see the unit
        # if type(self.unit) == str:
        #     # Remove the first split if mapper is not 0
        #     if mapper != 0:
        #         if len(data.split(self.unit)) > 1:
        #             data = data.split(self.unit, 1)[1]
        #         else:
        #             data = ""
        #     # Get more split if the mapper is not the last mapper
        #     if mapper != mapper_count - 1:
        #         data += self.read_length_from_file(info[0][0], start, info[0][1] - start).split(self.unit, 1)[0]

        if mapper == 0:
            f = open(filename)
            f.seek(start)
            data = f.read(read_size)
            if read_size == file_split_size :
                f.seek(start + read_size)
                data += f.readline()

        if mapper != 0 and mapper != mapper_count-1:
            f = open(filename)
            f.seek(start)
            line = len(f.readline())
            data = f.read(read_size - line)
            f.seek(start + read_size)
            data += f.readline()

        if mapper == mapper_count-1 and mapper != 0:
            f = open(filename)
            f.seek(start)
            line = len(f.readline())
            data = f.read(read_size - line)

        return data

    def read_length_from_file(self, filename, begin, length):
        f = open(filename, 'rb')
        f.seek(begin)
        data = f.read(length)
        f.close()
        return data

    def delete_task_file(self, task_name, mapper, reducer_count):
        for reducer in range(reducer_count):
            tmp_file = task_name + '_m' + str(mapper) + "_r" + str(reducer)
            os.remove(tmp_file)

    def fetch_result_file(self, output_file, reducer):
        output_filename = output_file + '_' + str(reducer)
        f = open(output_filename, 'rb')
        result = f.read()
        f.close()
        os.remove(output_filename)
        return result

    def notify_mapper_finish(self, mapper, mapper_ip_port):
        self.reducer_map_queue.put_nowait((mapper, mapper_ip_port))


if __name__ == "__main__":
    master_addr = sys.argv[1]
    worker_addr = sys.argv[2]
    worker = Worker(master_addr, worker_addr)
    server = zerorpc.Server(worker)
    server.bind('tcp://' + worker_addr)
    client = zerorpc.Client()
    client.connect('tcp://' + master_addr)
    worker.master = client
    worker.data_dir = client.register(worker_addr)
    server.run()






