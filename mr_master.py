# python2.7 mr_master.py 0.0.0.0:10000 .
import fnmatch
import os
import sys
import math
import time
import zerorpc
import gevent
from gevent.queue import Queue

class Master(object):

    def __init__(self, data_dir):
        gevent.spawn(self.controller)
        self.state = 'ready'
        self.workers = {}
        self.data_dir = data_dir
        self.task_info = {}      
        self.mapper_queue = Queue()
        self.reducer_queue = Queue()
        self.mapper_list = []
        self.reducer_list = []

    def controller(self):
        while True:
            print 'Master %s ' % self.state
            for w in self.workers:
                print '(%s, %s)' % (w, self.workers[w][0])            
            gevent.sleep(1)

    def register(self, ip_port):
        gevent.spawn(self.register_async, ip_port)
        return self.data_dir

    def register_async(self, ip_port):
        print 'Master:%s ' % self.state,
        print 'Registered worker (%s)' % ip_port
        c = zerorpc.Client()
        c.connect("tcp://" + ip_port)
        self.workers[ip_port] = ('ready', c)
        self.mapper_list.append((ip_port, c))
        self.reducer_list.append((ip_port, c))
        self.mapper_queue.put((ip_port, c))
        self.reducer_queue.put((ip_port, c))
        c.ping()

    def split_file(self, file_split_size, input_file):
        path = self.data_dir + '/' + input_file
        length = os.path.getsize(path)
        info = []
        info = [(path, length)]
        split = {}    
        for i in range(int(math.floor(float(length) / file_split_size))+1):
            begin = i * file_split_size
            if ((i+1) * file_split_size) < length:
                end = (i+1) * file_split_size
            else:
                end = length
            split[i] = []
            split[i].append((path, begin, end))
        return split, info

    def start_mr(self, test_name, test_code, file_split_size, reducer_count, input_file, output_file):
        task_id = "task_"+time.strftime("%d%H%M%S", time.localtime())
        file_split_size = int(file_split_size)
        split, info = self.split_file(file_split_size, input_file)
        mapper_count = len(split)
        reducer_count = int(reducer_count)

        self.task_info[task_id] = {"mappers": {}, "reducers": {}, 
                                    "test_name": test_name, "test_code": test_code,
                                    "file_split_size": file_split_size,
                                    "split": split, "info": info,
                                    "mapper_count": mapper_count, "reducer_count": reducer_count,                               
                                    "output_file": output_file,
                                    "done": False}
        # Map Reduce
        gevent.spawn(self.map, task_id)
        gevent.spawn(self.reduce, task_id)
        return task_id

    def job_tracking(self, task_id):
        mappers = self.task_info[task_id]["mappers"]
        reducers = self.task_info[task_id]["reducers"]
        total_mappers = self.task_info[task_id]["mapper_count"]
        total_reducers = self.task_info[task_id]["reducer_count"]

        mappers_done = 0
        reducers_done = 0
        #count done mappers and reducers
        for mapper in mappers:
            if mappers[mapper][3]:
                mappers_done += 1

        for reducer in reducers:
            if reducers[reducer][2]:
                reducers_done += 1
        tracking = {"mappers_done": mappers_done,
                       "total_mappers": total_mappers, 
                       "reducers_done": reducers_done,
                       "total_reducers": total_reducers}
        if reducers_done == total_reducers:
            self.task_info[task_id]["done"] = True
        return tracking

    # Assign map jobs to free mappers
    def map(self, task_id):
        print '----------------map---------------------'
        mapper_count = self.task_info[task_id]["mapper_count"]
        for mapper in range(mapper_count):
            ip_port, c = self.mapper_queue.get()
            #ip_port, c = self.mapper_list.pop()
            file_split_size = self.task_info[task_id]["file_split_size"]
            test_name = self.task_info[task_id]["test_name"]
            test_code = self.task_info[task_id]["test_code"]
            mapper_count = self.task_info[task_id]["mapper_count"]
            reducer_count = self.task_info[task_id]["reducer_count"]
            info = self.task_info[task_id]["info"]
            split = self.task_info[task_id]["split"]           

            self.task_info[task_id]["mappers"][mapper] = [c, ip_port, split, False]
            c.do_map(split, test_name, test_code,
                                mapper, mapper_count, reducer_count, 
                                task_id, info, file_split_size)

    def mapper_finish(self, success, task_id, mapper, ip_port):
        self.task_info[task_id]["mappers"][mapper][3] = True

        reducers = self.task_info[task_id]["reducers"]
        for reducer in reducers:
            reducer_tmp = reducers[reducer][0]
            reducer_tmp.notify_mapper_finish(mapper, ip_port)
        if ip_port in self.workers:
            self.mapper_queue.put_nowait((ip_port, self.workers[ip_port][1]))
            #self.mapper_list.append((ip_port, self.workers[ip_port][1]))
        

    # Assign reduce jobs to free reducers
    def reduce(self, task_id):
        reducer_count = self.task_info[task_id]["reducer_count"]
        for reducer in range(reducer_count):
            ip_port, c = self.reducer_queue.get()
            while ip_port not in self.workers:
                ip_port, c = self.reducer_queue.get()
            test_name = self.task_info[task_id]["test_name"]
            test_code = self.task_info[task_id]["test_code"]
            mapper_count = self.task_info[task_id]["mapper_count"]
            output_file = self.task_info[task_id]["output_file"]
            self.task_info[task_id]["reducers"][reducer] = [c, ip_port, False]
            for mapper in self.task_info[task_id]["mappers"]:
                if self.task_info[task_id]["mappers"][mapper][3]:
                    c.notify_mapper_finish(mapper, self.task_info[task_id]["mappers"][mapper][1])

            c.do_reduce(test_name, test_code, mapper_count, reducer, output_file, task_id)

    def reducer_finish(self, success, task_id, reducer, ip_port):
        self.task_info[task_id]["reducers"][reducer][2] = True
        if ip_port in self.workers:
            self.reducer_queue.put_nowait((ip_port, self.workers[ip_port][1]))

    # Collector get result from master
    def get_result(self, filename_base):
        print "Start Collecting"
        keys = self.task_info.keys()
        keys.sort(reverse=True)
        for task_id in keys:
            if self.task_info[task_id]["output_file"] == filename_base:

                for mapper in self.task_info[task_id]["mappers"]:
                    self.task_info[task_id]["mappers"][mapper][0]\
                        .delete_task_file(task_id, mapper, self.task_info[task_id]["reducer_count"])

                result = ""
                for reducer in self.task_info[task_id]["reducers"]:
                    result += self.task_info[task_id]["reducers"][reducer][0]\
                        .fetch_result_file(filename_base, reducer)
                self.task_info.pop(task_id, None)
                return True, result
        return False, ''


if __name__ == "__main__":
    master_addr = sys.argv[1]
    data_dir = sys.argv[2]
    server = zerorpc.Server(Master(data_dir))
    server.bind('tcp://' + master_addr)
    server.run()
