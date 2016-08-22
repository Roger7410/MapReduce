# python2.7 mr_seq.py word_count.py 100000 3 file_sim.txt seq_count


import fnmatch
import os
import sys
import math
import time
import logging
from datetime import datetime
import importlib
import cPickle as pickle
import gevent


class Sequential(object):
    def __init__(self):
        self.task_info = {}
        self.task = ''
        self.data_dir = os.getcwd()

    def start_mr(self, test_name, test_code, file_split_size, reducer_count, input_file, output_file):
        task_id = "task_"+time.strftime("%d%H%M%S", time.localtime())
        self.task_id = task_id
        self.output_file = output_file

        start_time = datetime.now()
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

        mapper_procs = gevent.spawn(self.map, task_id)
        mapper_procs.join()
        reducer_procs = gevent.spawn(self.reduce, task_id)
        reducer_procs.join()
        print "Finished"
        end_time = datetime.now()
        print('Time spent: {}'.format(end_time - start_time))
        return task_id

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

    def map(self, task_id):
        print "Staring Map"
        mapper_count = self.task_info[task_id]["mapper_count"]
        procs = []
        for mapper in range(mapper_count):
            proc = gevent.spawn(self.run_mapper, mapper, task_id)
            procs.append(proc)
        gevent.joinall(procs)
        print "Map Done"

    def run_mapper(self, mapper, task_id):
        split = self.task_info[task_id]["split"]
        test_name = self.task_info[task_id]["test_name"]
        test_code = self.task_info[task_id]["test_code"]
        mapper_count = self.task_info[task_id]["mapper_count"]
        reducer_count = self.task_info[task_id]["reducer_count"]
        info = self.task_info[task_id]["info"]
        file_split_size = self.task_info[task_id]["file_split_size"]
        module = importlib.import_module(test_name[:-3])
        self.unit = module.unit
        self.map_object = module.Map(mapper, mapper_count, reducer_count, task_id, file_split_size)
        #data = self.read_input(split[mapper], mapper, mapper_count, info, file_split_size)
        data = self.read_input(split, mapper, mapper_count, info, file_split_size)
        self.map_object.map(test_name, data)
        self.map_object.combine()
        self.map_object.write_to_file()
        success = True
        return success

    def reduce(self, task_id):
        print "Staring Reduce"
        reducer_count = self.task_info[task_id]["reducer_count"]
        procs = []
        for reducer in range(reducer_count):
            proc = gevent.spawn(self.run_reducer, reducer, task_id)
            procs.append(proc)
        gevent.joinall(procs)
        print "Reduce Done"

    def run_reducer(self, reducer, task_id):
        test_name = self.task_info[task_id]["test_name"]
        test_code = self.task_info[task_id]["test_code"]
        mapper_count = self.task_info[task_id]["mapper_count"]
        output_file = self.task_info[task_id]["output_file"]
        module = importlib.import_module(test_name[:-3])
        self.reduce_object = module.Reduce(reducer, output_file)
        finished_data = {}
        for mapper in range(mapper_count):
            data = pickle.load(open(task_id + "_m" + str(mapper) +
                                        '_r' + str(reducer), 'rb'))
            finished_data[mapper] = data
        self.reduce_object.reduce(task_id, finished_data)
        self.reduce_object.write_to_file()

    def read_input(self, split_mapper, mapper, mapper_count, info, file_split_size):
        data = ""
        filename = ""
        start = 0
        read_size = 0

        filename = split_mapper[mapper][0][0]
        start = split_mapper[mapper][0][1]
        read_size = split_mapper[mapper][0][2] - split_mapper[mapper][0][1]
        #data = self.read_length_from_file(filename, start, read_size)

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

    def read_length_from_file(self, filename, start, size):
        f = open(filename)
        f.seek(start)
        data = f.read(size)
        f.close()
        return data



    def remove_tmp_file(self, task_id, mapper, reducer_count):
        for reducer in range(reducer_count):
            tmp_filename = task_id + '_m' + str(mapper) + "_r" + str(reducer)
            os.remove(tmp_filename)

    def fetch_result_file(self, output_file, reducer):
        output_filename = output_file + '_' + str(reducer)
        f = open(output_filename, 'rb')
        result = f.read()
        f.close()
        os.remove(output_filename)
        return result

    def get_result(self):
        print "Starting Collect"
        task_id = self.task_id
        for mapper in range(self.task_info[task_id]["mapper_count"]):
            self.remove_tmp_file(task_id, mapper, self.task_info[task_id]["reducer_count"])
        result = ""
        for reducer in range(self.task_info[task_id]["reducer_count"]):
            result += self.fetch_result_file(output_file, reducer)
        f = open(self.output_file, 'wb')
        f.write(result)
        f.close()
        print "Collect Done"


if __name__ == "__main__":
    seq = Sequential()
    test_name = sys.argv[1]
    file_split_size = sys.argv[2]
    reducer_count = sys.argv[3]
    input_file = sys.argv[4]
    output_file = sys.argv[5]

    f = open(test_name)
    test_code = f.read()
    seq.start_mr(test_name, test_code, file_split_size, reducer_count, input_file, output_file)
    seq.get_result()







