import cPickle as pickle
import string

unit = ' '
debug = True


class Map(object):
    def __init__(self, mapper, mapper_count, reducer_count, task_id, file_split_size):
        self.table = {}
        for i in range(reducer_count):
            self.table[i] = {}
        self.task_id = task_id
        self.mapper = mapper
        self.mapper_count = mapper_count
        self.reducer_count = reducer_count
        self.file_split_size = file_split_size

    def map(self, k, v):
        v = v.replace('\n', ' ').replace('\r', ' ')
        words = v.split()
        for w in words:
            word = w.strip(string.punctuation)
            if word != '':
                self.emit(word, 1)

    def emit(self, k, v):
        reducer_dict = self.table[self.partition(k)]
        if k in reducer_dict:
            reducer_dict[k].append(v)
        else:
            reducer_dict[k] = [v]

    def partition(self, key):
        return hash(key) % self.reducer_count

    def combine(self):
        for reducer in self.table:
            for word in self.table[reducer]:
                self.table[reducer][word] = sum(self.table[reducer][word])

    def write_to_file(self):
        for reducer in range(self.reducer_count):
            pickle.dump(self.table[reducer], open(self.task_id+'_m'+str(self.mapper)+"_r"+str(reducer), "wb"))


class Reduce(object):
    def __init__(self, reducer, output_file):
        self.table = {}
        self.reducer = reducer
        self.output_file = output_file

    def reduce(self, k, vdict):
        for mapper in vdict:
            for word in vdict[mapper]:
                if word in self.table:
                    self.emit(word, self.table[word] + vdict[mapper][word])
                else:
                    self.emit(word, vdict[mapper][word])

    def emit(self, k, v):
        self.table[k] = v

    def write_to_file(self):
        f = open(self.output_file+"_"+str(self.reducer), "wb")
        keys = self.table.keys()
        keys.sort()

        for word in keys:
            f.write(word + ": " + str(self.table[word]) + '\n')
        f.close()
