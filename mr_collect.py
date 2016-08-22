import sys

import zerorpc

class Collect(object):
    def __init__(self, filename, output_filename, master_addr):
        self.filename = filename
        self.output_filename = output_filename
        c = zerorpc.Client()
        c.connect(master_addr)
        self.master_info = (master_addr, c)

    def get_result(self):
        done, output = self.master_info[1].get_result(self.filename)
        if done:
            f = open(self.output_filename, 'wb')
            f.write(output)
            f.close()
            print "Collect job finished"
        else:
            print "Error"


if __name__ == "__main__":
    filename = sys.argv[1]
    output_filename = sys.argv[2]
    master_addr = sys.argv[3]
    collect = Collect(filename, output_filename, master_addr)
    collect.get_result()