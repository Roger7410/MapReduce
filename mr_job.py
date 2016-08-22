import sys
import time
from datetime import datetime
import zerorpc
import gevent

if __name__ == "__main__":

    task_id = ""
    master_addr = sys.argv[1]
    test_name = sys.argv[2]
    f = open(test_name)
    test_code = f.read()
    file_split_size = sys.argv[3]
    reducer_count = sys.argv[4]
    input_file = sys.argv[5]
    output_file = sys.argv[6]

    c = zerorpc.Client()
    c.connect(master_addr)
    print "Strat %s" % task_id
    start_time = datetime.now()
    task_id = c.start_mr(test_name, test_code, file_split_size, reducer_count, input_file, output_file)
    
    job_finished = False

    while not job_finished:
        print "========== tracking %s ==========" % task_id
        mappers_done = c.job_tracking(task_id)['mappers_done']
        reducers_done = c.job_tracking(task_id)['reducers_done']
        total_mappers = c.job_tracking(task_id)['total_mappers']
        total_reducers = c.job_tracking(task_id)['total_reducers']

        print "Map finished %.2f " % float(float(mappers_done)/float(total_mappers)*100) + "%"
        print "Reduce finished %.2f " % float(float(reducers_done)/float(total_reducers)*100) + "%"

        if mappers_done == total_mappers and reducers_done == total_reducers:
            job_finished = True
            print "Job finished"
            end_time = datetime.now()
            print('Time spent: {}'.format(end_time - start_time))

        gevent.sleep(1)