seq
# python2.7 mr_seq.py word_count.py 100000 3 file_sim.txt seq_count

mapreduce
master
# python2.7 mr_master.py 0.0.0.0:10000 .
worker
# python2.7 mr_worker.py 0.0.0.0:10000 0.0.0.0:10001
job
python2.7 mr_job.py tcp://0.0.0.0:10000 word_count.py 100000 3 file_sim.txt mr_count
collect
python2.7 mr_collect.py mr_count mr_result_count tcp://0.0.0.0:10000