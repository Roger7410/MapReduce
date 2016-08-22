echo "Start job word_count"
python2.7 mr_job.py tcp://0.0.0.0:10000 word_count.py 100000 3 file_sim.txt mr_count
echo "Start collect"
python2.7 mr_collect.py mr_count mr_result_count tcp://0.0.0.0:10000


            
        

python2.7 mr_job.py tcp://10.0.0.219:22220 word_count.py 100000 4 file_sim.txt mr_count
