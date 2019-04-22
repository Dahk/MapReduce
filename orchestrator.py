from ibm_cf_connector import CloudFunctions
from cos_backend import COSBackend
import sys
import json
import pika
import time
import yaml

# not used, included for later uses
class CounterCallback(object):
    def __init__ (self, count):
        self.count = count

    def __call__ (self, ch, method, properties, body):
        self.count -= 1
        print(self.count)
        if not self.count:
            ch.stop_consuming()

# callable class for synchronous waits
class SingleCallback(object):
    def __call__ (self, ch, method, properties, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

class Orchestrator:
    def __init__ (self, target_bucket, target_fname, upload=True):
        self.target_fname = target_fname
        self.target_bucket = target_bucket
        self.ini_error = False
        format_str = "cloudfunctions:\n  'endpoint': ''\n  'namespace': ''\n  'api_key': ''\nrabbitamqp:\n  'url': ''\ncos:\n  service_endpoint: ''\n  secret_key: ''\n  access_key: ''"
        
        try:            
            # load keys securely
            with open('secret.yaml', 'r') as f:
                secret = yaml.safe_load(f)

            # initialitze the remote storage wrapper, and upload the target file
            self.cb = COSBackend(secret['cos']['service_endpoint'], secret['cos']['secret_key'], secret['cos']['access_key'])
            if upload:
                target_file = open(self.target_fname, "rb")
                self.cb.put_object(target_bucket, target_fname, target_file.read())
                target_file.close()

            # retrieve file length, ensure file has been uploaded
            try:
                self.fsize = int(self.cb.head_object(self.target_bucket, self.target_fname)['content-length'])
            except:
                print('File \'{}\' was not found in this bucket \'{}\'. Upload it and retry.'.format(self.target_fname, self.target_bucket))
                self.ini_error = True
                return None
        
            # initialize the function wrapper
            config = {}
            config['endpoint'] = secret['cloudfunctions']['endpoint']
            config['namespace'] = secret['cloudfunctions']['namespace']
            config['api_key'] = secret['cloudfunctions']['api_key']
            self.cf = CloudFunctions(config)

            # initialize the queue system
            self.pika_params = pika.URLParameters(secret['rabbitamqp']['url'])

        except KeyError:
            print('Wrong yaml document format. Please use the following one:')
            print(format_str)
            self.ini_error = True
        except FileNotFoundError as e:
            print('File \'{}\' not found.'.format(e.filename))
            self.ini_error = True

        # set the common args stub
        self.comargs = {}
        self.comargs['cos'] = secret['cos']
        self.comargs['rabbitamqp_url'] = secret['rabbitamqp']['url']
        self.comargs['target_bucket'] = self.target_bucket
        self.comargs['target_fname'] = self.target_fname

        # two separate queues, the reducer waits for the mappers and the orchestrator waits for the reducer
        self.mapper_qid = 'mapperQueue'
        self.reducer_qid = 'reducerQueue'


    def run (self, mapper, nthreads):
        # check if initialization was good
        if self.ini_error:
            return -4
        # validation of parameters
        if nthreads < 1:
            print('Minimum number of partitions or threads must be 1. \nExiting...')
            return -1
        if mapper != 'CountingWords' and mapper != 'WordCount':
            print('{} is not supported as a mapper yet. Supported mappers: CountingWords, WordCount. \nExiting...'.format(mapper))
            return -2

        # prepare arguments for the mapper (mapper args)
        chunk_size = int(self.fsize / nthreads)
        mapargs = self.comargs.copy()
        mapargs['qid'] = self.mapper_qid

        # stat connection with the queue system
        connection = pika.BlockingConnection(self.pika_params)
        channel = connection.channel()
        channel.queue_declare(queue=self.mapper_qid)
        channel.queue_purge(queue=self.mapper_qid)  # ensure no message was left

        # measure time
        start_t = time.time()

        # dispatch mappers except the last one
        for i in range(0, nthreads-1):
            mapargs['index'] = str(i)
            mapargs['Range'] = 'bytes={}-{}'.format(chunk_size*i, chunk_size*(i+1))
            self.cf.invoke(mapper, mapargs)
            print('[{}]'.format(mapargs['index']), chunk_size*i, 'to', chunk_size*(i+1))
        
        # dispatch the last mapper, so that it takes the rest of the file
        mapargs['index'] = nthreads-1
        mapargs['Range'] = 'bytes={}-{}'.format(chunk_size*(nthreads-1), self.fsize)
        self.cf.invoke(mapper, mapargs)
        print('[{}]'.format(mapargs['index']), chunk_size*(nthreads-1), 'to', self.fsize)

        # prepare arguments for the reducer (reducer args)
        redargs = self.comargs.copy()
        redargs['reduce_{}'.format(mapper)] = 'yes'
        redargs['nthreads'] = nthreads
        redargs['mapper_qid'] = self.mapper_qid
        redargs['reducer_qid'] = self.reducer_qid

        channel.queue_declare(queue=self.reducer_qid)
        channel.queue_purge(queue=self.reducer_qid)  # ensure no message was left

        self.cf.invoke('Reducer', redargs)

        # wait for the reducer to finish        
        channel.basic_consume(queue=self.reducer_qid, on_message_callback=SingleCallback())
        channel.start_consuming()

        # measure time
        end_t = time.time()

        connection.close()

        print('Execution time: {0:.5g}s'.format(end_t-start_t))


    def claimFile (self, result_type, result_fname):
        # check if initialization was good
        if self.ini_error:
            return -4

        try:
            result_file = open(result_fname, "w")
            cos_result = self.cb.get_object(self.target_bucket, '{}/{}-result'.format(self.target_fname, result_type))
            result_file.write(cos_result.decode('utf-8')) 
            result_file.close()
        except:
            print('Something went wrong, could not download result file for: {}, action: {}'.format(self.target_fname, result_type))

        
# command line main
orc = Orchestrator(sys.argv[2], sys.argv[3])
if sys.argv[1] == 'WC':
    orc.run('WordCount', int(sys.argv[4]))
    orc.claimFile('WC', 'WordCount-{}'.format(sys.argv[3]))
elif sys.argv[1] == 'CW':
    orc.run('CountingWords', int(sys.argv[4]))
    orc.claimFile('CW', 'CountingWords-{}'.format(sys.argv[3]))



