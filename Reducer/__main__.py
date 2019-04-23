import sys
from cos_backend import COSBackend
import json
import re
import pika

class ReduceCallback(object):
        def __init__ (self, cb, target_bucket, nthreads):
            self.cb = cb
            self.target_bucket = target_bucket
            self.nthreads = nthreads
            self.result = {}    # where we will be accumulating results

        def __call__ (self, ch, method, properties, body):

            file_tag = body.decode('utf-8') # decode file from queue message            
            target_segment = self.cb.get_object(self.target_bucket, file_tag)   # fetch results chunk            
            chunk = json.loads(target_segment.decode('utf-8'))  # parse results chunk

            # merge dictionary with the whole result dictionary
            for word in chunk:
                if word in self.result:
                    self.result[word] += chunk[word]
                else:
                    self.result[word] = chunk[word]

            ch.basic_ack(delivery_tag=method.delivery_tag)  # delete message from the queue
            self.cb.delete_object(self.target_bucket, file_tag)    # delete mapper result after merging
            
            # decrease and check counter
            self.nthreads -= 1
            if not self.nthreads:
                ch.stop_consuming()

def main(args):
    # initialize cos wrapper
    cb = COSBackend(args['cos']['service_endpoint'], args['cos']['secret_key'], args['cos']['access_key'])

    # initialize queue system for the mappers' queue
    pika_params = pika.URLParameters(args['rabbitamqp_url'])
    connection = pika.BlockingConnection(pika_params)
    channel = connection.channel()
    channel.queue_declare(queue=args['mapper_qid'])

    # check what we are reducing
    if 'reduce_WordCount' in args and args['reduce_WordCount'] == 'yes':
        callback = ReduceCallback(cb, args['target_bucket'], args['nthreads'])  # create a callback
        channel.basic_consume(callback, queue=args['mapper_qid'])      # set a callback
        channel.start_consuming()
        cb.put_object(args['target_bucket'], '{}/WC-result'.format(args['target_fname']), json.dumps(callback.result))  # commit result

    if 'reduce_CountingWords' in args and args['reduce_CountingWords'] == 'yes':
        callback = ReduceCallback(cb, args['target_bucket'], args['nthreads'])
        channel.basic_consume(callback, queue=args['mapper_qid'])
        channel.start_consuming()
        cb.put_object(args['target_bucket'], '{}/CW-result'.format(args['target_fname']), json.dumps(callback.result))

    # tell the orchestrator job is done
    channel.basic_publish(exchange='', routing_key=args['reducer_qid'], body='OK')
    connection.close()


if __name__ == "__main__":
    main(sys.argv[1])