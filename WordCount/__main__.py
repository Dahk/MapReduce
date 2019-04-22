import sys
from cos_backend import COSBackend
import json
import re
import pika

def main(args):
    # initialize cos wrapper
    cb = COSBackend(args['cos']['service_endpoint'], args['cos']['secret_key'], args['cos']['access_key'])

    # fetch the assigned range of bytes and parse that chunk into words to then get the total number of words
    # ( by the way, this must be done in one line (as a r-value) so that the object returned by the cb.get_object method gets
    # free'd by the garbage collector ASAP, therefore reserved memory doesn't stack up too much )
    words = re.findall(r'\w+', cb.get_object(args['target_bucket'], args['target_fname'], extra_get_args={'Range': args['Range']}).decode('UTF-8', errors='ignore'))
    counter = len(words)
    result = {'word_count': counter}

    # commit result on the cloud
    result_tag = '{}/WC-result-{}'.format(args['target_fname'], args['index'])
    cb.put_object(args['target_bucket'], result_tag, json.dumps(result))

    # notify via queue, message = result filename on the cloud
    pika_params = pika.URLParameters(args['rabbitamqp_url'])
    connection = pika.BlockingConnection(pika_params)
    channel = connection.channel()
    channel.basic_publish(exchange='', routing_key=args['qid'], body=result_tag)
    connection.close()


if __name__ == "__main__":
    main(sys.argv[1])