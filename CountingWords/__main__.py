import sys
from cos_backend import COSBackend
import json
import re
#import unidecode
import pika

def main(args):
    # initialize cos wrapper
    cb = COSBackend(args['cos']['service_endpoint'], args['cos']['secret_key'], args['cos']['access_key'])

    # fetch the assigned range of bytes
    target_segment = cb.get_object(args['target_bucket'], args['target_fname'], extra_get_args={"Range": args['Range']})
    target_segment = target_segment.decode('UTF-8', errors='ignore')

    words = re.findall(r'\w+', target_segment)  # parse chunk into words
    result = {}
    # count word repetition
    for word in words:
	    adapted_word = word.lower() #unidecode.unidecode(word).lower()
	    if adapted_word in result:
		    result[adapted_word] += 1
	    else:
		    result[adapted_word] = 1

    # commit result on the cloud
    result_tag = '{}/CW-result-{}'.format(args['target_fname'], args['index'])
    cb.put_object(args['target_bucket'], result_tag, json.dumps(result))

    # notify via queue, message = result file name on the cloud
    pika_params = pika.URLParameters(args['rabbitamqp_url'])
    connection = pika.BlockingConnection(pika_params)
    channel = connection.channel()
    channel.basic_publish(exchange='', routing_key=args['qid'], body=result_tag)
    connection.close()


if __name__ == "__main__":
    main(sys.argv[1])
