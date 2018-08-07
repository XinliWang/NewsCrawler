import pika
import json

class CloudAMQPClient:
    def __init__(self, cloudamqp_url, queue_name):
        self.cloudamqp_url = cloudamqp_url
        self.queue_name = queue_name
        # Parse cloudamqp_url
        self.params = pika.URLParameters(cloudamqp_url)
        self.params.socket_timeout = 3
        # Connect to CloudAMQP
        self.connection = pika.BlockingConnection(self.params)
        # Start a channel
        self.channel = self.connection.channel()
        # Declare a queue
        self.channel.queue_declare(queue=queue_name)

    def sendMessage(self, message):
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_name,
                                   body=json.dumps(message))
        print "[X] Sent message to %s : %s" %(self.queue_name, message)
        return

    def getMessage(self):
        method_frame, header_frame, body = self.channel.basic_get(self.queue_name)
        if method_frame:
            print "[O] Received message from  %s: %s" % (self.queue_name, body)
            # Acknowledge one or more messages
            self.channel.basic_ack(method_frame.delivery_tag)
            return json.dumps(body)
        else:
            print "No message returned"
            return None

    # sleep
    def sleep(self, seconds):
        self.connection.sleep(seconds)
