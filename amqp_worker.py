import ConfigParser
import daemon
import errno
import json
import os
import pika
import socket
import sys
import time
import traceback
from argparse import ArgumentParser, FileType

__version__ = '0.0.3'


class AMQPWorker(object):
    MAX_SLEEP_TIME = 64

    def __init__(self, server, receive_queue, worker_func, complete_queue=None,
                 is_daemon=False, log_file=None, working_dir=None):
        self.server = server
        self.receive_queue = receive_queue
        self.worker_func = worker_func
        self.complete_queue = complete_queue
        self.is_daemon = is_daemon
        self.log_file = log_file
        self.working_dir = working_dir
        self.error_queue = '{0}_errors'.format(receive_queue)
        self.connection = self.channel = None

    def _start(self):
        sleep_time = 1
        iterations = 0
        running = True
        while running:
            try:
                if iterations > 0:
                    print('Retrying in {0} seconds'.format(sleep_time))
                    time.sleep(sleep_time)
                    sleep_time = min(self.MAX_SLEEP_TIME, sleep_time * 2)
                iterations += 1
                self.initialize_connection()
                sleep_time = 1
                self.receive_jobs()
            except socket.error as error:
                print('Error connecting to rabbitmq: {0!s}'.format(error))
            except pika.exceptions.AMQPConnectionError as error:
                print('Lost connection to rabbitmq')
            except Exception:
                traceback.print_exc()
            except KeyboardInterrupt:
                print('Goodbye!')
                running = False
            finally:
                # Force disconnect to release the jobs
                if self.connection:
                    if not self.connection.closed:
                        self.connection.close()
                    self.connection._adapter_disconnect()
                    self.connection = None

    def consume_callback(self, channel, method, _, message):
        return_message = None
        try:
            return_message = self.worker_func(**json.loads(message))
        except TypeError as exc:
            # Save the original in the error queue
            self.channel.basic_publish(
                exchange='', body=message, routing_key=self.error_queue,
                properties=pika.BasicProperties(delivery_mode=2))
            print('Message moved to error_queue: {0}'.format(exc))
        if return_message is not None:
            self.channel.basic_publish(
                exchange='', body=json.dumps(return_message),
                routing_key=self.complete_queue,
                properties=pika.BasicProperties(delivery_mode=2))
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def initialize_connection(self):
        print('Attempting connection')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.server))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.receive_queue, durable=True)
        self.channel.queue_declare(queue=self.error_queue, durable=True)
        if self.complete_queue:
            self.channel.queue_declare(queue=self.complete_queue, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        print('Connected')

    def receive_jobs(self):
        self.channel.basic_consume(self.consume_callback,
                                   queue=self.receive_queue)
        self.channel.start_consuming()

    def start(self):
        if self.working_dir:
            make_dirs(self.working_dir)
            os.chdir(self.working_dir)
        if self.is_daemon:
            if self.log_file:
                make_dirs(os.path.dirname(self.log_file))
            else:
                self.log_file = '/dev/null'
            log_fp = open(self.log_file, 'a')
            kwargs = {}
            if self.working_dir:
                kwargs['working_directory'] = self.working_dir
            with daemon.DaemonContext(stdout=log_fp, stderr=log_fp, **kwargs):
                # Line-buffer the output streams
                sys.stdout = os.fdopen(sys.stdout.fileno(), 'a', 1)
                sys.stderr = os.fdopen(sys.stderr.fileno(), 'a', 1)
                self._start()
        else:
            self._start()


def base_argument_parser(*args, **kwargs):
    parser = ArgumentParser(*args, **kwargs)
    parser.add_argument('-D', '--not-daemon', action='store_false',
                        dest='daemon')
    parser.add_argument('ini_file', type=FileType())
    return parser


def parse_base_args(parser, config_section='DEFAULT'):
    args = parser.parse_args()

    config = ConfigParser.ConfigParser()
    try:
        config.readfp(args.ini_file)
    except ConfigParser.Error as error:
        parser.error('Error with ini_file {0}: {1}'.format(args.ini_file.name,
                                                           error))
    settings = dict(config.items(config_section))
    return args, settings


def make_dirs(path):
    if not path:
        return
    try:
        os.makedirs(path)
    except OSError as error:
        if error.errno != errno.EEXIST:
            raise
