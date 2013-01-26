import ConfigParser
import atexit
import daemon
import errno
import json
import logging
import os
import pika
import socket
import sys
import time
import traceback
from argparse import ArgumentParser, FileType

__version__ = '0.0.5'


class AMQPWorker(object):
    MAX_SLEEP_TIME = 64

    def __init__(self, server, receive_queue, worker_func, complete_queue=None,
                 working_dir=None, is_daemon=False, log_file=None,
                 pid_file=None):
        """Initialize an AMQPWorker.

        :param worker_func: is called with message from receive_queue where the
            dictionary keywords become the keyword arguments. If this function
            returns a dictionary, that dictionary is added as a message to the
            complete_queue. Alternatively an iterable of dictionaries can be
            returned, each of which will be added to the complete_queue.

        """
        self.server = server
        self.receive_queue = receive_queue
        self.worker_func = worker_func
        self.complete_queue = complete_queue
        self.is_daemon = is_daemon
        self.log_file = os.path.abspath(log_file)
        self.pid_file = os.path.abspath(pid_file)
        self.working_dir = working_dir
        self.error_queue = '{0}_errors'.format(receive_queue)
        self.connection = self.channel = None
        # Don't log pika messages to stdout
        logging.getLogger('pika').addHandler(logging.NullHandler())

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
                if self.connection and not self.connection.is_closed:
                    # This call complains when daemon mode dies, oh well
                    self.connection.close()
                self.connection = None

    def consume_callback(self, channel, method, _, message):
        return_messages = None
        try:
            return_messages = self.worker_func(**json.loads(message))
        except TypeError as exc:
            # Save the original in the error queue
            self.channel.basic_publish(
                exchange='', body=message, routing_key=self.error_queue,
                properties=pika.BasicProperties(delivery_mode=2))
            print('Message moved to error_queue: {0}'.format(exc))
        if return_messages is not None:
            if isinstance(return_messages, dict):
                return_messages = [return_messages]
            for return_message in return_messages:
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
            if self.pid_file:
                # Exit on existing pidfile
                if os.path.isfile(self.pid_file):
                    print('pidfile `{0}` already exists'.format(self.pid_file))
                    sys.exit(1)
                pid_file = open(self.pid_file, 'w')
                files_preserve = [pid_file]
            else:
                pid_file = None
                files_preserve = None
            log_fp = open(self.log_file, 'a')
            kwargs = {}
            if self.working_dir:
                kwargs['working_directory'] = self.working_dir
            with daemon.DaemonContext(files_preserve=files_preserve,
                                      stdout=log_fp, stderr=log_fp, **kwargs):
                def delete_pid_file():
                    if self.pid_file:
                        os.remove(self.pid_file)

                # Configure pid file state and cleanup
                if self.pid_file:
                    atexit.register(delete_pid_file)
                    pid_file.write(str(os.getpid()))
                    pid_file.close()

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
