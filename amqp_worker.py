import ConfigParser
import atexit
import daemon
import errno
import json
import logging
import os
import pika
import signal
import socket
import sys
import time
import traceback
from argparse import ArgumentParser, FileType

__version__ = '0.1rc1'


class AMQPWorker(object):
    PRIORITY_QUEUE_POLL_DELAY = 0.5  # In seconds
    MAX_SLEEP_TIME = 64

    def __init__(self, server, receive_queue, worker_func, error_queue=None,
                 complete_queue=None, working_dir=None, is_daemon=False,
                 log_file=None, pid_file=None):
        """Initialize an AMQPWorker.

        :param worker_func: is called with message from receive_queue where the
            dictionary keywords become the keyword arguments. If this function
            returns a dictionary, that dictionary is added as a message to the
            complete_queue. Alternatively an iterable of dictionaries can be
            returned, each of which will be added to the complete_queue.
        :param receive_queue: is the queue from which to receive jobs. If this
            argument is of a type list, then each item should be a queue, with
            the highest priority queues listed first.
        :param complete_queue: is the queue to put the job results. If this
            argument is of a type list, then each item should be a queue, with
            the highest priority queues listed first.

        """
        def fullpath(path):
            return os.path.abspath(os.path.expanduser(path)) if path else None

        self.server = server
        self.receive_queue = receive_queue
        self.worker_func = worker_func
        self.complete_queue = complete_queue
        self.is_daemon = is_daemon
        self.log_file = fullpath(log_file)
        self.pid_file = fullpath(pid_file)
        self.working_dir = fullpath(working_dir)
        self.error_queue = error_queue
        self.connection = self.channel = None
        self.use_priority = isinstance(receive_queue, list)
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
            kwargs = json.loads(message)
            if '_priority' in kwargs:
                try:
                    priority = int(kwargs['_priority'])
                except ValueError:
                    priority = None
                del kwargs['_priority']
            else:
                priority = None
            return_messages = self.worker_func(**kwargs)
        except TypeError as exc:
            if self.error_queue:
                # Save the original in the error queue
                self.channel.basic_publish(
                    exchange='', body=message, routing_key=self.error_queue,
                    properties=pika.BasicProperties(delivery_mode=2))
                print('Message moved to error_queue: {0}'.format(exc))
            else:
                print('Failed on {0}. Reason: {1}'.format(message, exc))
        if return_messages is not None:
            # Determine the return queue
            if isinstance(self.complete_queue, list):
                max_priority = len(self.complete_queue)
                if priority is not None:
                    priority = min(max(0, priority), max_priority - 1)
                    queue = self.complete_queue[priority]
                else:
                    queue = self.complete_queue[max_priority / 2]
            else:
                queue = self.complete_queue

            if isinstance(return_messages, dict):
                return_messages = [return_messages]
            for return_message in return_messages:
                self.channel.basic_publish(
                    exchange='', body=json.dumps(return_message),
                    routing_key=queue,
                    properties=pika.BasicProperties(delivery_mode=2))
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def handle_command(self, command):
        if command == 'start':
            self.start()
        elif command == 'stop':
            self.stop()
        elif command == 'restart':
            self.stop()
            time.sleep(1)  # Give the process time to terminate
            self.start()

    def initialize_connection(self):
        print('Attempting connection')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.server))
        self.channel = self.connection.channel()
        if self.use_priority:
            for receive_queue in self.receive_queue:
                self.channel.queue_declare(queue=receive_queue, durable=True)
        else:
            self.channel.queue_declare(queue=self.receive_queue, durable=True)
            self.channel.basic_qos(prefetch_count=1)
        if self.error_queue:
            self.channel.queue_declare(queue=self.error_queue, durable=True)
        if isinstance(self.complete_queue, list):
            for complete_queue in self.complete_queue:
                self.channel.queue_declare(queue=complete_queue, durable=True)
        elif self.complete_queue:
            self.channel.queue_declare(queue=self.complete_queue, durable=True)
        print('Connected')

    def receive_jobs(self):
        if self.use_priority:
            # Attempt to consume from highest priority queues first
            while True:
                for queue in self.receive_queue:
                    method, header, body = self.channel.basic_get(queue)
                    if method:
                        self.consume_callback(self.channel, method, header,
                                              body)
                        break
                else:
                    time.sleep(self.PRIORITY_QUEUE_POLL_DELAY)
        else:
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
                    if self.pid_file and os.path.isfile(self.pid_file):
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

    def stop(self):
        try:
            with open(self.pid_file) as fp:
                pid = int(fp.read())
            os.kill(pid, signal.SIGINT)
        except IOError as exc:
            if exc.errno == 2:  # File does not exist
                print('Nothing to stop: pid_file `{0}` does not exist'
                      .format(self.pid_file))
            else:
                print(exc)
        except OSError as exc:
            if exc.errno == 3:  # No such process
                print('Process not running. Removing pid_file.')
                os.unlink(self.pid_file)
            else:
                print(exc)
        except ValueError:
            print('Invalid pid_file. Removing.')
            os.unlink(self.pid_file)


def base_argument_parser(*args, **kwargs):
    parser = ArgumentParser(*args, **kwargs)
    parser.add_argument('-D', '--not-daemon', action='store_false',
                        dest='daemon', help='Do not run in the background.')
    parser.add_argument('-c', '--command', default='start')
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
    for key in settings:  # Make lists from items spanning multiple lines
        if '\n' in settings[key]:
            settings[key] = settings[key].split('\n')
    return args, settings


def make_dirs(path):
    if not path:
        return
    try:
        os.makedirs(path)
    except OSError as error:
        if error.errno != errno.EEXIST:
            raise
