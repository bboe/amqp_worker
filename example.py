import amqp_worker
import os
import sys


def do_work(some_id):
    with open('foo', 'w') as fp:
        fp.write(some_id + '\n')
    print(some_id)
    return {'some_id': some_id}


def main():
    parser = amqp_worker.base_argument_parser()
    args, settings = amqp_worker.parse_base_args(parser, 'main')

    log_file = os.path.expanduser(settings['log_file'])
    working_dir = os.path.expanduser(settings['working_dir'])

    worker = amqp_worker.AMQPWorker(settings['server'],
                                    settings['receive_queue'],
                                    do_work, is_daemon=args.daemon,
                                    complete_queue=settings['complete_queue'],
                                    working_dir=working_dir,
                                    log_file=log_file)
    worker.start()


if __name__ == '__main__':
    sys.exit(main())
