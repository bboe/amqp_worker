import amqp_worker
import os
import sys


def do_work(some_id):
    with open('foo', 'a') as fp:
        # The following raises an exception if some_id is not int
        fp.write(int(some_id) + '\n')
    print(some_id)
    return [{'some_id': some_id}, {'some_id': 1024}]


def main():
    parser = amqp_worker.base_argument_parser()
    args, settings = amqp_worker.parse_base_args(parser, 'main')

    worker = amqp_worker.AMQPWorker(
        settings['server'], settings['receive_queue'],
        do_work, is_daemon=args.daemon,
        error_queue=settings.get('error_queue'),
        complete_queue=settings['complete_queue'],
        working_dir=settings['working_dir'],
        log_file=settings['log_file'], pid_file=settings['pid_file'])

    worker.handle_command(args.command)


if __name__ == '__main__':
    sys.exit(main())
