import amqp_worker
import sys


def do_work(some_id):
    with open('foo', 'a') as fp:
        # The following raises an exception if some_id is not int
        fp.write('{}\n'.format(int(some_id)))
    print(some_id)
    return [{'some_id': some_id}, {'some_id': 1024}]


def main():
    parser = amqp_worker.base_argument_parser()
    args, settings = amqp_worker.parse_base_args(parser, 'main')
    # All of the settings in the ini file correspond to a parameter in
    # the constructor.
    worker = amqp_worker.AMQPWorker(worker_func=do_work, is_daemon=args.daemon,
                                    **settings)
    worker.handle_command(args.command)


if __name__ == '__main__':
    sys.exit(main())
