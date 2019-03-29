#!/usr/bin/env python
import argparse
from typing import Type, TYPE_CHECKING

from golem.config.environments import set_environment

from scripts.node_integration_tests.playbooks import run_playbook
from scripts.node_integration_tests.playbooks.test_base import NodeId

if TYPE_CHECKING:
    from scripts.node_integration_tests.playbooks.test_base import Config, Test


class DictAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None) -> None:
        assert(self.nargs == 2)
        dest = getattr(namespace, self.dest)
        if dest is None:
            setattr(namespace, self.dest, {values[0]: values[1]})
        else:
            dest[values[0]] = values[1]


def parse_args():
    parser = argparse.ArgumentParser(description="Runs a single test.")
    parser.add_argument(
        'test_class',
        help="a dot-separated path to the test class within `playbooks`,"
             " e.g. golem.regular_run.RegularRun",
    )
    parser.add_argument(
        '--task-package',
        help='a directory within `tasks` containing the task package'
    )
    parser.add_argument(
        '--task-settings',
        help='the task settings set to use, see `tasks.__init__.py`'
    )
    parser.add_argument(
        '--datadir',
        nargs=2,
        action=DictAction,
        metavar=('NODE', 'PATH'),
        help="override datadir path for given node"
    )
    parser.add_argument(
        '--dump-output-on-fail',
        action='store_true',
        help="dump the nodes' outputs on test fail",
    )
    parser.add_argument(
        '--dump-output-on-crash',
        action='store_true',
        help="dump node output of the crashed node on abnormal termination",
    )
    parser.add_argument(
        '--mainnet',
        action='store_true',
        help="use the mainnet environment to run the test "
             "(the playbook must also use mainnet)",
    )
    return parser.parse_args()


def get_test_class(test_class_path: str) -> 'Type[Test]':
    test_module_path, _, test_class_name = \
        test_class_path.rpartition('.')
    test_path, _, test_module_name = \
        test_module_path.rpartition('.')
    tests_path = 'scripts.node_integration_tests.playbooks'

    if test_path:
        tests_path += '.' + test_path

    try:
        test_module = getattr(
            __import__(
                tests_path,
                fromlist=[test_module_name]
            ),
            test_module_name
        )

        return getattr(test_module, test_class_name)
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "The provided playbook `%s` couldn't be located in `playbooks` " %
            test_class_path
        ) from e


def override_config(config: 'Config', args: argparse.Namespace) -> None:
    for k, v in vars(args).items():
        if v is None:
            continue
        if k in [
                'task_package',
                'task_settings',
                'dump_output_on_fail',
                'dump_output_on_crash',
        ]:
            setattr(config, k, v)
        elif k == 'datadir':
            for node_name, datadir in v.items():
                node_id = NodeId(node_name)
                if node_id not in config.nodes:
                    raise Exception("can't override datadir for undefined node"
                                    f" '{node_name}'")
                node_configs = config.nodes[node_id]
                if isinstance(node_configs, list):
                    for node_config in node_configs:
                        node_config.datadir = datadir
                else:
                    node_configs.datadir = datadir


def main():
    args = parse_args()

    if args.mainnet:
        set_environment('mainnet', 'disabled')

    test_class = get_test_class(args.test_class)
    config = test_class.get_config()

    override_config(config, args)

    run_playbook(test_class.get_playbook_class(), config)


if __name__ == '__main__':
    main()
