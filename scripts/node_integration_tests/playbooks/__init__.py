import sys
from typing import Type, TYPE_CHECKING

if TYPE_CHECKING:
    from .playbook_base import Config, NodeTestPlaybook


def run_playbook(playbook_cls: 'Type[NodeTestPlaybook]', config: 'Config'):
    playbook = playbook_cls.start(config)

    if playbook.exit_code:
        print("exit code", playbook.exit_code)

    sys.exit(playbook.exit_code)
