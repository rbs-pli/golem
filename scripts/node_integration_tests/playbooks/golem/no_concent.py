from typing import TYPE_CHECKING

from ..test_base import DebugTest

if TYPE_CHECKING:
    from ..test_base import Config


class NoConcent(DebugTest):
    @staticmethod
    def get_config() -> 'Config':
        config = DebugTest.get_config()
        for node_config in config.nodes.values():
            node_config.concent = 'disabled'
        return config
