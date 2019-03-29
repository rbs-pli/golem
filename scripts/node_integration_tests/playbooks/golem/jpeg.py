from typing import TYPE_CHECKING

from ..test_base import DebugTest

if TYPE_CHECKING:
    from ..test_base import Config


class Jpeg(DebugTest):
    @staticmethod
    def get_config() -> 'Config':
        config = DebugTest.get_config()
        config.task_settings = 'jpeg'
        return config
