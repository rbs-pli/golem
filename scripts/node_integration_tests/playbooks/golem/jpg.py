from typing import Type, TYPE_CHECKING

from apps.blender.task.blenderrendertask import BlenderTaskTypeInfo

from ..test_base import DebugTest

if TYPE_CHECKING:
    from ..test_base import Config
    from ..playbook_base import NodeTestPlaybook


class Jpg(DebugTest):
    @staticmethod
    def get_config() -> 'Config':
        config = DebugTest.get_config()
        config.task_settings = 'jpg'
        return config

    @staticmethod
    def get_playbook_class() -> 'Type[NodeTestPlaybook]':
        from ..playbook_base import NodeTestPlaybook

        class JpgPlaybook(NodeTestPlaybook):
            @property
            def output_extension(self):
                return BlenderTaskTypeInfo().output_formats[0]

        return JpgPlaybook
