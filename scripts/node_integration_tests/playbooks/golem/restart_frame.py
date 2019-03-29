from copy import deepcopy
from functools import partial
from typing import Tuple, Type, TYPE_CHECKING

from ..test_base import DebugTest, NodeId

if TYPE_CHECKING:
    from ..playbook_base import NodeTestPlaybook
    from ..test_base import Config


class RestartFrame(DebugTest):
    @staticmethod
    def get_config() -> 'Config':
        config = DebugTest.get_config()
        requestor_config = config.nodes[NodeId.requestor]
        requestor_config_2 = deepcopy(requestor_config)
        requestor_config_2.script = 'requestor/always_accept_provider'
        config.nodes[NodeId.requestor] = [
            requestor_config,
            requestor_config_2,
        ]
        return config

    @staticmethod
    def get_playbook_class() -> 'Type[NodeTestPlaybook]':
        from ..playbook_base import NodeTestPlaybook

        class Playbook(NodeTestPlaybook):
            def step_restart_task_frame(self):
                def on_success(result):
                    print(f'Restarted frame from task: {self.task_id}.')
                    self.next()

                return self.call(NodeId.requestor,
                                 'comp.task.subtasks.frame.restart',
                                 self.task_id,
                                 '1',
                                 on_success=on_success)

            def step_success(self):
                self.success()

            steps: Tuple = NodeTestPlaybook.initial_steps + (
                partial(NodeTestPlaybook.step_create_task,
                        node_id=NodeId.requestor),
                partial(NodeTestPlaybook.step_get_task_id,
                        node_id=NodeId.requestor),
                partial(NodeTestPlaybook.step_get_task_status,
                        node_id=NodeId.requestor),
                partial(NodeTestPlaybook.step_wait_task_finished,
                        node_id=NodeId.requestor),
                NodeTestPlaybook.step_stop_nodes,
                NodeTestPlaybook.step_restart_nodes,
            ) + NodeTestPlaybook.initial_steps + (
                partial(NodeTestPlaybook.step_get_nodes_known_tasks,
                        node_id=NodeId.requestor),
                step_restart_task_frame,
                partial(NodeTestPlaybook.step_get_task_id,
                        node_id=NodeId.requestor),
                partial(NodeTestPlaybook.step_get_task_status,
                        node_id=NodeId.requestor),
                partial(NodeTestPlaybook.step_wait_task_finished,
                        node_id=NodeId.requestor),
                NodeTestPlaybook.step_verify_output,
                step_success,
            )

        return Playbook
