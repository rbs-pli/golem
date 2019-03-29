from functools import partial
from typing import Tuple, Type, TYPE_CHECKING

from scripts.node_integration_tests import helpers

from ..test_base import DebugTest, NodeId

if TYPE_CHECKING:
    from ..playbook_base import NodeTestPlaybook


class RegularRun(DebugTest):
    @staticmethod
    def get_playbook_class() -> 'Type[NodeTestPlaybook]':
        from ..playbook_base import NodeTestPlaybook

        class Playbook(NodeTestPlaybook):
            def step_wait_task_finished(self):
                verification_rejected = helpers.search_output(
                    self.output_queues[NodeId.provider],
                    '.*SubtaskResultsRejected.*'
                )

                if verification_rejected:
                    self.fail(verification_rejected.group(0))
                    return

                return super().step_wait_task_finished(NodeId.requestor)

            steps: Tuple = NodeTestPlaybook.initial_steps + (
                partial(NodeTestPlaybook.step_create_task,
                        node_id=NodeId.requestor),
                partial(NodeTestPlaybook.step_get_task_id,
                        node_id=NodeId.requestor),
                partial(NodeTestPlaybook.step_get_task_status,
                        node_id=NodeId.requestor),
                step_wait_task_finished,
                NodeTestPlaybook.step_verify_output,
                partial(NodeTestPlaybook.step_get_subtasks,
                        node_id=NodeId.requestor),
                partial(NodeTestPlaybook.step_verify_node_income,
                        node_id=NodeId.provider, from_node=NodeId.requestor),
            )

        return Playbook
