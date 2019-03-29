import enum
import os
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Type,
    TYPE_CHECKING,
    Union,
)

# DO NOT IMPORT NodeTestPlaybook HERE!!! See Test class docs for details.

if TYPE_CHECKING:
    from .playbook_base import NodeTestPlaybook


class NodeConfig:
    def __init__(
            self,
            *,
            concent: str = 'staging',
            datadir: Optional[str] = None,
            log_level: Optional[str] = None,
            mainnet: bool = False,
            opts: Dict[str, Any] = {},
            password: str = 'dupa.8',
            protocol_id: int = 1337,
            rpc_port: int = 61000,
            script: str = 'node',
            ) -> None:
        self.concent = concent
        # if datadir is None it will be automatically created
        self.datadir = datadir
        self.log_level = log_level
        self.mainnet = mainnet
        self.opts = opts
        self.password = password
        self.protocol_id = protocol_id
        self.rpc_port = rpc_port
        self.script = script

    def make_args(self) -> Dict[str, Any]:
        args = {
            '--accept-concent-terms': None,
            '--accept-terms': None,
            '--concent': self.concent,
            '--datadir': self.datadir,
            '--password': self.password,
            '--protocol_id': self.protocol_id,
            '--rpc-address': f'localhost:{self.rpc_port}',
        }
        if self.log_level is not None:
            args['--log-level'] = self.log_level
        if self.mainnet:
            args['--mainnet'] = None
        return args

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.__dict__}"


class NodeId(enum.Enum):
    """
    This enum holds commonly used nodes names.
    Feel free to extend this enum in your tests that require more nodes.
    """
    def _generate_next_value_(name, start, count, last_values):
        return name

    requestor = enum.auto()
    provider = enum.auto()


class Config:
    def __init__(
            self,
            *,
            dump_output_on_crash: bool = False,
            dump_output_on_fail: bool = False,
            nodes: Dict[NodeId, Union[NodeConfig, List[NodeConfig]]] = {},
            task_package: str = 'test_task_1',
            task_settings: str = 'default',
            ) -> None:
        self.dump_output_on_crash = dump_output_on_crash
        self.dump_output_on_fail = dump_output_on_fail
        self.nodes = nodes
        self._nodes_index = 0
        self.task_package = task_package
        self.task_settings = task_settings

    @property
    def current_nodes(self) -> Dict[NodeId, NodeConfig]:
        return {
            node_id: (
                node_config if isinstance(node_config, NodeConfig)
                else node_config[min(self._nodes_index, len(node_config)-1)]
            )
            for node_id, node_config in self.nodes.items()
        }

    def next_nodes(self) -> None:
        self._nodes_index += 1

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.__dict__}"


class Test:
    """
    This class provides a basic interface to define an integration test.

    The separation of config and playbook is needed, because playbook is heavy
    and has a lot of requirements. In ../tests/base.py we want to get only
    config just to properly set-up key reuse.
    """

    @staticmethod
    def get_config() -> Config:
        concent = os.environ.get('GOLEM_CONCENT_VARIANT', 'staging')
        return Config(
            nodes={
                NodeId.requestor: NodeConfig(
                    concent=concent,
                    password=os.environ.get(
                        'GOLEM_REQUESTOR_PASSWORD', 'dupa.8'),
                    rpc_port=int(os.environ.get(
                        'GOLEM_REQUESTOR_RPC_PORT', '61000')),
                ),
                NodeId.provider: NodeConfig(
                    concent=concent,
                    password=os.environ.get(
                        'GOLEM_PROVIDER_PASSWORD', 'dupa.8'),
                    rpc_port=int(os.environ.get(
                        'GOLEM_PROVIDER_RPC_PORT', '61001')),
                ),
            }
        )

    @staticmethod
    def get_playbook_class() -> 'Type[NodeTestPlaybook]':
        from .playbook_base import NodeTestPlaybook
        return NodeTestPlaybook


class DebugTest(Test):
    @staticmethod
    def get_config() -> Config:
        config = Test.get_config()
        for node_config in config.nodes.values():
            node_config.log_level = 'DEBUG'
        return config
