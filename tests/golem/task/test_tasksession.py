# pylint: disable=too-many-lines, protected-access
import calendar
import datetime
import os
import pathlib
import pickle
import random
import time
import uuid
from unittest import TestCase
from unittest.mock import patch, ANY, Mock, MagicMock

from golem_messages import factories as msg_factories
from golem_messages import idgenerator
from golem_messages import message
from golem_messages import cryptography
from golem_messages.factories.datastructures import p2p as dt_p2p_factory
from golem_messages.factories.datastructures import tasks as dt_tasks_factory
from golem_messages.utils import encode_hex
from pydispatch import dispatcher

from twisted.internet.defer import Deferred

import golem
from golem import model, testutils
from golem.core import variables
from golem.core.keysauth import KeysAuth
from golem.docker.environment import DockerEnvironment
from golem.docker.image import DockerImage
from golem.network.hyperdrive import client as hyperdrive_client
from golem.model import Actor
from golem.network import history
from golem.network.hyperdrive.client import HyperdriveClientOptions
from golem.task import taskstate
from golem.task.taskkeeper import CompTaskKeeper
from golem.task.tasksession import TaskSession, logger, get_task_message
from golem.tools.assertlogs import LogTestCase

from tests import factories


def fill_slots(msg):
    for slot in msg.__slots__:
        if hasattr(msg, slot):
            continue
        setattr(msg, slot, None)


class DockerEnvironmentMock(DockerEnvironment):
    DOCKER_IMAGE = ""
    DOCKER_TAG = ""
    ENV_ID = ""
    SHORT_DESCRIPTION = ""


class TestTaskSessionPep8(testutils.PEP8MixIn, TestCase):
    PEP8_FILES = [
        'golem/task/tasksession.py',
        'tests/golem/task/test_tasksession.py',
    ]


class ConcentMessageMixin():
    def assert_concent_cancel(self, mock_call, subtask_id, message_class_name):
        self.assertEqual(mock_call[0], subtask_id)
        self.assertEqual(mock_call[1], message_class_name)

    def assert_concent_submit(self, mock_call, subtask_id, message_class):
        self.assertEqual(mock_call[0], subtask_id)
        self.assertIsInstance(mock_call[1], message_class)


def _offerpool_add(*_):
    res = Deferred()
    res.callback(True)
    return res


# pylint:disable=no-member,too-many-instance-attributes
@patch('golem.task.tasksession.OfferPool.add', _offerpool_add)
@patch('golem.task.tasksession.get_provider_efficiency', Mock())
@patch('golem.task.tasksession.get_provider_efficacy', Mock())
class TaskSessionTaskToComputeTest(TestCase):
    def setUp(self):
        self.maxDiff = None
        self.requestor_keys = cryptography.ECCx(None)
        self.requestor_key = encode_hex(self.requestor_keys.raw_pubkey)
        self.provider_keys = cryptography.ECCx(None)
        self.provider_key = encode_hex(self.provider_keys.raw_pubkey)

        self.task_manager = Mock(tasks_states={}, tasks={})
        server = Mock(task_manager=self.task_manager)
        server.get_key_id = lambda: self.provider_key
        server.get_share_options.return_value = None
        self.conn = Mock(server=server)
        self.use_concent = True
        self.task_id = uuid.uuid4().hex
        self.node_name = 'ABC'

    def _get_task_session(self):
        ts = TaskSession(self.conn)
        ts._is_peer_blocked = Mock(return_value=False)
        ts.verified = True
        ts.concent_service.enabled = self.use_concent
        ts.key_id = 'requestor key id'
        return ts

    def _get_requestor_tasksession(self, accept_provider=True):
        ts = self._get_task_session()
        ts.key_id = "provider key id"
        ts.can_be_not_encrypted.append(message.tasks.WantToComputeTask)
        ts.task_server.should_accept_provider.return_value = accept_provider
        ts.task_server.config_desc.max_price = 100
        ts.task_server.keys_auth._private_key = \
            self.requestor_keys.raw_privkey
        ts.task_server.keys_auth.public_key = self.requestor_keys.raw_pubkey
        ts.conn.send_message.side_effect = lambda msg: msg._fake_sign()
        return ts

    def _get_task_parameters(self):
        return {
            'node_name': self.node_name,
            'perf_index': 1030,
            'price': 30,
            'max_resource_size': 3,
            'max_memory_size': 1,
            'task_header': self._get_task_header()
        }

    def _get_wtct(self):
        msg = message.tasks.WantToComputeTask(
            concent_enabled=self.use_concent,
            **self._get_task_parameters()
        )
        msg.sign_message(self.provider_keys.raw_privkey)  # noqa pylint: disable=no-member
        return msg

    def _fake_add_task(self):
        task_header = self._get_task_header()
        self.task_manager.tasks[self.task_id] = Mock(header=task_header)

    def _get_task_header(self):
        task_header = dt_tasks_factory.TaskHeaderFactory(
            task_id=self.task_id,
            task_owner=dt_p2p_factory.Node(
                key=self.requestor_key,
            ),
            subtask_timeout=1,
            max_price=1, )
        task_header.sign(self.requestor_keys.raw_privkey)  # noqa pylint: disable=no-value-for-parameter
        return task_header

    def _set_task_state(self):
        task_state = taskstate.TaskState()
        task_state.package_hash = '667'
        task_state.package_size = 42
        self.conn.server.task_manager.tasks_states[self.task_id] = task_state
        return task_state

    @patch('golem.network.history.MessageHistoryService.instance')
    def test_cannot_assign_task_provider_not_accepted(self, *_):
        mt = self._get_wtct()
        ts2 = self._get_requestor_tasksession(accept_provider=False)
        self._fake_add_task()

        ctd = message.tasks.ComputeTaskDef(task_id=mt.task_id)
        self._set_task_state()

        ts2.task_manager.get_next_subtask.return_value = ctd
        ts2.task_manager.should_wait_for_node.return_value = False
        ts2.task_server.should_accept_provider.return_value = False
        ts2.interpret(mt)
        ms = ts2.conn.send_message.call_args[0][0]
        self.assertIsInstance(ms, message.tasks.CannotAssignTask)
        self.assertEqual(ms.task_id, mt.task_id)

    @patch('golem.network.history.MessageHistoryService.instance')
    def test_cannot_assign_task_wrong_ctd(self, *_):
        mt = self._get_wtct()
        ts2 = self._get_requestor_tasksession()
        self._fake_add_task()

        self._set_task_state()

        ts2.task_manager.should_wait_for_node.return_value = False
        ts2.task_manager.check_next_subtask.return_value = False
        ts2.interpret(mt)
        ts2.task_manager.check_next_subtask.assert_called_once_with(
            mt.task_id,
            mt.price,
        )
        ms = ts2.conn.send_message.call_args[0][0]
        self.assertIsInstance(ms, message.tasks.CannotAssignTask)
        self.assertEqual(ms.task_id, mt.task_id)

    def test_cannot_compute_task_computation_failure(self):
        ts2 = self._get_requestor_tasksession()
        ts2.task_manager.get_node_id_for_subtask.return_value = ts2.key_id
        ts2._react_to_cannot_compute_task(message.tasks.CannotComputeTask(
            reason=message.tasks.CannotComputeTask.REASON.WrongCTD,
            task_to_compute=None,
        ))
        assert ts2.task_manager.task_computation_cancelled.called

    def test_cannot_compute_task_bad_subtask_id(self):
        ts2 = self._get_requestor_tasksession()
        ts2.task_manager.task_computation_failure.called = False
        ts2.task_manager.get_node_id_for_subtask.return_value = "___"
        ts2._react_to_cannot_compute_task(message.tasks.CannotComputeTask(
            reason=message.tasks.CannotComputeTask.REASON.WrongCTD,
            task_to_compute=None,
        ))
        assert not ts2.task_manager.task_computation_failure.called

    @patch('golem.network.history.MessageHistoryService.instance')
    def test_request_task(self, *_):
        mt = self._get_wtct()
        ts2 = self._get_requestor_tasksession(accept_provider=True)
        self._fake_add_task()

        ctd = message.tasks.ComputeTaskDef(task_id=mt.task_id)
        task_state = self._set_task_state()

        ts2.task_manager.get_next_subtask.return_value = ctd
        ts2.task_manager.should_wait_for_node.return_value = False
        ts2.conn.send_message.side_effect = \
            lambda msg: msg.sign_message(self.requestor_keys.raw_privkey)
        options = HyperdriveClientOptions("CLI1", 0.3)
        ts2.task_server.get_share_options.return_value = options
        ts2.interpret(mt)
        ms = ts2.conn.send_message.call_args[0][0]
        self.assertIsInstance(ms, message.tasks.TaskToCompute)
        expected = [
            ['requestor_id', self.requestor_key],
            ['provider_id', ts2.key_id],
            ['requestor_public_key', self.requestor_key],
            ['requestor_ethereum_public_key', self.requestor_key],
            ['compute_task_def', ctd],
            ['want_to_compute_task', (False, (mt.header, mt.sig, mt.slots()))],
            ['package_hash', 'sha1:' + task_state.package_hash],
            ['concent_enabled', self.use_concent],
            ['price', 1],
            ['size', task_state.package_size],
            ['ethsig', ms.ethsig],
            ['resources_options', {'client_id': 'CLI1', 'version': 0.3,
                                   'options': {}}],
        ]
        self.assertCountEqual(ms.slots(), expected)

    def test_task_to_compute_eth_signature(self):
        wtct = self._get_wtct()
        ts2 = self._get_requestor_tasksession(accept_provider=True)
        self._fake_add_task()

        ctd = message.tasks.ComputeTaskDef(task_id=wtct.task_id)
        self._set_task_state()

        ts2.task_manager.get_next_subtask.return_value = ctd
        ts2.task_manager.should_wait_for_node.return_value = False
        options = HyperdriveClientOptions("CLI1", 0.3)
        ts2.task_server.get_share_options.return_value = options
        ts2.interpret(wtct)
        ttc = ts2.conn.send_message.call_args[0][0]
        self.assertIsInstance(ttc, message.tasks.TaskToCompute)
        self.assertEqual(ttc.requestor_ethereum_public_key, self.requestor_key)
        self.assertTrue(ttc.verify_ethsig())

# pylint:enable=no-member


@patch("golem.network.nodeskeeper.store")
class TestTaskSession(ConcentMessageMixin, LogTestCase,
                      testutils.TempDirFixture):

    def setUp(self):
        super().setUp()
        random.seed()
        self.task_session = TaskSession(Mock())
        self.task_session.key_id = 'unittest_key_id'
        self.task_session.task_server.get_share_options.return_value = \
            hyperdrive_client.HyperdriveClientOptions('1', 1.0)
        keys_auth = KeysAuth(
            datadir=self.path,
            difficulty=4,
            private_key_name='prv',
            password='',
        )
        self.task_session.task_server.keys_auth = keys_auth
        self.pubkey = keys_auth.public_key
        self.privkey = keys_auth._private_key

    @patch('golem.task.tasksession.TaskSession.send')
    def test_hello(self, send_mock, *_):
        self.task_session.conn.server.get_key_id.return_value = key_id = \
            'key id%d' % (random.random() * 1000,)
        node = dt_p2p_factory.Node()
        self.task_session.task_server.client.node = node
        self.task_session.send_hello()
        expected = [
            ['rand_val', self.task_session.rand_val],
            ['proto_id', variables.PROTOCOL_CONST.ID],
            ['node_name', None],
            ['node_info', node.to_dict()],
            ['port', None],
            ['client_ver', golem.__version__],
            ['client_key_id', key_id],
            ['solve_challenge', None],
            ['challenge', None],
            ['difficulty', None],
            ['metadata', None],
        ]
        msg = send_mock.call_args[0][0]
        self.assertCountEqual(msg.slots(), expected)

    @patch(
        'golem.network.history.MessageHistoryService.get_sync_as_message',
    )
    @patch(
        'golem.network.history.add',
    )
    def test_send_report_computed_task(self, add_mock, get_mock, *_):
        ts = self.task_session
        ts.verified = True
        ts.task_server.get_node_name.return_value = "ABC"
        wtr = factories.taskserver.WaitingTaskResultFactory()

        ttc = msg_factories.tasks.TaskToComputeFactory(
            task_id=wtr.task_id,
            subtask_id=wtr.subtask_id,
            compute_task_def__deadline=calendar.timegm(time.gmtime()) + 3600,
        )
        get_mock.return_value = ttc
        ts.task_server.get_key_id.return_value = 'key id'
        ts.send_report_computed_task(
            wtr, wtr.owner.pub_addr, wtr.owner.pub_port, wtr.owner)

        rct: message.tasks.ReportComputedTask = \
            ts.conn.send_message.call_args[0][0]
        self.assertIsInstance(rct, message.tasks.ReportComputedTask)
        self.assertEqual(rct.subtask_id, wtr.subtask_id)
        self.assertEqual(rct.node_name, "ABC")
        self.assertEqual(rct.address, wtr.owner.pub_addr)
        self.assertEqual(rct.port, wtr.owner.pub_port)
        self.assertEqual(rct.extra_data, [])
        self.assertEqual(rct.node_info, wtr.owner.to_dict())
        self.assertEqual(rct.package_hash, 'sha1:' + wtr.package_sha1)
        self.assertEqual(rct.multihash, wtr.result_hash)
        self.assertEqual(rct.secret, wtr.result_secret)

        add_mock.assert_called_once_with(
            msg=ANY,
            node_id=ts.key_id,
            local_role=Actor.Provider,
            remote_role=Actor.Requestor,
        )

        ts2 = TaskSession(Mock())
        ts2.verified = True
        ts2.key_id = "DEF"
        ts2.can_be_not_encrypted.append(rct.__class__)
        ts2.task_manager.subtask2task_mapping = {wtr.subtask_id: wtr.task_id}
        task_state = taskstate.TaskState()
        task_state.subtask_states[wtr.subtask_id] = taskstate.SubtaskState()
        task_state.subtask_states[wtr.subtask_id].deadline = \
            calendar.timegm(time.gmtime()) + 3600
        ts2.task_manager.tasks_states = {
            wtr.task_id: task_state,
        }
        ts2.task_manager.get_node_id_for_subtask.return_value = "DEF"
        get_mock.side_effect = history.MessageNotFound

        with patch(
            'golem.network.concent.helpers.process_report_computed_task',
            return_value=msg_factories.tasks.AckReportComputedTaskFactory()
        ):
            ts2.interpret(rct)

    def test_react_to_hello_nodeskeeper_store(self, mock_store, *_):
        msg = msg_factories.base.HelloFactory()
        self.task_session._react_to_hello(msg)
        mock_store.assert_called_once_with(msg.node_info)

    def test_react_to_hello_protocol_version(self, *_):
        # given
        conn = MagicMock()
        ts = TaskSession(conn)
        ts.task_server.config_desc = Mock()
        ts.task_server.config_desc.key_difficulty = 0
        ts.disconnect = Mock()
        ts.send = Mock()

        key_id = 'deadbeef'
        peer_info = MagicMock()
        peer_info.key = key_id
        msg = message.base.Hello(
            port=1, node_name='node2', client_key_id=key_id,
            node_info=peer_info, proto_id=-1)
        fill_slots(msg)

        # when
        with self.assertLogs(logger, level='INFO'):
            ts._react_to_hello(msg)

        # then
        ts.disconnect.assert_called_with(
            message.base.Disconnect.REASON.ProtocolVersion)

        # re-given
        msg.proto_id = variables.PROTOCOL_CONST.ID

        # re-when
        with self.assertNoLogs(logger, level='INFO'):
            ts._react_to_hello(msg)

        # re-then
        self.assertTrue(ts.send.called)

    def test_react_to_hello_key_not_difficult(self, *_):
        # given
        conn = MagicMock()
        ts = TaskSession(conn)
        ts.task_server.config_desc = Mock()
        ts.task_server.config_desc.key_difficulty = 80
        ts.disconnect = Mock()
        ts.send = Mock()

        key_id = 'deadbeef'
        peer_info = MagicMock()
        peer_info.key = key_id
        msg = message.base.Hello(
            port=1, node_name='node2', client_key_id=key_id,
            node_info=peer_info, proto_id=variables.PROTOCOL_CONST.ID)
        fill_slots(msg)

        # when
        with self.assertLogs(logger, level='INFO'):
            ts._react_to_hello(msg)

        # then
        ts.disconnect.assert_called_with(
            message.base.Disconnect.REASON.KeyNotDifficult)

    def test_react_to_hello_key_difficult(self, *_):
        # given
        difficulty = 4
        conn = MagicMock()
        ts = TaskSession(conn)
        ts.task_server.config_desc = Mock()
        ts.task_server.config_desc.key_difficulty = difficulty
        ts.disconnect = Mock()
        ts.send = Mock()

        ka = KeysAuth(datadir=self.path, difficulty=difficulty,
                      private_key_name='prv', password='')
        peer_info = MagicMock()
        peer_info.key = ka.key_id
        msg = message.base.Hello(
            port=1, node_name='node2', client_key_id=ka.key_id,
            node_info=peer_info, proto_id=variables.PROTOCOL_CONST.ID)
        fill_slots(msg)

        # when
        with self.assertNoLogs(logger, level='INFO'):
            ts._react_to_hello(msg)
        # then
        self.assertTrue(ts.send.called)

    @patch('golem.task.tasksession.get_task_message')
    def test_result_received(self, get_msg_mock, *_):
        conn = Mock()
        conn.send_message.side_effect = lambda msg: msg._fake_sign()
        ts = TaskSession(conn)
        ts.task_manager.verify_subtask.return_value = True
        keys_auth = KeysAuth(
            datadir=self.path,
            difficulty=4,
            private_key_name='prv',
            password='',
        )
        ts.task_server.keys_auth = keys_auth
        subtask_id = "xxyyzz"
        get_msg_mock.return_value = msg_factories \
            .tasks.ReportComputedTaskFactory(
                subtask_id=subtask_id,
            )

        def finished():
            if not ts.task_manager.verify_subtask(subtask_id):
                ts._reject_subtask_result(subtask_id, '')
                ts.dropped()
                return

            payment = ts.task_server.accept_result(
                subtask_id,
                'key_id',
                'eth_address',
            )
            rct = msg_factories.tasks.ReportComputedTaskFactory(
                task_to_compute__compute_task_def__subtask_id=subtask_id,
            )
            ts.send(msg_factories.tasks.SubtaskResultsAcceptedFactory(
                report_computed_task=rct,
                payment_ts=payment.processed_ts))
            ts.dropped()

        ts.task_manager.computed_task_received = Mock(
            side_effect=finished(),
        )
        ts.result_received(subtask_id, pickle.dumps({'stdout': 'xyz'}))

        self.assertTrue(ts.msgs_to_send)
        sra = ts.msgs_to_send[0]
        self.assertIsInstance(sra, message.tasks.SubtaskResultsAccepted)

        conn.close.assert_called()

    def _get_srr(self, key2=None, concent=False):
        key1 = 'known'
        key2 = key2 or key1
        srr = msg_factories.tasks.SubtaskResultsRejectedFactory(
            report_computed_task__task_to_compute__concent_enabled=concent
        )
        srr._fake_sign()
        ctk = self.task_session.task_manager.comp_task_keeper
        ctk.get_node_for_task_id.return_value = key1
        self.task_session.key_id = key2
        return srr

    def __call_react_to_srr(self, srr):
        with patch('golem.task.tasksession.TaskSession.dropped') as dropped:
            self.task_session._react_to_subtask_results_rejected(srr)
        dropped.assert_called_once_with()

    def test_result_rejected(self, *_):
        dispatch_listener = Mock()
        dispatcher.connect(dispatch_listener, signal='golem.message')

        srr = self._get_srr()
        self.__call_react_to_srr(srr)

        self.task_session.task_server.subtask_rejected.assert_called_once_with(
            sender_node_id=self.task_session.key_id,
            subtask_id=srr.report_computed_task.subtask_id,  # noqa pylint:disable=no-member
        )

        dispatch_listener.assert_called_once_with(
            event='received',
            signal='golem.message',
            message=srr,
            sender=ANY,
        )

    def test_result_rejected_with_wrong_key(self, *_):
        srr = self._get_srr(key2='notmine')
        self.__call_react_to_srr(srr)
        self.task_session.task_server.subtask_rejected.assert_not_called()

    def test_result_rejected_with_concent(self, *_):
        srr = self._get_srr(concent=True)

        def concent_deposit(**_):
            result = Deferred()
            result.callback(None)
            return result

        self.task_session.task_server.client.transaction_system\
            .concent_deposit.side_effect = concent_deposit
        self.__call_react_to_srr(srr)
        stm = self.task_session.concent_service.submit_task_message
        stm.assert_called()
        kwargs = stm.call_args_list[0][1]
        self.assertEqual(kwargs['subtask_id'], srr.subtask_id)
        self.assertIsInstance(kwargs['msg'],
                              message.concents.SubtaskResultsVerify)
        self.assertEqual(kwargs['msg'].subtask_results_rejected, srr)

    # pylint: disable=too-many-statements
    def test_react_to_task_to_compute(self, *_):
        conn = Mock()
        ts = TaskSession(conn)
        ts.key_id = "KEY_ID"
        ts.task_computer.has_assigned_task.return_value = False
        ts.concent_service.enabled = False
        ts.send = Mock(side_effect=lambda msg: print(f"send {msg}"))

        env = Mock()
        env.docker_images = [DockerImage("dockerix/xii", tag="323")]
        ts.task_server.get_environment_by_id.return_value = env

        keys = cryptography.ECCx(None)
        ts.task_server.keys_auth.ecc.raw_pubkey = keys.raw_pubkey

        reasons = message.tasks.CannotComputeTask.REASON

        def __reset_mocks():
            ts.send.reset_mock()
            ts.task_manager.reset_mock()
            ts.task_computer.reset_mock()
            conn.reset_mock()

        # msg.ctd is None -> failure
        msg = msg_factories.tasks.TaskToComputeFactory(compute_task_def=None)
        msg.want_to_compute_task.sign_message(keys.raw_privkey)  # noqa pylint: disable=no-member
        msg._fake_sign()
        ts._react_to_task_to_compute(msg)
        ts.task_server.add_task_session.assert_not_called()
        ts.task_server.task_given.assert_not_called()
        ts.task_manager.comp_task_keeper.receive_subtask.assert_not_called()
        ts.send.assert_not_called()
        ts.task_computer.session_closed.assert_called_with()
        assert conn.close.called

        # No source code in the local environment -> failure
        __reset_mocks()
        header = ts.task_manager.comp_task_keeper.get_task_header()
        header.task_owner.key = 'KEY_ID'
        header.task_owner.pub_addr = '10.10.10.10'
        header.task_owner.pub_port = 1112

        ctd = message.tasks.ComputeTaskDef()
        ctd['docker_images'] = [
            DockerImage("dockerix/xiii", tag="323").to_dict(),
        ]

        def _prepare_and_react(compute_task_def, resource_size=102400):
            msg = msg_factories.tasks.TaskToComputeFactory(
                compute_task_def=compute_task_def,
            )
            msg.want_to_compute_task.provider_public_key = encode_hex(
                keys.raw_pubkey)
            msg.want_to_compute_task.sign_message(keys.raw_privkey)  # noqa pylint: disable=no-member
            msg._fake_sign()
            ts.task_server.task_keeper.task_headers = {
                msg.task_id: MagicMock(),
            }
            ts.task_server.task_keeper\
                .task_headers[msg.task_id].subtasks_count = 10
            ts.task_server.client.transaction_system.get_available_gnt\
                .return_value = msg.price * 10
            ts.task_server.config_desc.max_resource_size = resource_size
            ts._react_to_task_to_compute(msg)
            return msg

        # Source code from local environment -> proper execution
        __reset_mocks()
        env.get_source_code.return_value = "print 'Hello world'"
        msg = _prepare_and_react(ctd)
        ts.task_manager.comp_task_keeper.receive_subtask.assert_called_with(msg)
        ts.task_computer.session_closed.assert_not_called()
        ts.task_server.add_task_session.assert_called_with(msg.subtask_id, ts)
        ts.task_server.task_given.assert_called_with(
            header.task_owner.key,
            ctd,
            msg.price,
        )
        conn.close.assert_not_called()

        def __assert_failure(ts, conn, reason):
            ts.task_manager.comp_task_keeper.receive_subtask.assert_not_called()
            ts.task_computer.session_closed.assert_called_with()
            assert conn.close.called
            ts.send.assert_called_once_with(ANY)
            msg = ts.send.call_args[0][0]
            self.assertIsInstance(msg, message.tasks.CannotComputeTask)
            self.assertIs(msg.reason, reason)

        # Wrong key id -> failure
        __reset_mocks()
        header.task_owner.key = 'KEY_ID2'

        _prepare_and_react(ctd)
        __assert_failure(ts, conn, reasons.WrongKey)

        # Wrong task owner key id -> failure
        __reset_mocks()
        header.task_owner.key = 'KEY_ID2'

        _prepare_and_react(ctd)
        __assert_failure(ts, conn, reasons.WrongKey)

        # Wrong return port -> failure
        __reset_mocks()
        header.task_owner.key = 'KEY_ID'
        header.task_owner.pub_port = 0

        _prepare_and_react(ctd)
        __assert_failure(ts, conn, reasons.WrongAddress)

        # Proper port and key -> proper execution
        __reset_mocks()
        header.task_owner.pub_port = 1112

        _prepare_and_react(ctd)
        conn.close.assert_not_called()

        # Wrong data size -> failure
        __reset_mocks()
        _prepare_and_react(ctd, 1024)
        __assert_failure(ts, conn, reasons.ResourcesTooBig)

        # Allow custom code / code in ComputerTaskDef -> proper execution
        __reset_mocks()
        ctd['extra_data']['src_code'] = "print 'Hello world!'"
        msg = _prepare_and_react(ctd)
        ts.task_computer.session_closed.assert_not_called()
        ts.task_server.add_task_session.assert_called_with(msg.subtask_id, ts)
        ts.task_server.task_given.assert_called_with(
            header.task_owner.key,
            ctd,
            msg.price,
        )
        conn.close.assert_not_called()

        # No environment available -> failure
        __reset_mocks()
        ts.task_server.get_environment_by_id.return_value = None
        _prepare_and_react(ctd)
        __assert_failure(ts, conn, reasons.WrongEnvironment)

        # Envrionment is Docker environment but with different images -> failure
        __reset_mocks()
        ts.task_server.get_environment_by_id.return_value = \
            DockerEnvironmentMock(additional_images=[
                DockerImage("dockerix/xii", tag="323"),
                DockerImage("dockerix/xiii", tag="325"),
                DockerImage("dockerix/xiii")
            ])
        _prepare_and_react(ctd)
        __assert_failure(ts, conn, reasons.WrongDockerImages)

    @patch('golem.task.taskkeeper.ProviderStatsManager', Mock())
    def test_react_to_ack_reject_report_computed_task(self, *_):
        task_keeper = CompTaskKeeper(pathlib.Path(self.path))

        session = self.task_session
        session.conn.server.client.concent_service = MagicMock()
        session.task_manager.comp_task_keeper = task_keeper
        session.key_id = 'owner_id'

        cancel = session.concent_service.cancel_task_message

        ttc = msg_factories.tasks.TaskToComputeFactory(
            concent_enabled=True,
        )
        task_id = ttc.task_id
        subtask_id = ttc.subtask_id

        rct = msg_factories.tasks.ReportComputedTaskFactory(
            task_to_compute=ttc)

        msg_ack = message.tasks.AckReportComputedTask(
            report_computed_task=rct
        )
        msg_ack._fake_sign()
        msg_rej = message.tasks.RejectReportComputedTask(
            attached_task_to_compute=ttc
        )
        msg_rej._fake_sign()

        # Subtask is not known
        session._react_to_ack_report_computed_task(msg_ack)
        self.assertFalse(cancel.called)
        session._react_to_reject_report_computed_task(msg_rej)
        self.assertFalse(cancel.called)

        # Save subtask information
        task_owner = dt_p2p_factory.Node(key='owner_id')
        task = Mock(header=Mock(task_owner=task_owner))
        task_keeper.subtask_to_task[subtask_id] = task_id
        task_keeper.active_tasks[task_id] = task

        # Subtask is known
        with patch("golem.task.tasksession.get_task_message") as get_mock:
            get_mock.return_value = rct
            session._react_to_ack_report_computed_task(msg_ack)
            session.concent_service.submit_task_message.assert_called_once_with(
                subtask_id=msg_ack.subtask_id,
                msg=ANY,
                delay=ANY,
            )
        self.assertTrue(cancel.called)
        self.assert_concent_cancel(
            cancel.call_args[0], subtask_id, 'ForceReportComputedTask')

        cancel.reset_mock()
        session._react_to_reject_report_computed_task(msg_ack)
        self.assert_concent_cancel(
            cancel.call_args[0], subtask_id, 'ForceReportComputedTask')

    def test_subtask_to_task(self, *_):
        task_keeper = Mock(subtask_to_task=dict())
        mapping = dict()

        self.task_session.task_manager.comp_task_keeper = task_keeper
        self.task_session.task_manager.subtask2task_mapping = mapping
        task_keeper.subtask_to_task['sid_1'] = 'task_1'
        mapping['sid_2'] = 'task_2'

        assert self.task_session._subtask_to_task('sid_1', Actor.Provider)
        assert self.task_session._subtask_to_task('sid_2', Actor.Requestor)
        assert not self.task_session._subtask_to_task('sid_2', Actor.Provider)
        assert not self.task_session._subtask_to_task('sid_1', Actor.Requestor)

    @patch('golem.task.taskkeeper.ProviderStatsManager', Mock())
    def test_react_to_cannot_assign_task(self, *_):
        self._test_react_to_cannot_assign_task()

    @patch('golem.task.taskkeeper.ProviderStatsManager', Mock())
    def test_react_to_cannot_assign_task_with_wrong_sender(self, *_):
        self._test_react_to_cannot_assign_task("KEY_ID2", expected_requests=1)

    def _test_react_to_cannot_assign_task(
            self,
            key_id="KEY_ID",
            expected_requests=0,
    ):
        task_keeper = CompTaskKeeper(self.new_path)
        task_keeper.add_request(
            dt_tasks_factory.TaskHeaderFactory(
                task_id="abc",
                task_owner=dt_p2p_factory.Node(
                    key="KEY_ID",
                ),
                subtask_timeout=1,
                max_price=1,
            ),
            20,
        )
        assert task_keeper.active_tasks["abc"].requests == 1
        self.task_session.task_manager.comp_task_keeper = task_keeper
        msg_cat = message.tasks.CannotAssignTask(task_id="abc")
        msg_cat._fake_sign()
        self.task_session.key_id = key_id
        self.task_session._react_to_cannot_assign_task(msg_cat)
        self.assertEqual(
            task_keeper.active_tasks["abc"].requests,
            expected_requests,
        )

    def test_react_to_want_to_compute_no_handshake(self, *_):
        mock_msg = Mock()
        mock_msg.concent_enabled = False

        self._prepare_handshake_test()

        ts = self.task_session

        ts._handshake_required = Mock()
        ts._handshake_required.return_value = True

        with self.assertLogs(logger, level='WARNING'):
            ts._react_to_want_to_compute_task(mock_msg)

        ts.task_server.start_handshake.assert_called_once_with(ts.key_id)

    def test_react_to_want_to_compute_handshake_busy(self, *_):
        mock_msg = Mock()
        mock_msg.concent_enabled = False

        self._prepare_handshake_test()

        ts = self.task_session

        ts._handshake_required = Mock()
        ts._handshake_required.return_value = False

        ts._handshake_in_progress = Mock()
        ts._handshake_in_progress.return_value = True

        with self.assertLogs(logger, level='WARNING'):
            ts._react_to_want_to_compute_task(mock_msg)

    def test_react_to_want_to_compute_invalid_task_header_signature(self, *_):
        different_requestor_keys = cryptography.ECCx(None)
        provider_keys = cryptography.ECCx(None)
        wtct = msg_factories.tasks.WantToComputeTaskFactory(
            sign__privkey=provider_keys.raw_privkey,
            task_header__sign__privkey=different_requestor_keys.raw_privkey,
        )
        self._prepare_handshake_test()
        ts = self.task_session
        ts.verified = True

        ts._react_to_want_to_compute_task(wtct)

        sent_msg = ts.conn.send_message.call_args[0][0]
        ts.task_server.remove_task_session.assert_called()
        self.assertIsInstance(sent_msg, message.tasks.CannotAssignTask)
        self.assertEqual(sent_msg.reason,
                         message.tasks.CannotAssignTask.REASON.NotMyTask)

    def test_react_to_want_to_compute_not_my_task_id(self, *_):
        provider_keys = cryptography.ECCx(None)
        wtct = msg_factories.tasks.WantToComputeTaskFactory(
            sign__privkey=provider_keys.raw_privkey,
            task_header__sign__privkey=self.privkey,
        )
        self._prepare_handshake_test()
        ts = self.task_session
        ts.verified = True
        ts.task_manager.is_my_task.return_value = False

        ts._react_to_want_to_compute_task(wtct)

        sent_msg = ts.conn.send_message.call_args[0][0]
        ts.task_server.remove_task_session.assert_called()
        self.assertIsInstance(sent_msg, message.tasks.CannotAssignTask)
        self.assertEqual(sent_msg.reason,
                         message.tasks.CannotAssignTask.REASON.NotMyTask)

    def _prepare_handshake_test(self):
        ts = self.task_session.task_server
        tm = self.task_session.task_manager

        tm.is_my_task = Mock()
        tm.is_my_task.return_value = True

        tm.is_my_task = Mock()
        tm.is_my_task.return_value = True

        tm.should_wait_for_node = Mock()
        tm.should_wait_for_node.return_value = False

        ts.should_accept_provider = Mock()
        ts.should_accept_provider.return_value = True

        tm.check_next_subtask = Mock()
        tm.check_next_subtask.return_value = True


class WaitingForResultsTestCase(
        testutils.DatabaseFixture,
        testutils.TempDirFixture,
):
    def setUp(self):
        testutils.DatabaseFixture.setUp(self)
        testutils.TempDirFixture.setUp(self)
        history.MessageHistoryService()
        self.ts = TaskSession(Mock())
        self.ts.conn.send_message.side_effect = \
            lambda msg: msg._fake_sign()
        self.ts.task_server.get_node_name.return_value = "Zażółć gęślą jaźń"
        requestor_keys = KeysAuth(
            datadir=self.path,
            difficulty=4,
            private_key_name='prv',
            password='',
        )
        self.ts.task_server.get_key_id.return_value = "key_id"
        self.ts.key_id = requestor_keys.key_id
        self.ts.task_server.get_share_options.return_value = \
            hyperdrive_client.HyperdriveClientOptions('1', 1.0)

        keys_auth = KeysAuth(
            datadir=self.path,
            difficulty=4,
            private_key_name='prv',
            password='',
        )
        self.ts.task_server.keys_auth = keys_auth
        self.ts.concent_service.variant = variables.CONCENT_CHOICES['test']
        ttc_prefix = 'task_to_compute'
        hdr_prefix = f'{ttc_prefix}__want_to_compute_task__task_header'
        self.msg = msg_factories.tasks.WaitingForResultsFactory(
            sign__privkey=requestor_keys.ecc.raw_privkey,
            **{
                f'{ttc_prefix}__sign__privkey': requestor_keys.ecc.raw_privkey,
                f'{ttc_prefix}__requestor_public_key':
                    encode_hex(requestor_keys.ecc.raw_pubkey),
                f'{ttc_prefix}__want_to_compute_task__sign__privkey':
                    keys_auth.ecc.raw_privkey,
                f'{ttc_prefix}__want_to_compute_task__provider_public_key':
                    encode_hex(keys_auth.ecc.raw_pubkey),
                f'{hdr_prefix}__sign__privkey':
                    requestor_keys.ecc.raw_privkey,
                f'{hdr_prefix}__requestor_public_key':
                    encode_hex(requestor_keys.ecc.raw_pubkey),
            },
        )

    def test_task_server_notification(self, *_):
        self.ts._react_to_waiting_for_results(self.msg)
        self.ts.task_server.subtask_waiting.assert_called_once_with(
            task_id=self.msg.task_id,
            subtask_id=self.msg.subtask_id,
        )


class ForceReportComputedTaskTestCase(testutils.DatabaseFixture,
                                      testutils.TempDirFixture):
    def setUp(self):
        testutils.DatabaseFixture.setUp(self)
        testutils.TempDirFixture.setUp(self)
        history.MessageHistoryService()
        self.ts = TaskSession(Mock())
        self.ts.conn.send_message.side_effect = \
            lambda msg: msg._fake_sign()
        self.ts.task_server.get_node_name.return_value = "Zażółć gęślą jaźń"
        self.ts.task_server.get_key_id.return_value = "key_id"
        self.ts.key_id = 'unittest_key_id'
        self.ts.task_server.get_share_options.return_value = \
            hyperdrive_client.HyperdriveClientOptions('1', 1.0)

        keys_auth = KeysAuth(
            datadir=self.path,
            difficulty=4,
            private_key_name='prv',
            password='',
        )
        self.ts.task_server.keys_auth = keys_auth
        self.n = dt_p2p_factory.Node()
        self.task_id = str(uuid.uuid4())
        self.subtask_id = str(uuid.uuid4())
        self.node_id = self.n.key

    def tearDown(self):
        testutils.DatabaseFixture.tearDown(self)
        testutils.TempDirFixture.tearDown(self)
        history.MessageHistoryService.instance = None

    @staticmethod
    def _mock_task_to_compute(task_id, subtask_id, node_id, **kwargs):
        task_to_compute = msg_factories.tasks.TaskToComputeFactory(**kwargs)
        task_to_compute._fake_sign()
        nmsg_dict = dict(
            task=task_id,
            subtask=subtask_id,
            node=node_id,
            msg_date=datetime.datetime.now(),
            msg_cls='TaskToCompute',
            msg_data=pickle.dumps(task_to_compute),
            local_role=model.Actor.Provider,
            remote_role=model.Actor.Requestor,
        )
        service = history.MessageHistoryService.instance
        service.add_sync(nmsg_dict)

    def assert_submit_task_message(self, subtask_id, wtr):
        self.ts.concent_service.submit_task_message.assert_called_once_with(
            subtask_id, ANY)

        msg = self.ts.concent_service.submit_task_message.call_args[0][1]
        self.assertEqual(msg.result_hash, 'sha1:' + wtr.package_sha1)

    def test_send_report_computed_task_concent_no_message(self):
        wtr = factories.taskserver.WaitingTaskResultFactory(owner=self.n)
        self.ts.send_report_computed_task(
            wtr, wtr.owner.pub_addr, wtr.owner.pub_port, self.n)
        self.ts.concent_service.submit.assert_not_called()

    def test_send_report_computed_task_concent_success(self):
        wtr = factories.taskserver.WaitingTaskResultFactory(
            xtask_id=self.task_id, xsubtask_id=self.subtask_id, owner=self.n)
        self._mock_task_to_compute(self.task_id, self.subtask_id,
                                   self.ts.key_id, concent_enabled=True)
        self.ts.send_report_computed_task(
            wtr, wtr.owner.pub_addr, wtr.owner.pub_port, self.n)

        self.assert_submit_task_message(self.subtask_id, wtr)

    def test_send_report_computed_task_concent_success_many_files(self):
        result = []
        for i in range(100, 300, 99):
            p = pathlib.Path(self.tempdir) / str(i)
            with p.open('wb') as f:
                f.write(b'\0' * i * 2 ** 20)
            result.append(str(p))

        wtr = factories.taskserver.WaitingTaskResultFactory(
            xtask_id=self.task_id, xsubtask_id=self.subtask_id, owner=self.n,
            result=result
        )
        self._mock_task_to_compute(self.task_id, self.subtask_id,
                                   self.ts.key_id, concent_enabled=True)

        self.ts.send_report_computed_task(
            wtr, wtr.owner.pub_addr, wtr.owner.pub_port, self.n)

        self.assert_submit_task_message(self.subtask_id, wtr)

    def test_send_report_computed_task_concent_disabled(self):
        wtr = factories.taskserver.WaitingTaskResultFactory(
            task_id=self.task_id, subtask_id=self.subtask_id, owner=self.n)

        self._mock_task_to_compute(
            self.task_id, self.subtask_id, self.node_id, concent_enabled=False)

        self.ts.send_report_computed_task(
            wtr, wtr.owner.pub_addr, wtr.owner.pub_port, self.n)
        self.ts.concent_service.submit.assert_not_called()


class GetTaskMessageTest(TestCase):
    def test_get_task_message(self):
        msg = msg_factories.tasks.TaskToComputeFactory()
        with patch('golem.task.tasksession.history'
                   '.MessageHistoryService.get_sync_as_message',
                   Mock(return_value=msg)):
            msg_historical = get_task_message('TaskToCompute', 'foo', 'bar',
                                              'baz')
            self.assertEqual(msg, msg_historical)

    def test_get_task_message_fail(self):
        with patch('golem.task.tasksession.history'
                   '.MessageHistoryService.get_sync_as_message',
                   Mock(side_effect=history.MessageNotFound())):
            msg = get_task_message('TaskToCompute', 'foo', 'bar', 'baz')
            self.assertIsNone(msg)


class SubtaskResultsAcceptedTest(TestCase):
    def setUp(self):
        self.task_session = TaskSession(Mock())
        self.task_server = Mock()
        self.task_session.conn.server = self.task_server
        self.requestor_keys = cryptography.ECCx(None)
        self.requestor_key_id = encode_hex(self.requestor_keys.raw_pubkey)
        self.provider_keys = cryptography.ECCx(None)
        self.provider_key_id = encode_hex(self.provider_keys.raw_pubkey)

    def test_react_to_subtask_results_accepted(self):
        # given
        rct = msg_factories.tasks.ReportComputedTaskFactory(
            task_to_compute__sign__privkey=self.requestor_keys.raw_privkey,
            task_to_compute__requestor_public_key=self.requestor_key_id,
            task_to_compute__want_to_compute_task__sign__privkey=(
                self.provider_keys.raw_privkey),
            task_to_compute__want_to_compute_task__provider_public_key=(
                self.provider_key_id),
        )
        sra = msg_factories.tasks.SubtaskResultsAcceptedFactory(
            sign__privkey=self.requestor_keys.raw_privkey,
            report_computed_task=rct,
        )
        self.task_server.keys_auth._private_key = \
            self.provider_keys.raw_privkey
        self.task_server.keys_auth.public_key = \
            self.provider_keys.raw_pubkey
        ctk = self.task_session.task_manager.comp_task_keeper
        ctk.get_node_for_task_id.return_value = self.requestor_key_id
        self.task_session.key_id = self.requestor_key_id
        self.task_server.client.transaction_system.is_income_expected\
                                                  .return_value = False

        dispatch_listener = Mock()
        dispatcher.connect(dispatch_listener, signal='golem.message')

        # when
        self.task_session._react_to_subtask_results_accepted(sra)

        # then
        self.task_server.subtask_accepted.assert_called_once_with(
            self.requestor_key_id,
            sra.subtask_id,
            sra.task_to_compute.requestor_ethereum_address,  # noqa pylint:disable=no-member
            sra.task_to_compute.price,  # noqa pylint:disable=no-member
            sra.payment_ts,
        )
        cancel = self.task_session.concent_service.cancel_task_message
        cancel.assert_called_once_with(
            sra.subtask_id,
            'ForceSubtaskResults',
        )

        dispatch_listener.assert_called_once_with(
            event='received',
            signal='golem.message',
            message=sra,
            sender=ANY,
        )

    def test_react_with_wrong_key(self):
        # given
        key_id = "CDEF"
        sra = msg_factories.tasks.SubtaskResultsAcceptedFactory()
        ctk = self.task_session.task_manager.comp_task_keeper
        ctk.get_node_for_task_id.return_value = "ABC"
        self.task_session.key_id = key_id

        # when
        self.task_session._react_to_subtask_results_accepted(sra)

        # then
        self.task_server.subtask_accepted.assert_not_called()

    def test_result_received(self):
        self.task_server.keys_auth._private_key = \
            self.requestor_keys.raw_privkey
        self.task_server.keys_auth.public_key = \
            self.requestor_keys.raw_pubkey
        self.task_server.accept_result.return_value = 11111

        def computed_task_received(*args):
            args[2]()

        self.task_session.task_manager.computed_task_received = \
            computed_task_received

        rct = msg_factories.tasks.ReportComputedTaskFactory()
        ttc = rct.task_to_compute
        ttc.sign_message(private_key=self.requestor_keys.raw_privkey)

        self.task_session.send = Mock()

        history_dict = {
            'TaskToCompute': ttc,
            'ReportComputedTask': rct,
        }
        with patch('golem.task.tasksession.get_task_message',
                   side_effect=lambda **kwargs:
                   history_dict[kwargs['message_class_name']]):
            self.task_session.result_received(
                ttc.compute_task_def.get('subtask_id'),  # noqa pylint:disable=no-member
                pickle.dumps({'stdout': 'xyz'}),
            )

        assert self.task_session.send.called
        sra = self.task_session.send.call_args[0][0]  # noqa pylint:disable=unsubscriptable-object
        self.assertIsInstance(sra.task_to_compute, message.tasks.TaskToCompute)
        self.assertIsInstance(sra.report_computed_task,
                              message.tasks.ReportComputedTask)
        self.assertTrue(sra.task_to_compute.sig)
        self.assertTrue(
            sra.task_to_compute.verify_signature(
                self.requestor_keys.raw_pubkey
            )
        )


class ReportComputedTaskTest(
        ConcentMessageMixin,
        LogTestCase,
        testutils.TempDirFixture,
):

    @staticmethod
    def _create_pull_package(result):
        def pull_package(*_, **kwargs):
            success = kwargs.get('success')
            error = kwargs.get('error')
            if result:
                success(Mock())
            else:
                error(Exception('Pull failed'))

        return pull_package

    def setUp(self):
        super().setUp()
        keys_auth = KeysAuth(
            datadir=self.path,
            difficulty=4,
            private_key_name='prv',
            password='',
        )
        self.ecc = keys_auth.ecc
        self.node_id = encode_hex(self.ecc.raw_pubkey)
        self.task_id = idgenerator.generate_id_from_hex(self.node_id)
        self.subtask_id = idgenerator.generate_id_from_hex(self.node_id)

        ts = TaskSession(Mock())
        ts.result_received = Mock()
        ts.key_id = "ABC"
        ts.task_manager.get_node_id_for_subtask.return_value = ts.key_id
        ts.task_manager.subtask2task_mapping = {
            self.subtask_id: self.task_id,
        }
        ts.task_manager.tasks = {
            self.task_id: Mock()
        }
        ts.task_manager.tasks_states = {
            self.task_id: Mock(subtask_states={
                self.subtask_id: Mock(deadline=calendar.timegm(time.gmtime()))
            })
        }
        ts.task_server.task_keeper.task_headers = {}
        ecc = Mock()
        ecc.get_privkey.return_value = os.urandom(32)
        ts.task_server.keys_auth = keys_auth
        self.ts = ts

        gsam = patch('golem.network.concent.helpers.history'
                     '.MessageHistoryService.get_sync_as_message',
                     Mock(side_effect=history.MessageNotFound))
        gsam.start()
        self.addCleanup(gsam.stop)

    def _prepare_report_computed_task(self, **kwargs):
        return msg_factories.tasks.ReportComputedTaskFactory(
            task_to_compute__task_id=self.task_id,
            task_to_compute__subtask_id=self.subtask_id,
            **kwargs,
        )

    def test_result_received(self):
        msg = self._prepare_report_computed_task()
        self.ts.task_manager.task_result_manager.pull_package = \
            self._create_pull_package(True)

        with patch('golem.network.concent.helpers.process_report_computed_task',
                   return_value=message.tasks.AckReportComputedTask()):
            self.ts._react_to_report_computed_task(msg)
        self.assertTrue(self.ts.task_server.verify_results.called)

        cancel = self.ts.concent_service.cancel_task_message
        self.assert_concent_cancel(
            cancel.call_args[0], self.subtask_id, 'ForceGetTaskResult')

    def test_reject_result_pull_failed_no_concent(self):
        msg = self._prepare_report_computed_task(
            task_to_compute__concent_enabled=False)

        with patch('golem.network.concent.helpers.history.add'):
            self.ts.task_manager.task_result_manager.pull_package = \
                self._create_pull_package(False)

        with patch('golem.task.tasksession.get_task_message', return_value=msg):
            with patch('golem.network.concent.helpers.'
                       'process_report_computed_task',
                       return_value=message.tasks.AckReportComputedTask()):
                self.ts._react_to_report_computed_task(msg)
        assert self.ts.task_server.reject_result.called
        assert self.ts.task_manager.task_computation_failure.called

    def test_reject_result_pull_failed_with_concent(self):
        msg = self._prepare_report_computed_task(
            task_to_compute__concent_enabled=True)

        self.ts.task_manager.task_result_manager.pull_package = \
            self._create_pull_package(False)

        with patch('golem.network.concent.helpers.process_report_computed_task',
                   return_value=message.tasks.AckReportComputedTask()):
            self.ts._react_to_report_computed_task(msg)
        stm = self.ts.concent_service.submit_task_message
        self.assertEqual(stm.call_count, 2)

        self.assert_concent_submit(stm.call_args_list[0][0], self.subtask_id,
                                   message.concents.ForceGetTaskResult)
        self.assert_concent_submit(stm.call_args_list[1][0], self.subtask_id,
                                   message.concents.ForceGetTaskResult)

        # ensure the first call is delayed
        self.assertGreater(stm.call_args_list[0][0][2], datetime.timedelta(0))
        # ensure the second one is not
        self.assertEqual(len(stm.call_args_list[1][0]), 2)
