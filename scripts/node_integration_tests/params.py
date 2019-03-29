from copy import copy
import itertools
import os


def _params_from_dict(d: dict) -> list:
    return list(
        itertools.chain.from_iterable(
            [(k, v) if v is not None else (k, ) for k, v in d.items()]
        )
    )


def _debug(args_dict):
    return {
        **args_dict,
        **{'--log-level': 'DEBUG'}
    }


def _concent_disabled(args_dict):
    args_dict = copy(args_dict)
    args_dict['--concent'] = 'disabled'
    return args_dict


def _mainnet(args_dict):
    args_dict = copy(args_dict)
    args_dict['--net'] = 'mainnet'
    return args_dict


PROVIDER_RPC_PORT = os.environ.get('GOLEM_PROVIDER_RPC_PORT', '61001')
REQUESTOR_RPC_PORT = os.environ.get('GOLEM_REQUESTOR_RPC_PORT', '61000')

_PROVIDER_PASSWORD = os.environ.get('GOLEM_PROVIDER_PASSWORD', 'dupa.8')
_REQUESTOR_PASSWORD = os.environ.get('GOLEM_REQUESTOR_PASSWORD', 'dupa.8')

_REQUESTOR_ARGS = {
    '--concent': os.environ.get('GOLEM_CONCENT_VARIANT', 'staging'),
    '--password': _REQUESTOR_PASSWORD,
    '--accept-terms': None,
    '--accept-concent-terms': None,
    '--rpc-address': 'localhost:%s' % REQUESTOR_RPC_PORT,
    '--protocol_id': '1337',
}
REQUESTOR_ARGS = _params_from_dict(_REQUESTOR_ARGS)  # scripts

_PROVIDER_ARGS = {
    '--concent': os.environ.get('GOLEM_CONCENT_VARIANT', 'staging'),
    '--password': _PROVIDER_PASSWORD,
    '--accept-terms': None,
    '--accept-concent-terms': None,
    '--rpc-address': 'localhost:%s' % PROVIDER_RPC_PORT,
    '--protocol_id': '1337',
}
PROVIDER_ARGS = _params_from_dict(_PROVIDER_ARGS)

_PROVIDER_ARGS_DEBUG = _debug(_PROVIDER_ARGS)
PROVIDER_ARGS_DEBUG = _params_from_dict(_PROVIDER_ARGS_DEBUG)
_REQUESTOR_ARGS_DEBUG = _debug(_REQUESTOR_ARGS)
REQUESTOR_ARGS_DEBUG = _params_from_dict(_REQUESTOR_ARGS_DEBUG)

_REQUESTOR_ARGS_NO_CONCENT = _concent_disabled(_REQUESTOR_ARGS_DEBUG)
REQUESTOR_ARGS_NO_CONCENT = _params_from_dict(_REQUESTOR_ARGS_NO_CONCENT)
_PROVIDER_ARGS_NO_CONCENT = _concent_disabled(_PROVIDER_ARGS_DEBUG)
PROVIDER_ARGS_NO_CONCENT = _params_from_dict(_PROVIDER_ARGS_NO_CONCENT)

_REQUESTOR_ARGS_MAINNET = _mainnet(_concent_disabled(_REQUESTOR_ARGS))
REQUESTOR_ARGS_MAINNET = _params_from_dict(_REQUESTOR_ARGS_MAINNET)
_PROVIDER_ARGS_MAINNET = _mainnet(_concent_disabled(_PROVIDER_ARGS))
PROVIDER_ARGS_MAINNET = _params_from_dict(_PROVIDER_ARGS_MAINNET)

_REQUESTOR_ARGS_MAINNET_DEBUG = _debug(_REQUESTOR_ARGS_MAINNET)
REQUESTOR_ARGS_MAINNET_DEBUG = _params_from_dict(_REQUESTOR_ARGS_MAINNET_DEBUG)
_PROVIDER_ARGS_MAINNET_DEBUG = _debug(_PROVIDER_ARGS_MAINNET)
PROVIDER_ARGS_MAINNET_DEBUG = _params_from_dict(_PROVIDER_ARGS_MAINNET_DEBUG)
