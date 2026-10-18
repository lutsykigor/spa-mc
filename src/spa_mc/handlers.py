"""Turning a handler into something a worker interpreter can import, and importing it there.

Workers are separate interpreters: they cannot receive a function object, only find the same function by name.
So a handler must be a module-level function. One defined in the script being run (`__main__`) is loaded in each
worker from the script's file under another module name, as `multiprocessing` does, so the script's
`if __name__ == '__main__':` block does not run again.
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import sys
from collections.abc import Callable

MAIN_ALIAS = '__spa_mc_main__'


def reference(target: str | Callable, role: str = 'handler') -> str:
    """`'module:qualname'` for a module-level function; a string is taken as already being one."""
    if isinstance(target, str):
        return target
    module = getattr(target, '__module__', None)
    qualname = getattr(target, '__qualname__', None)
    if not callable(target) or inspect.ismethod(target) or not module or not qualname or '<' in qualname:
        raise TypeError(
            f'{role} must be a module-level function, because each worker is a separate interpreter that '
            f'imports it by name; lambdas, closures and bound methods cannot be shared: got {target!r}'
        )
    if module == '__main__':
        if main_path() is None:
            raise TypeError(f'{role} {qualname!r} is defined in an interactive session; define it in a file')
        module = MAIN_ALIAS
    return f'{module}:{qualname}'


def main_path() -> str | None:
    return getattr(sys.modules.get('__main__'), '__file__', None)


def resolve(spec: str, main_file: str | None) -> Callable:
    module_name, _, attr = spec.partition(':')
    if module_name == MAIN_ALIAS:
        target = _load_main(main_file)
    else:
        target = importlib.import_module(module_name)
    for part in attr.split('.'):
        target = getattr(target, part)
    return target


def _load_main(main_file: str | None):
    if MAIN_ALIAS in sys.modules:
        return sys.modules[MAIN_ALIAS]
    if main_file is None:
        raise ImportError('the handler lives in the main script, but its file is unknown')
    spec = importlib.util.spec_from_file_location(MAIN_ALIAS, main_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[MAIN_ALIAS] = module
    spec.loader.exec_module(module)
    return module
