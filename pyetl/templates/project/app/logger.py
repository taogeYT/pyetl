# -*- coding: utf-8 -*-

__all__ = ["module_log", "instance_log"]

import logging
import os
project_path = os.path.dirname(os.path.abspath(__file__))
project = os.path.basename(project_path)
console = logging.StreamHandler()
formatter = logging.Formatter(
    '<%(asctime)s %(name)s %(levelname)s> %(message)s',
    '%Y-%m-%d %H:%M:%S'
)
console.setFormatter(formatter)
_debug = {
    True: logging.DEBUG,
    False: logging.INFO,
    None: logging.INFO
}


def instance_log(instance, debug=None):
    if debug:
        name = "(mode debug) %s.%s" % (instance.__module__,
                                       instance.__class__.__name__)
    else:
        name = "%s.%s" % (instance.__module__,
                          instance.__class__.__name__)
    log = logging.getLogger(name)
    log.addHandler(console)
    log.setLevel(_debug[debug])
    instance.log = log


def module_log(name, debug=None):
    """
    module_log(__file__)
    Args:
        name: __file__ variable
        debug: open debug
    Returns:
        log: instance
    Raises
        keyError: None
    """
    relative_path = name.replace(project_path, '')
    tmp_name = ''.join([project, relative_path]).replace(os.sep, '.')
    module_name = os.path.splitext(os.path.basename(tmp_name))[0]
    if debug:
        name = "[debug on] %s" % module_name
    else:
        name = "%s" % module_name
    log = logging.getLogger(name)
    log.addHandler(console)
    log.setLevel(_debug[debug])
    return log


if __name__ == '__main__':
    import os
    log = module_log(__file__, debug=True)
    log.info('Log module test start')
    log.warn('Log mode warning')
    log.debug('Log module test end')
    # help(module_log)
