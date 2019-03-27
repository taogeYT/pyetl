import logging

console = logging.StreamHandler()
formatter = logging.Formatter(
    # %(levelname)s %(module)s/%(name)s[line:%(lineno)d]: %(message)s'
    # '%(asctime)s %(levelname)s %(name)s.%(module)s: %(message)s',
    # '<%(asctime)s %(levelname)s %(name)s> : %(message)s'
    '<%(asctime)s %(name)s.%(module)s %(levelname)s> %(message)s',
    '%Y-%m-%d %H:%M:%S'
)
console.setFormatter(formatter)
log = logging.getLogger('pyetl')
log.addHandler(console)
log.setLevel(logging.INFO)


def print(*args, notice=''):
    log.debug('%s\n%s' % (notice, ' '.join(['%s' % i for i in args])))


if __name__ == '__main__':
    log.setLevel(logging.DEBUG)
    print('hello world')
    log.info("log")
    log.info("info")
    log.info("log info")
