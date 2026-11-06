"""Pipelined RESP setup and checks for the A/B runs.

  python3 -I resp_setup.py [--host H] <port> preload <keys> <value_size>
      SET k%020d (loadgen's key format) for keys 0..keys-1, by MSETs of
      1,000 keys, 32 in flight.
  python3 -I resp_setup.py [--host H] <port> hashes <count> <fields> <value_size>
      HSET scan:<i> f0 v ... f<fields-1> v for i in 0..count-1, 16 in flight.
  python3 -I resp_setup.py [--host H] <port> setget-order <batches> <depth>
      SET order:<port> v<i> GET, <depth> pipelined at a time, <batches>
      times. Reply i must be v<i-1>: each write saw the one before it and
      its reply left in order.

Exits 1 if any reply is wrong.
"""
import argparse
import socket
import sys


def enc(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class Conn:
    def __init__(self, host, port):
        self.s = socket.create_connection((host, port))
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def _fill(self):
        chunk = self.s.recv(1 << 20)
        if not chunk:
            raise EOFError("server closed")
        self.buf += chunk

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _bulk(self, n):
        while len(self.buf) < n + 2:
            self._fill()
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def reply(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t in (b"+", b"-"):
            return (t, rest)
        if t == b":":
            return (t, int(rest))
        if t == b"$":
            n = int(rest)
            return (t, None if n < 0 else self._bulk(n))
        if t == b"*":
            n = int(rest)
            return (t, [self.reply() for _ in range(max(n, 0))])
        raise ValueError("bad reply %r" % line)

    def pipeline(self, cmds, window):
        sent = got = 0
        replies = []
        while got < len(cmds):
            batch = []
            while sent < len(cmds) and sent - got < window:
                batch.append(cmds[sent])
                sent += 1
            if batch:
                self.s.sendall(b"".join(batch))
            replies.append(self.reply())
            got += 1
        return replies


def preload(c, count, size):
    value = b"x" * size
    cmds = []
    for start in range(0, count, 1000):
        args = ["MSET"]
        for i in range(start, min(start + 1000, count)):
            args += ["k%020d" % i, value]
        cmds.append(enc(*args))
    bad = [r for r in c.pipeline(cmds, 32) if r != (b"+", b"OK")]
    print("preload %d keys: %d bad replies %s" % (count, len(bad), bad[:2]))
    return not bad


def hashes(c, count, fields, size):
    value = b"v" * size
    cmds = []
    for h in range(count):
        args = ["HSET", "scan:%d" % h]
        for f in range(fields):
            args += ["f%d" % f, value]
        cmds.append(enc(*args))
    bad = [r for r in c.pipeline(cmds, 16) if r != (b":", fields)]
    print("hashes %d x %d fields: %d bad replies %s" % (count, fields, len(bad), bad[:2]))
    return not bad


def setget_order(c, port, batches, depth):
    key = "order:%d" % port
    n = batches * depth
    cmds = [enc("SET", key, "v%d" % i, "GET") for i in range(1, n + 1)]
    bad = 0
    i = 1
    for start in range(0, n, depth):
        c.s.sendall(b"".join(cmds[start:start + depth]))
        for _ in range(depth):
            want = (b"$", None) if i == 1 else (b"$", b"v%d" % (i - 1))
            if c.reply() != want:
                bad += 1
            i += 1
    print("setget-order: %d replies at depth %d, %d out of order or wrong" % (n, depth, bad))
    return bad == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("port", type=int)
    parser.add_argument("mode", choices=("preload", "hashes", "setget-order"))
    parser.add_argument("values", type=int, nargs="+")
    args = parser.parse_args()
    want = {"preload": 2, "hashes": 3, "setget-order": 2}[args.mode]
    if len(args.values) != want:
        parser.error("%s takes %d numbers" % (args.mode, want))
    c = Conn(args.host, args.port)
    if args.mode == "preload":
        ok = preload(c, *args.values)
    elif args.mode == "hashes":
        ok = hashes(c, *args.values)
    else:
        ok = setget_order(c, args.port, *args.values)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
