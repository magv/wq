import contextlib
import os
import random
import signal
import subprocess
import tempfile
import time
import tomlkit
from subprocess import Popen, check_call, check_output

def file_exists(path, timeout):
    tstart = time.time()
    while True:
        if os.path.exists(path):
            return True
        if time.time() > tstart + timeout:
            return False
        time.sleep(min(timeout/10, 0.1))

@contextlib.contextmanager
def basic_setup(config={}):
    with tempfile.TemporaryDirectory(prefix="wqtest-") as tmpdir:
        port = random.randint(10000, 30000)
        config.setdefault("client", {})
        config.setdefault("server", {})
        config["client"]["server_host"] = "127.0.0.1"
        config["client"]["server_port"] = port
        config["server"]["host"] = "127.0.0.1"
        config["server"]["port"] = port
        config["server"]["database"] = os.path.join(tmpdir, "wqserver.db")
        confpath = os.path.join(tmpdir, "wq.toml")
        with open(confpath, "w") as f:
            tomlkit.dump(config, f)
        yield {"tmpdir": tmpdir, "wq": ["wq", "-F", confpath]}

def test_basic():
    TO = 10
    with basic_setup() as cfg:
        PA = dict(env={**os.environ, **{"TMPDIR": cfg["tmpdir"]}})
        CA = dict(timeout=TO, **PA)
        ps = Popen(cfg["wq"] + ["serve"], **PA)
        pw = Popen(cfg["wq"] + ["work"], **PA)
        check_call(cfg["wq"] + ["add", "-C", cfg["tmpdir"], "echo ok >1"], **CA)
        assert file_exists(os.path.join(cfg["tmpdir"], "1"), TO)
        pw.terminate(); pw.wait(TO)
        ps.terminate(); ps.wait(TO)

def test_server_restart():
    TO = 10
    with basic_setup() as cfg:
        PA = dict(env={**os.environ, **{"TMPDIR": cfg["tmpdir"]}})
        CA = dict(timeout=TO, **PA)
        ps = Popen(cfg["wq"] + ["serve"], **PA)
        check_call(cfg["wq"] + ["add", "-C", cfg["tmpdir"], "echo ok >1"], **CA)
        check_call(cfg["wq"] + ["add", "-C", cfg["tmpdir"], "echo ok >2"], **CA)
        ps.terminate()
        ps.wait(TO)
        ps = Popen(cfg["wq"] + ["serve"], **PA)
        pw = Popen(cfg["wq"] + ["work"], **PA)
        assert file_exists(os.path.join(cfg["tmpdir"], "1"), TO)
        assert file_exists(os.path.join(cfg["tmpdir"], "2"), TO)
        pw.terminate(); pw.wait(TO)
        ps.terminate(); ps.wait(TO)

def test_server_late():
    TO = 10
    with basic_setup() as cfg:
        PA = dict(env={**os.environ, **{"TMPDIR": cfg["tmpdir"]}})
        pc = Popen(cfg["wq"] + ["add", "-C", cfg["tmpdir"], "echo ok >1"], **PA)
        pw = Popen(cfg["wq"] + ["work"], **PA)
        time.sleep(1)
        ps = Popen(cfg["wq"] + ["serve"], **PA)
        pc.wait(TO)
        assert file_exists(os.path.join(cfg["tmpdir"], "1"), TO)
        pw.terminate(); pw.wait(TO)
        ps.terminate(); ps.wait(TO)

def test_worker_restart():
    TO = 10
    with basic_setup() as cfg:
        PA = dict(env={**os.environ, **{"TMPDIR": cfg["tmpdir"]}})
        CA = dict(timeout=TO, **PA)
        ps = Popen(cfg["wq"] + ["serve"], **PA)
        pw = Popen(cfg["wq"] + ["work"], **PA)
        time.sleep(2)
        pw.terminate(); pw.wait(TO)
        pw = Popen(cfg["wq"] + ["work"], **PA)
        check_call(cfg["wq"] + ["add", "-C", cfg["tmpdir"], "echo ok >1"], **CA)
        assert file_exists(os.path.join(cfg["tmpdir"], "1"), TO)
        pw.terminate(); pw.wait(TO)
        ps.terminate(); ps.wait(TO)

def test_worker_conflict():
    TO = 10
    with basic_setup() as cfg:
        PA = dict(env={**os.environ, **{"TMPDIR": cfg["tmpdir"]}})
        ps = Popen(cfg["wq"] + ["serve"], **PA)
        pw1 = Popen(cfg["wq"] + ["work"], **PA)
        pw2 = Popen(cfg["wq"] + ["work"], **PA)
        time.sleep(2)
        assert pw1.poll() is not None or pw2.poll() is not None

def test_worker_replace():
    TO = 10
    with basic_setup() as cfg:
        PA = dict(env={**os.environ, **{"TMPDIR": cfg["tmpdir"]}})
        CA = dict(timeout=TO, **PA)
        ps = Popen(cfg["wq"] + ["serve"], **PA)
        pw = Popen(cfg["wq"] + ["work"], **PA)
        check_call(cfg["wq"] + ["add", "-C", cfg["tmpdir"], "sleep 8; echo ok >1"], **CA)
        time.sleep(4)
        pw.terminate(); pw.wait(TO)
        pw = Popen(cfg["wq"] + ["work"], **PA)
        assert file_exists(os.path.join(cfg["tmpdir"], "1"), TO)
        pw.terminate(); pw.wait(TO)
        ps.terminate(); ps.wait(TO)

def test_lsw():
    TO = 10
    with basic_setup() as cfg:
        PA = dict(env={**os.environ, **{"TMPDIR": cfg["tmpdir"]}})
        CA = dict(timeout=TO, **PA)
        ps = Popen(cfg["wq"] + ["serve"], **PA)
        o1 = check_output(cfg["wq"] + ["lsw"], **CA)
        pw = Popen(cfg["wq"] + ["work"], **PA)
        time.sleep(1)
        o2 = check_output(cfg["wq"] + ["lsw"], **CA)
        pw.send_signal(signal.SIGINT); pw.wait(TO)
        o3 = check_output(cfg["wq"] + ["lsw"], **CA)
        assert o1 != o2
        assert o2 != o3
        assert o1 == o3
