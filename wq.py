#!/usr/bin/env python3
"""\
NAME
    wq - a work queue for one person

SYNOPSIS
    wq [-h] [-F config] command args ...

DESCRIPTION

    Wq is a one-person Linux work queue. Like slurm or htcondor,
    but for poor people with no root access.

    To make it work:
    - set up the config file,
    - start a server in a screen/tmux session on some machine,
    - start a worker in a screen/tmux session on each worker machine,
    - submit your jobs.

COMMANDS

    wq add [-h] [-C workdir] [-r resources] [-p priority] [-n name] command
        Submit a command to the server for execution.

    wq ls
        List jobs.

    wq lsw
        List workers.

    wq work
        Start or restart the worker on the current machine.

    wq serve
        Start a server.

OPTIONS
    -h          show a help message and exit
    -F config   use this configuration file
    -C workdir  execute the command in this directory
    -r resources
    -p priority
    -n name

CONFIGURATION

    The configuration is stored in ~/.config/wq.toml, by default it is:

        [client]
        server_url = "http://127.0.0.1:23024"

        [server]
        host = "127.0.0.1"
        port = 23024
        database = "~/.local/share/wqserver.db"

ENVIRONMENT
    TMPDIR      Temporary files will be created here.
"""

import argparse
import asyncio
import contextlib
import datetime
import errno
import json
import os
import pwd
import random
import re
import resource
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import tomllib
import traceback
import typing

import msgspec

import aiohttp
from aiohttp import web

# Common utils

def maybe_shorten(text:str, maxw:int) -> str:
    return text[:maxw-3] + "..." if len(text) > maxw else text

def itos(n:int) -> str:
    C = "bcdfghjklmnpqrstvwxyz"
    V = "aeiou"
    letters = []
    rem = 0
    while n > 0:
        syl = rem + (n % 2205)*991
        n //= 2205
        rem = syl // 2205
        letters.append(C[syl % 21]); syl //= 21
        letters.append(V[syl %  5]); syl //=  5
        letters.append(C[syl % 21])
    return "".join(letters)

def groupby(items:typing.Iterable, key) -> dict:
    key2items:dict = {}
    for item in items:
        k = key(item)
        if k in key2items:
            key2items[k].append(item)
        else:
            key2items[k] = [item]
    return key2items

def set_intersection(*sets) -> list:
    a = set(sets[0])
    for b in sets[1:]:
        a = a.intersection(set(b))
    return sorted(a)

def count(items:typing.Iterable) -> int:
    return sum(1 for item in items if item)

def print_table(rows, indent="", separator="  "):
    if len(rows) == 0: return
    rows = [[str(cell) for cell in row] for row in rows]
    colw = [0] * max(len(row) for row in rows)
    for row in rows:
        for i, cell in enumerate(row):
            colw[i] = max(colw[i], len(cell))
    for row in rows:
        if len(row) == 0:
            print(indent + separator.join("-"*w for w in colw))
        else:
            print(indent + separator.join(cell.ljust(colw[i])
                                          for i, cell in enumerate(row)).rstrip())

def logf(message:str):
    utcdate = datetime.datetime.now(datetime.timezone.utc)
    print(utcdate.astimezone().strftime("%Y-%m-%d %H:%M:%S.%f"), message, file=sys.stderr)

def json_encode(val) -> str:
    return msgspec.json.encode(val).decode("utf8")

def json_decoder(typ:type):
    return msgspec.json.Decoder(type=typ).decode

# SQL (but not really) key-value database

class KVDBTable:
    def __init__(self, table:str, typ:type, db:"KVDB"):
        self.table = table
        self.typ = typ
        self.decode = json_decoder(typ)
        self.cur = db._cur
    def __setitem__(self, key:str, value:object):
        args = (key, json_encode(value))
        self.cur.execute(f"REPLACE INTO {self.table} (key, value) VALUES (?, ?)", args)
    def __getitem__(self, key:str):
        self.cur.execute(f"SELECT value FROM {self.table} WHERE key=?", (key,))
        row = self.cur.fetchone()
        if not row: raise KeyError(key)
        try:
            return self.decode(row[0])
        except ValueError as e:
            raise ValueError(f"while decoding {row[0]}: {e}")
    def __contains__(self, key:str):
        self.cur.execute(f"SELECT key FROM {self.table} WHERE key=?", (key,))
        row = self.cur.fetchone()
        return row is not None
    def __len__(self):
        self.cur.execute(f"SELECT count(*) FROM {self.table}")
        row = self.cur.fetchone()
        return row[0]
    def __delitem__(self, key:str):
        self.cur.execute(f"DELETE FROM {self.table} WHERE key=?", (key,))
        if not self.cur.rowcount: raise KeyError(key)
    def __iter__(self):
        self.cur.execute(f"SELECT key FROM {self.table}")
        for row in self.cur: yield row[0]
    def keys(self):
        self.cur.execute(f"SELECT key FROM {self.table}")
        for row in self.cur: yield row[0]
    def items(self):
        self.cur.execute(f"SELECT key, value FROM {self.table}")
        for row in self.cur: yield (row[0], self.decode(row[1]))
    def values(self):
        self.cur.execute(f"SELECT value FROM {self.table}")
        for row in self.cur: yield self.decode(row[0])

class KVDB:
    def __init__(self, path:str, types:dict[str,type]):
        self._db = sqlite3.connect(path)
        self._db.executescript("PRAGMA journal_mode = wal;")
        self._db.executescript("PRAGMA synchronous = off;")
        self.tables = types
        for name in types.keys():
            self._db.executescript(f"""
CREATE TABLE IF NOT EXISTS {name} (
    key TEXT UNIQUE NOT NULL,
    value TEXT NOT NULL
);""")
        self._db.commit()
        self._db.autocommit = False
        self._cur = self._db.cursor()
    def sync(self):
        self._db.commit()
    def table(self, name:str):
        return KVDBTable(name, self.tables[name], self)

# Server Database

class Worker(msgspec.Struct):
    id: str
    node: str
    resources: dict[str, list[tuple[int, str, list[str]]]]
    taken_jobs: list[str]
    system: dict[str, int|float|str]
    stats: dict[str, int|float]
    first_seen: float
    last_seen: float

class JobStatus_Common(msgspec.Struct,
                       tag_field="status",
                       tag=lambda n: n.removeprefix("JobStatus_").lower()):
    date: float
class JobStatus_Added(JobStatus_Common): pass
class JobStatus_Taken(JobStatus_Common): worker_id: str
class JobStatus_Started(JobStatus_Common): worker_id: str; res: object
class JobStatus_Reassigned(JobStatus_Common): worker_id: str
class JobStatus_Stopped(JobStatus_Common): worker_id: str; reason: str
class JobStatus_Done(JobStatus_Common): worker_id: str; exit_code: int
JobStatus = (
    JobStatus_Added | JobStatus_Taken | JobStatus_Started |
    JobStatus_Reassigned | JobStatus_Stopped | JobStatus_Done
)

class Job(msgspec.Struct):
    id: str
    name: str|None
    command: str
    environment: dict[str, str]
    workdir: str
    priority: int
    resources: dict[str, tuple[int, str, list[str]]]
    history: list[JobStatus]

def webapi(fn):
    async def handler(request):
        try:
            body = await request.json(loads=decoder)
        except msgspec.ValidationError as e:
            return web.HTTPBadRequest(reason=str(e))
        data = await fn(**body)
        return web.json_response(data, dumps=json_encode)
    decoder = json_decoder(typing.TypedDict("ARGS", typing.get_type_hints(fn)))
    return handler

def has_enough_resources(available:dict[str,list[tuple[int,str,list[str]]]], requested:dict[str,tuple[int,str,list[str]]]):
    for system, (ramount, runit, rtags) in requested.items():
        if system not in available: return False
        for aamount, aunit, atags in available[system]:
            if aunit == runit and all(f in atags for f in rtags) and aamount >= ramount:
                break
        else:
            return False
    return True

class Server:
    worker_index: int
    job_index: int
    workers: dict[str, Worker]
    inactive_workers: dict[str, Worker]
    jobs: dict[str, Job]
    node2worker: dict[str, str]
    kvdb: KVDB
    def __init__(self, config:dict):#db_path m_period, m_command):
        db_path = config["database"]
        logf(f"Opening database {db_path!r}")
        self.kvdb = KVDB(db_path, {
            "jobs": Job,
            "workers": Worker,
            "inactive_workers": Worker,
            "node2worker": str
        })
        self.jobs = self.kvdb.table("jobs")
        self.workers = self.kvdb.table("workers")
        self.inactive_workers = self.kvdb.table("inactive_workers")
        self.node2worker = self.kvdb.table("node2worker")
        self.worker_index = 0
        self.job_index = 0
        logf(f"Got {len(self.jobs)} jobs, "
            f"{len(self.workers)} active workers, "
            f"{len(self.inactive_workers)} inactive workers")

    def sync(self):
        self.kvdb.sync()

    # Server browser interface

    async def api_index(self, req):
        return web.Response(text=f"Ugh.")

    # Server API

    async def api_post_job(self, name:str|None, command:str, resources:dict[str,tuple[int,str,list[str]]], priority:int, workdir:str, environment:dict[str,str]):
        while True:
            self.job_index += 1
            id = itos(self.job_index)
            if id not in self.jobs: break
        job = Job(id=id, name=name, command=command, workdir=workdir, environment=environment, resources=resources, priority=priority, history=[JobStatus_Added(date=time.time())])
        self.jobs[job.id] = job
        self.sync()
        logf(f"New job {job.id!r} {resources}: {maybe_shorten(command, 40)!r}")
        return {"job_id": job.id}

    async def api_register_worker(self, node:str, resources:dict[str,list[tuple[int,str,list[str]]]], system:dict[str,int|float|str], stats:dict[str,int|float]):
        if node in self.node2worker:
            worker_id = self.node2worker[node]
            worker = self.workers[worker_id]
            logf(f"Worker from node {node!r} ({system}) wants to register, but {worker.id!r} is already there")
            return {"worker_id": None, "past_worker": worker}
        while True:
            self.worker_index += 1
            id = itos(self.worker_index)
            if id not in self.workers and id not in self.inactive_workers: break
        now = time.time()
        worker = Worker(id=id,
                    node=node,
                    resources=resources,
                    system=system,
                    stats=stats,
                    first_seen=now,
                    last_seen=now,
                    taken_jobs=[])
        self.workers[worker.id] = worker
        self.node2worker[worker.node] = worker.id
        self.sync()
        logf(f"Worker {worker.id!r} (node {node!r}) started on {system!r}")
        return {"worker_id": worker.id}

    async def api_replace_worker(self, worker_id:str, resources:dict[str,list[tuple[int,str,list[str]]]], system:dict[str,int|float|str], stats:dict[str,int|float]):
        if worker_id not in self.workers:
            raise web.HTTPBadRequest(reason=f"bad worker id")
        past_worker = self.workers[worker_id]
        # Create new worker
        while True:
            self.worker_index += 1
            id = itos(self.worker_index)
            if id not in self.workers and id not in self.inactive_workers: break
        now = time.time()
        worker = Worker(id=id,
                    node=past_worker.node,
                    resources=resources,
                    system=system,
                    stats=stats,
                    first_seen=now,
                    last_seen=now,
                    taken_jobs=[])
        # Reassign past jobs
        for jobid in past_worker.taken_jobs:
            job = self.jobs[jobid]
            job.history.append(JobStatus_Reassigned(date=now, worker_id=worker.id))
            self.jobs[job.id] = job
            worker.taken_jobs.append(job.id)
        past_worker.taken_jobs = []
        # Deactivate past worker
        del self.workers[worker_id]
        self.inactive_workers[worker_id] = past_worker
        # Record new worker
        self.workers[worker.id] = worker
        self.node2worker[worker.node] = worker.id
        self.sync()
        logf(f"Worker {worker.id!r} (node {worker.node!r}) took over from {past_worker.id!r} on {system!r}")
        return {"worker_id": worker.id, "job_ids": worker.taken_jobs}

    async def api_update_worker(self, worker_id:str, stats:dict[str,int|float]):
        if worker_id not in self.workers:
            raise web.HTTPBadRequest(reason=f"bad worker id")
        worker = self.workers[worker_id]
        worker.last_seen = time.time()
        worker.stats = stats
        self.workers[worker.id] = worker
        self.sync()
        return {}

    async def api_unregister_worker(self, worker_id:str):
        if worker_id not in self.workers:
            raise web.HTTPBadRequest(reason=f"bad worker id")
        worker = self.workers[worker_id]
        if not worker.taken_jobs:
            self.inactive_workers[worker_id] = worker
            del self.workers[worker_id]
            del self.node2worker[worker.node]
            self.sync()
            logf(f"Worker {worker_id!r} has exited")
        else:
            logf(f"Worker {worker_id!r} has exited, but still has jobs")
        return {}

    async def api_take_job(self, worker_id:str, resources:dict[str,list[tuple[int,str,list[str]]]], stats:dict[str,int|float]):
        if worker_id not in self.workers:
            raise web.HTTPBadRequest(reason=f"bad worker id")
        now = time.time()
        worker = self.workers[worker_id]
        worker.last_seen = now
        worker.stats = stats
        self.workers[worker.id] = worker
        self.sync()
        for repeat in range(2):
            for job in self.jobs.values():
                if not isinstance(job.history[-1], JobStatus_Added): continue
                if has_enough_resources(resources, job.resources):
                    job.history.append(JobStatus_Taken(
                        date = time.time(),
                        worker_id = worker.id
                    ))
                    worker.taken_jobs.append(job.id)
                    self.jobs[job.id] = job
                    self.workers[worker.id] = worker
                    self.sync()
                    logf(f"Job {job.id!r} is taken by {worker_id!r}")
                    return {"job": job}
            await asyncio.sleep(1)
        raise web.HTTPRequestTimeout(reason="no jobs")

    async def api_job_started(self, worker_id:str, job_id:str, allocation:object):
        if worker_id not in self.workers:
            raise web.HTTPBadRequest(reason=f"bad worker id")
        worker = self.workers[worker_id]
        if job_id not in self.jobs:
            raise web.HTTPBadRequest(reason="bad job id")
        job = self.jobs[job_id]
        job.history.append(JobStatus_Started(
            date = time.time(),
            worker_id = worker.id,
            res = allocation
        ))
        self.jobs[job.id] = job
        self.sync()
        logf(f"Job {job.id!r} is started by {worker.id!r}, res {allocation}")
        return {}

    async def api_cancel_job(self, worker_id:str, job_id:str, reason:str):
        if worker_id not in self.workers:
            raise web.HTTPBadRequest(reason=f"bad worker id")
        worker = self.workers[worker_id]
        if job_id not in self.jobs:
            raise web.HTTPBadRequest(reason=f"bad job id")
        job = self.jobs[job_id]
        now = time.time()
        job.history.append(JobStatus_Stopped(
            date = now,
            worker_id = worker.id,
            reason = reason
        ))
        worker.last_seen = now
        worker.taken_jobs.remove(job.id)
        self.jobs[job.id] = job
        self.workers[worker.id] = worker
        self.sync()
        logf(f"Job {job.id!r} is cancelled by {worker.id!r} because: {reason!r}")
        return {}

    async def api_finish_job(self, worker_id:str, job_id:str, exit_code:int):
        if worker_id not in self.workers:
            raise web.HTTPBadRequest(reason="bad worker id")
        worker = self.workers[worker_id]
        if job_id not in self.jobs:
            raise web.HTTPBadRequest(reason="bad job id")
        job = self.jobs[job_id]
        now = time.time()
        job.history.append(JobStatus_Done(
            date = now,
            worker_id = worker.id,
            exit_code = exit_code
        ))
        worker.last_seen = now
        worker.taken_jobs.remove(job.id)
        self.jobs[job.id] = job
        self.workers[worker.id] = worker
        self.sync()
        logf(f"Job {job.id!r} is finished by {worker.id!r}, exit code {exit_code}")
        return {}

    async def api_ls(self):
        return {
            "jobs": list(self.jobs.values()),
            "workers": list(self.workers.values())
        }

    async def api_ls_job(self, job_id:str):
        if job_id not in self.jobs:
            raise web.HTTPBadRequest(reason=f"bad job id")
        return self.jobs[job_id]

    async def api_lsw(self):
        return {
            "workers": list(self.workers.values())
        }

async def main_serve(config):
    srv = Server(config["server"])
    app = web.Application()
    app["wq_server"] = srv
    app["wq_config"] = config
    app.add_routes([
        web.get("/",                        srv.api_index),
        web.post("/api/cancel_job",         webapi(srv.api_cancel_job)),
        web.post("/api/finish_job",         webapi(srv.api_finish_job)),
        web.post("/api/job_started",        webapi(srv.api_job_started)),
        web.post("/api/ls",                 webapi(srv.api_ls)),
        web.post("/api/ls_job",             webapi(srv.api_ls_job)),
        web.post("/api/lsw",                webapi(srv.api_lsw)),
        web.post("/api/post_job",           webapi(srv.api_post_job)),
        web.post("/api/register_worker",    webapi(srv.api_register_worker)),
        web.post("/api/replace_worker",     webapi(srv.api_replace_worker)),
        web.post("/api/take_job",           webapi(srv.api_take_job)),
        web.post("/api/unregister_worker",  webapi(srv.api_unregister_worker)),
        web.post("/api/update_worker",      webapi(srv.api_update_worker)),
    ])
    await web._run_app(app, host=config["server"]["host"], port=config["server"]["port"])

# Worker Resources

#class Resources:
#    def allocate(self, spec:dict[str,int]) -> Allocation: pass
#    def free(self, allocation:Allocation) -> Null: pass
#    def restrict(self, allocation, popen_args:dict) -> dict: pass

class ResourceCPU(msgspec.Struct):
    index: int
    core: int
    socket: int
    tags: list[str]
    allowed: bool
    free: bool

def tags_match(tags:list[str], required_tags:list[str]):
    for tag in required_tags:
        if tag not in tags: return False
    return True

class Resources_CPU:
    processors: list[ResourceCPU]
    def __init__(self):
        affinity = os.sched_getaffinity(0)
        with open("/proc/cpuinfo", "r") as f:
            text = f.read()
        processors = []
        for processor_text in text.split("\n\n"):
            data = {}
            for line in processor_text.splitlines():
                k, v = line.split(":", 1)
                data[k.strip()] = v.strip()
            if not data: continue
            index = int(data["processor"])
            core = int(data["core id"])
            socket = int(data["physical id"])
            tags = data["flags"].lower().split()
            vendor = data["vendor_id"].lower()
            if vendor == "authenticamd": vendor = "amd"
            if vendor == "genuineintel": vendor = "intel"
            tags.append(vendor)
            tags = tuple(sorted(set(tags)))
            allowed = index in affinity
            processors.append(ResourceCPU(index, core, socket, tags, allowed, True))
        self.processors = sorted(processors, key=lambda p: p.index)
    def allocate(self, jobid:str, count:int, unit:str, fltr:list[str]) -> list[int]:
        match unit:
            case "cpu":
                alloc = []
                for p in self.processors:
                    if p.allowed and p.free and tags_match(p.tags, fltr):
                        alloc.append(p.index)
                        count -= 1
                        if count <= 0: break
                if count > 0:
                    raise ValueError("Count not find enough CPUs")
                return alloc
            case "core":
                alloc = []
                for cpus in groupby(self.processors, key=lambda p: p.core).values():
                    if all(p.allowed and p.free and tags_match(p.tags, fltr) for p in cpus):
                        alloc.extend(p.index for p in cpus)
                        count -= 1
                        if count <= 0: break
                if count > 0:
                    raise ValueError("Count not find enough cores")
                return alloc
            case "socket":
                alloc = []
                for cpus in groupby(self.processors, key=lambda p: p.socket).values():
                    if all(p.allowed and p.free and tags_match(p.tags, fltr) for p in cpus):
                        alloc.extend(p.index for p in cpus)
                        count -= 1
                        if count <= 0: break
                if count > 0:
                    raise ValueError("Count not find enough sockets")
                return alloc
            case "node":
                if count != 1:
                    raise ValueError("Can allocate only 1 node")
                for p in self.processors:
                    if not (p.allowed and p.free and tags_match(p.tags, fltr)):
                        raise ValueError("Can't allocate every CPUs")
                return [p.index for p in self.processors]
            case _: raise ValueError(f"Unknown CPU unit {unit!r}")
    def reserve(self, allocation:list[int]) -> None:
        for idx in allocation:
            self.processors[idx].free = False
    def free(self, allocation:list[int]) -> None:
        for idx in allocation:
            self.processors[idx].free = True
    def restrict(self, allocation:list[int], popen_args:dict) -> dict:
        popen_args["env"]["WQ_THREADS"] = str(len(allocation))
        popen_args["env"]["WQ_CPU_LIST"] = ",".join(str(idx) for idx in allocation)
        popen_args["env"]["OMP_NUM_THREADS"] = str(len(allocation))
        popen_args["env"]["OMP_PROC_BIND"] = "true"
        return popen_args
    def report(self) -> list[tuple[int, str, list[str]]]:
        units = []
        # List processors per tag set
        for tags, cpus in groupby(self.processors, key=lambda p: p.tags).items():
            c = count(p.allowed and p.free for p in cpus)
            units.append((c, "cpu", tags))
        # List cores per tag set
        cores = groupby(self.processors, key=lambda p: p.core)
        core_tags = {k:tuple(set_intersection(*[p.tags for p in cpus])) for k, cpus in cores.items()}
        for tags, ids in groupby(cores.keys(), lambda p: core_tags[p]).items():
            c = count(all(p.allowed and p.free for p in cores[c]) for c in ids)
            units.append((c, "core", tags))
        # List sockets per tag set
        sockets = groupby(self.processors, key=lambda p: p.socket)
        socket_tags = {k:tuple(set_intersection(*[p.tags for p in cpus])) for k, cpus in sockets.items()}
        for tags, ids in groupby(sockets.keys(), lambda p: socket_tags[p]).items():
            c = count(all(p.allowed and p.free for p in sockets[c]) for c in ids)
            units.append((c, "socket", tags))
        # List nodes per tag set
        node_tags = set_intersection(*[p.tags for p in self.processors])
        c = 1 if all(p.allowed and p.free for p in self.processors) else 0
        units.append((c, "node", list(node_tags)))
        return units

def normalize_user_cpu_spec(value:int, unit:str, fltr:list[str]):
    if unit not in ("", "cpu", "core", "socket", "node"):
        raise ValueError(f"Unknown unit of CPU: {unit!r}")
    return (value, unit or "cpu", fltr)

def normalize_user_mem_spec(value:int, unit:str, fltr:list[str]):
    units = {
        "": 1024**4, "b": 1,
        "kb": 1024, "mb": 1024**2, "gb": 1024**3, "tb": 1024**4,
        "k" : 1024, "m" : 1024**2, "g" : 1024**3, "t" : 1024**4
    }
    scale = units.get(unit.lower(), None)
    if scale is None:
        raise ValueError(f"Unknown unit of memory: {unit!r}")
    return (value*scale, "b", fltr)

def parse_quantity(value:str, units:dict[str, float]) -> float:
    m = re.fullmatch("([0-9.]*) *([a-zA-Z]*)", value)
    if m is None: raise ValueError(value)
    quantity, unit = m.groups()
    if unit not in units: raise ValueError(value)
    return float(quantity)*units[unit]

def parse_bytes(size:str) -> int:
    return int(parse_quantity(size, {
        "": 1, "b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3, "tb": 1024**4
    }))

def parse_seconds(duration:str) -> float:
    return parse_quantity(duration, {
        "": 1, "s": 1, "m": 60, "h": 60*60, "d": 24*60*60, "w": 7*24*60*60
    })

def format_quantity(value:float, units:list[tuple[float, str, float]]) -> str:
    for unit, name, maxval in units:
        v = value/unit
        if v > maxval: continue
        if v < 9.9: return f"{v:.1f} {name}"
        return f"{v:.0f} {name}"
    return f"{v:.0f} {name}"

def format_time(value:float) -> str:
    return format_quantity(value, (
        (1, "s", 90),
        (60, "m", 90),
        (60*60, "h", 48),
        (24*60*60, "d", 14),
        (7*24*60*60, "w", 50),
        (365*24*60*60, "y", 0)
    ))

class Resources_Mem:
    total: int
    available: int
    def __init__(self):
        mi = meminfo()
        self.total = parse_bytes(mi["memavailable"])
        self.available = self.total
    def allocate(self, jobid:str, count:int, unit:str, fltr:str) -> int:
        assert unit == "b"
        return count
    def reserve(self, allocation:int) -> None:
        self.available -= allocation
    def free(self, allocation:int) -> None:
        self.available += allocation
    def restrict(self, allocation, popen_args):
        return popen_args
    def report(self):
        return [(self.available, "b", [])]

class Resources_Disk:
    tmpdir: str
    total: int
    available: int
    def __init__(self, tmpdir):
        output = subprocess.check_output(["df", tmpdir], encoding="utf-8").splitlines()
        self.tmpdir = tmpdir
        self.total = int(output[1].split()[3])*1024
        self.available = self.total
    def allocate(self, jobid:str, count:int, unit:str, fltr:str) -> tuple[int, str]:
        assert unit == "b"
        tmpdir = os.path.join(self.tmpdir, f"wq_{jobid}")
        os.mkdir(tmpdir, mode=0o700)
        return (count, tmpdir)
    def reserve(self, allocation:tuple[int, str]) -> None:
        size, tmpdir = allocation
        self.available -= size
    def free(self, allocation:tuple[int, str]) -> None:
        size, tmpdir = allocation
        shutil.rmtree(tmpdir, ignore_errors=True)
        self.available += size
    def restrict(self, allocation:tuple[int, str], popen_args):
        size, tmpdir = allocation
        popen_args["env"]["TMPDIR"] = tmpdir
        popen_args["env"]["FORMTMP"] = tmpdir
        popen_args["env"]["FORMTMPSORT"] = tmpdir
        return popen_args
    def report(self):
        return [(self.available, "b", [])]

# Worker

def meminfo() -> dict[str, str]:
    info = {}
    with open("/proc/meminfo", "r") as f:
        for line in f:
            k, v = line.lower().split(":", 1)
            info[k.strip()] = v.strip()
    return info

def read_string(path:str, default):
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return default

def system_info() -> dict:
    euid = os.geteuid()
    uname = os.uname()
    pid = os.getpid()
    return {
        "hostname": uname.nodename,
        "username": pwd.getpwuid(euid).pw_name,
        "pid": pid,
        "comm": read_string(f"/proc/{pid}/comm", None),
        "cmdline": read_string(f"/proc/{pid}/cmdline", None),
        "sysname": uname.sysname,
        "release": uname.release,
        "version": uname.version,
        "machine": uname.machine,
        "machine_id": read_string("/etc/machine-id", None),
        "boot_id": read_string("/proc/sys/kernel/random/boot_id", None)
    }

def is_worker_running(sysinfo:dict) -> bool:
    pid = sysinfo["pid"]
    try:
        os.kill(pid, 0)
    except OSError as e:
        if e.errno == errno.ESRCH:
            return False
    comm = read_string(f"/proc/{pid}/comm", None)
    cmdline = read_string(f"/proc/{pid}/cmdline", None)
    return comm == sysinfo["comm"] and cmdline == sysinfo["cmdline"]

def system_stats() -> dict:
    mi = meminfo()
    uptime = read_string("/proc/uptime", "").split()[0]
    return {
        "loadavg": max(os.getloadavg()),
        "memtotal": parse_bytes(mi["memtotal"]),
        "memavailable": parse_bytes(mi["memavailable"]),
        "memfree": parse_bytes(mi["memfree"]),
        "uptime": float(uptime) if uptime else None
    }

def forkoff(preaction, action):
    r, w = os.pipe()
    try:
        child = os.fork()
        if child > 0:
            # Original process, after fork
            os.close(w)
            with os.fdopen(r, "r") as f:
                return f.read()
    except OSError:
        sys.exit(1)
    # Child process
    os.setsid()
    for fd in range(3, resource.getrlimit(resource.RLIMIT_NOFILE)[0]):
        if fd != w:
            try:
                os.close(fd)
            except OSError:
                pass
    devnull_fd = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull_fd, 0)
    os.dup2(devnull_fd, 1)
    os.dup2(devnull_fd, 2)
    os.close(devnull_fd)
    try:
        grandchild = os.fork()
        if grandchild > 0:
            sys.exit(0)
    except OSError:
        sys.exit(1)
    # Grandchild process
    try:
        preaction()
        with os.fdopen(w, "w") as f:
            f.write(str(preaction()))
        action()
        sys.exit(0)
    except Exception:
        sys.exit(1)

class RunningCommand(msgspec.Struct):
    job_id:str
    allocation:object
    pid_path:str
    status_path:str

def start_command(job, resources):
    env = job["environment"].copy()
    env["WQ_JOBID"] = job["id"]
    popen_args = {
        "cwd": job["workdir"],
        "env": env
    }
    allocation = {}
    try:
        for key, rsys in resources.items():
            alloc = rsys.allocate(job["id"], *job["resources"].get(key, (0, None, None)))
            rsys.reserve(alloc)
            popen_args = rsys.restrict(alloc, popen_args)
            allocation[key] = alloc
        tmpdir = popen_args["env"]["TMPDIR"]
        pid_path = os.path.join(tmpdir, job["id"] + ".wqpid")
        alloc_path = os.path.join(tmpdir, job["id"] + ".wqalloc")
        status_path = os.path.join(tmpdir, job["id"] + ".wqstatus")
        script_path = os.path.join(tmpdir, job["id"] + ".sh")
        with open(alloc_path, "wb") as f:
            f.write(msgspec.json.encode(allocation))
        with open(script_path, "w") as f:
            f.write(job["command"])
        popen_args["args"] = ["/bin/sh", script_path]
        proc = subprocess.run(
                ["wq", "runcmd", pid_path, status_path],
                input=msgspec.json.encode(popen_args))
        if proc.returncode != 0:
            raise OSError("Failed to launch command")
        return RunningCommand(job_id=job["id"],
                              allocation=allocation,
                              pid_path=pid_path,
                              status_path=status_path)
    except Exception:
        for key, rsys in resources.items():
            if key in allocation:
                rsys.free(allocation[key])
        raise

def adopt_command(job_id, resources):
    tmpdir = os.environ.get("TMPDIR", "/tmp")
    tmpdir = os.path.join(tmpdir, f"wq_{job_id}")
    pid_path = os.path.join(tmpdir, job_id + ".wqpid")
    alloc_path = os.path.join(tmpdir, job_id + ".wqalloc")
    status_path = os.path.join(tmpdir, job_id + ".wqstatus")
    with open(alloc_path, "rb") as f:
        allocation = msgspec.json.decode(f.read())
    for key, rsys in resources.items():
        rsys.reserve(allocation[key])
    return RunningCommand(job_id=job_id,
                          allocation=allocation,
                          pid_path=pid_path,
                          status_path=status_path)

def finalize_command(command, resources):
    for key, rsys in resources.items():
        rsys.free(command.allocation[key])

RPC_MINSLEEP = 1
RPC_MAXSLEEP = 30

class RPC:
    def __init__(self, config):
        self.server_url = config["client"]["server_url"]
        self.session = aiohttp.ClientSession()
    async def __aenter__(self):
        await self.session.__aenter__()
        return self
    async def __aexit__(self, exc, exc_typ, tb):
        await self.session.__aexit__(exc, exc_typ, tb)
    async def __call__(self, method:str, data:dict={}, repeat=True):
        timeout = RPC_MINSLEEP
        while True:
            try:
                async with self.session.post(f"{self.server_url}/api/{method}", json=data) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    logf(f"RPC {method}() returned code {resp.status}: {resp.reason}")
            except OSError as e:
                logf(f"Failed to complete RPC call: {e}")
            except aiohttp.client_exceptions.ClientError as e:
                logf(f"Failed to complete RPC call: {e}")
            if not repeat: return None
            await asyncio.sleep(timeout*(0.8 + 0.4*random.random()))
            timeout = min(RPC_MAXSLEEP, timeout*2)

async def main_work(config):
    tmpdir = os.environ.get("TMPDIR", "/tmp")
    resources = {
        "cpu": Resources_CPU(),
        "mem": Resources_Mem(),
        "tmp": Resources_Disk(tmpdir)
    }
    commands:list[RunningCommand] = []
    async with RPC(config) as rpc:
        si = system_info()
        reg = await rpc("register_worker", {
            "node": si["machine_id"] or si["hostname"],
            "resources": {k: r.report() for k, r in resources.items()},
            "system": si,
            "stats": system_stats()
        })
        if reg["worker_id"] is None:
            if is_worker_running(reg["past_worker"]["system"]):
                logf(f"Already running as {reg['past_worker']}")
                sys.exit(1)
            else:
                logf(f"Previous worker failed {reg['past_worker']}")
                reg = await rpc("replace_worker", {
                    "worker_id": reg["past_worker"]["id"],
                    "resources": {k: r.report() for k, r in resources.items()},
                    "system": si,
                    "stats": system_stats()
                })
                for job_id in reg["job_ids"]:
                    cmd = adopt_command(job_id, resources)
                    commands.append(cmd)
        try:
            while True:
                # Get some work from the server
                resp = await rpc("take_job", {
                        "worker_id": reg["worker_id"],
                        "resources": {k: r.report() for k, r in resources.items()},
                        "stats": system_stats()
                    }, repeat=False)
                if resp is not None:
                    logf(f"New job (currently have {len(commands)}): {resp}")
                    try:
                        cmd = start_command(resp["job"], resources)
                        commands.append(cmd)
                    except Exception as e:
                        logf(f"Exception while starting: {type(e).__name__}: {e}")
                        logf(traceback.format_exc())
                        await rpc("cancel_job", {
                            "worker_id": reg["worker_id"],
                            "job_id": resp["job"]["id"],
                            "reason": f"Exception while starting: {type(e).__name__}: {e}"
                        })
                    else:
                        await rpc("job_started", {
                            "worker_id": reg["worker_id"],
                            "job_id": cmd.job_id,
                            "allocation": cmd.allocation
                        })
                # Poll running commands
                for i, cmd in reversed(list(enumerate(commands))):
                    if os.path.exists(cmd.status_path):
                        with open(cmd.status_path, "r") as f:
                            rc = int(f.read())
                        logf(f"Command {cmd} is done, exit code: {rc}")
                        await rpc("finish_job", {
                            "worker_id": reg["worker_id"],
                            "job_id": cmd.job_id,
                            "exit_code": rc
                        })
                        finalize_command(cmd, resources)
                        commands.pop(i)
                await asyncio.sleep(0.8 + 0.4*random.random())
        finally:
            await rpc("unregister_worker", {"worker_id": reg["worker_id"]})

def main_runcmd(pid_path:str, status_path:str):
    popen_args = json.load(sys.stdin)
    cpu_list = popen_args["env"].get("WQ_CPU_LIST", None)
    if cpu_list:
        os.sched_setaffinity(0, [int(i) for i in cpu_list.split(",")])
    def daemon_start():
        with open(pid_path, "w") as f:
            f.write(str(os.getpid()))
        return "ok"
    def daemon_work():
        proc = subprocess.Popen(**popen_args)
        rcode = proc.wait()
        status_tmppath = status_path + ".tmp"
        with open(status_tmppath, "w") as f:
            f.write(str(rcode))
        os.rename(status_tmppath, status_path)
    res = forkoff(daemon_start, daemon_work)
    if res != "ok": sys.exit(1)

# Client

def status_line(status):
    match status["status"]:
        case "added": return "added"
        case "taken": return f"taken by {status['worker_id']!r}"
        case "started": return f"started by {status['worker_id']!r}"
        case "reassigned": return f"reassigned to {status['worker_id']!r}"
        case "stopped": return f"stopped by {status['worker_id']!r}"
        case "done": return f"done by {status['worker_id']!r}, exit code {status['exit_code']}"
        case x: return x

async def main_ls_job(config, job_id):
    async with RPC(config) as rpc:
        job = await rpc("ls_job", {"job_id": job_id})
        print("Properties:")
        print_table([
            ["id", repr(job["id"])],
            ["name", repr(job["name"])],
            ["command", repr(job["command"])],
            ["dir", repr(job["workdir"])],
            ["priority", repr(job["priority"])]
        ], indent="  ")
        print()
        print("Required resources:")
        print_table([
            (key, value, unit, " ".join(tags))
            for key, (value, unit, tags) in job["resources"].items()
        ], indent="  ")
        print()
        print("History:")
        table = []
        for s in job["history"]:
            table.append([
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(s["date"])),
                status_line(s)
            ])
        print_table(table, indent="  ")

async def main_ls(config, job_id):
    if job_id is not None: return await main_ls_job(config, job_id)
    async with RPC(config) as rpc:
        o = await rpc("ls")
        jobs = o["jobs"]
        table = [["id", "prio", "w", "s", "when", "dir", "command"], []]
        for j in jobs:
            status = j["history"][-1]
            match status["status"]:
                case "added":
                    W = " "
                    S = "-"
                case "taken" | "started" | "reassigned":
                    W = status["worker_id"]
                    S = ">"
                case "stopped":
                    W = " "
                    S = "!"
                case "done":
                    W = " "
                    S = "∙" if j["history"][-1]["exit_code"] == 0 else "X"
                case _:
                    W = S = "?"
            table.append([
                j["id"],
                j["priority"],
                W,
                S,
                format_time(time.time() - status["date"]) + " ago",
                j["workdir"],
                maybe_shorten(j["command"], 50)
        ])
        print_table(table)

async def main_lsw(config):
    async with RPC(config) as rpc:
        o = await rpc("lsw")
        table = []
        for w in o["workers"]:
            table.append([
                w["id"],
                w["system"]["hostname"],
                format_time(time.time() - w["last_seen"]) + " ago",
                len(w["taken_jobs"]),
                w["stats"]["loadavg"],
            ])
        table.sort(key=lambda row: row[1])
        print_table([["id", "hostname", "last seen", "jobs", "load"], []] + table)

async def main_add(config, name:str|None, script:str, workdir:str, resdefs:list[str], prio:int):
    systems = {
        "cpu": normalize_user_cpu_spec,
        "mem": normalize_user_mem_spec,
        "tmp": normalize_user_mem_spec
    }
    resources = {}
    for spec in resdefs:
        # Syntax: <system>=<number><unit>(/<tag>+<tag>...)??
        m = re.match(r"([^=.+]*)=([0-9]*)([a-zA-Z]*)(?:/(.*))?", spec)
        if m is None:
            raise ValueError(f"Bad resource syntax: {spec}")
        system, val, unit, fltr = m.groups()
        if system not in systems:
            raise ValueError(f"Unknown resource type {system!r}")
        resources[system] = systems[system](int(val), unit, fltr.split("+") if fltr else [])
    resources.setdefault("cpu", (1, "cpu", []))
    resources.setdefault("mem", (1024**3, "b", []))
    resources.setdefault("tmp", (1024**3, "b", []))
    async with RPC(config) as rpc:
        jobdata = await rpc("post_job", {
            "name": name,
            "command": script,
            "workdir": os.path.abspath(workdir),
            "resources": resources,
            "environment": dict(os.environ),
            "priority": prio
        })
        print(jobdata)

def real_main():
    homedir = os.environ["HOME"]
    confdir = os.environ.get("XDG_CONFIG_HOME", os.path.join(homedir, ".config"))
    datadir = os.environ.get("XDG_DATA_HOME", os.path.join(homedir, ".local", "share"))

    parser = argparse.ArgumentParser(prog="wq")
    parser.print_help = lambda: print(__doc__, end="")
    parser.add_argument("-F", "--config", action="store", default=os.path.join(confdir, "wq.toml"), metavar="file")
    parser_cmd = parser.add_subparsers(dest="cmd")
    p = parser_cmd.add_parser("serve")
    p = parser_cmd.add_parser("ls")
    p.add_argument("job_id", action="store", default=None, nargs="?")
    p = parser_cmd.add_parser("lsw")
    p = parser_cmd.add_parser("work")
    p = parser_cmd.add_parser("add")
    p.add_argument("script")
    p.add_argument("-C", "--directory", action="store", default=os.getcwd())
    p.add_argument("-n", "--name", action="store", default=None)
    p.add_argument("-r", "--resources", action="append", default=[])
    p.add_argument("-p", "--priority", action="store", default=0, type=int)
    p = parser_cmd.add_parser("runcmd")
    p.add_argument("pid_path", action="store")
    p.add_argument("status_path", action="store")
    args = parser.parse_args()

    if args.cmd == "runcmd":
        main_runcmd(args.pid_path, args.status_path)
        return

    config = {}
    if os.path.exists(args.config):
        with open(args.config, "rb") as f:
            config = tomllib.load(f)
    config.setdefault("client", {})
    config.setdefault("server", {})
    config.setdefault("worker", {})
    config["client"].setdefault("server_url", "http://127.0.0.1:23024")
    config["server"].setdefault("database", os.path.join(datadir, "wqserver.db"))
    config["server"].setdefault("host", "127.0.0.1")
    config["server"].setdefault("port", 23024)

    match args.cmd:
        case "ls":      asyncio.run(main_ls(config, args.job_id))
        case "lsw":     asyncio.run(main_lsw(config))
        case "serve":   asyncio.run(main_serve(config))
        case "add":     asyncio.run(main_add(config, args.name, args.script, args.directory, args.resources, args.priority))
        case "work":    asyncio.run(main_work(config))

def main():
    try:
        real_main()
    except KeyboardInterrupt:
        sys.exit(1)

if __name__ == "__main__":
    main()
