## What?

Wq is a Linux job scheduler for poor people with no root access.

## Why?

Imagine you’ve joined a research institution, and they tell you
that to run your computations, “*just ssh into some machine, and
run it there*”?

- *“Oh, and make sure to check that no-one else is using that machine.”*
- *“And if someone *is* using it, just visit his office and ask him not to.”*
- *“And don’t use machines 34 and 57, those are reserved for a VIP.”*
- *“No, we don’t need a job scheduling system.”*

Your takeaway? They need a job scheduling system.

But which is faster, to convince them to set up [slurm] or
[htcondor] for everyone, or to hack around it yourself? Ponder
no more, friends, because now you can just

    python3 -m pip install --user git+https://github.com/magv/wq

[htcondor]: https://htcondor.org/htcondor/overview/
[slurm]: https://slurm.schedmd.com/quickstart.html

## How to setup?

- Make sure you have [python] version 3.10 or newer. Compile it
  by hand, if you must, or use [uv], [pyenv], [spack], etc, if
  that’s easier.

- Make sure your python has a recent version of [pip] installed.
  Upgrade it, if you must:

      python3 -m pip install --upgrade --user pip

- Install wq:

      python3 -m pip install --user git+https://github.com/magv/wq

- Figure out which machine you want to use as a server, and put
  its IP address into the config file at `~/.config/wq.conf`:

      [client]
      server_url = "http://SERVER-IP-HERE:23024"

      [server]
      host = "SERVER-IP-HERE"
      port = 23024

  The port can also be chosen freely.

- Run `wq serve` on the server in a [tmux] or a [screen] session.

- Run `wq work` on each worker machine in a [tmux] or a [screen]
  session.

[pip]: https://pip.pypa.io/
[pyenv]: https://github.com/pyenv/pyenv
[python]: https://www.python.org/
[spack]: https://spack.io/
[tmux]: https://github.com/tmux/tmux/
[uv]: https://github.com/astral-sh/uv
[screen]: https://www.gnu.org/software/screen/

## How to use?

### Submitting jobs

Use `wq submit [-r <resources>] ... <command>` to submit your
jobs.

Each job should ask for resources to be allocated to it:

- To ask for CPUs, there are four options:

  - Ask for logical CPUs (i.e. hyperthreads) using `-r cpu=1`,
    or `-r cpu=1cpu`. This is the most common option.

  - Ask for whole CPU cores using `-r cpu=1core`. This way all
    hyperthreads of a given core will be allocated together
    as a group.

  - Ask for whole CPU sockets (i.e. physical processors) using
    `-r cpu=1socket`.

  - Ask for every CPU in a given node using `-r cpu=1node`.

- To ask for CPUs with specific feature flags, use `-r
  cpu=1/avx2+fma`. The feature flags here are exactly as listed
  in `/proc/cpuinfo`.

- To ask for memory, use `-r mem=1GB`. The recognized units
  here are `B`, `KB`, `MB`, `GB`, and `TB`.

- To ask for temporary disk storage, use `-r tmp=1GB`. The
  storage will be allocated in a temporary folder, and the
  environment variable `TMPDIR` will be set to point to it.
  After the job is finished, this directory will be removed.

### Listing jobs

Use `wq ls` to see jobs.

Use `wq ls <jobid>` to see a particular job.

### Listing workers

Use `wq lsw` to see workers.

## Q&A

### Is it any good?

It’s useful, but very preliminary.
Use at your own risk.

### Is it secure?

Not at the moment.
Right now *everyone* can submit jobs, there is no access control.
Don’t use this in untrusted environments.
