## What?

Wq is a job scheduler for poor people with no root access.

## Why?

Imagine you’ve joined a research institution, and they tell you
that to run your computations, “*just ssh into some machine, and
run it there*”?

- *“Oh, and make sure to check that no-one else is using that machine.”*
- *“And if someone *is* using it, just visit his office and ask him not to.”*
- *“And don’t use machines 34 and 57, those are reserved for a VIP.”*
- *“No, we don’t need a job scheduling system.”*

Your takeaway? They need a job scheduling system.

The question then is: which is faster, to convince them to set
up [slurm] or [htcondor] for everyone, or to hack around it
yourself? You now must ponder no more, because to hack around
it is as easy as

    python3 -m pip install --user git+https://github.com/magv/wq

[slurm]: https://slurm.schedmd.com/quickstart.html
[htcondor]: https://htcondor.org/htcondor/overview/
