"""The DBGuard manager (``dbguard``), which decides that a primary is dead and drives the failover.

Reading order for the failover story. ``controller`` runs one loop per set over polls from
``probe``. ``detector`` decides DEAD, ``failover`` runs the steps with ``selection`` choosing
the winner, and ``rejoin`` brings the old primary back by repoint or clone. ``switchover`` is
the planned path, ``bootstrap`` finds the primary at startup, ``replacement`` provisions a
new replica, and ``api`` serves it all over HTTP. ``fake`` is the simulated fleet tests use.
"""
