"""The DBGuard agent (``dbguard-agent``), one per node, the only process that changes its role.

``core`` holds the logic (fence, promote, repoint, rebuild, /primary, the wake guard and the
self-fence lease), ``http`` the routes, ``db`` the bounded SQL layer, ``supervisor`` the mysqld
child process, and ``settings`` the environment it is configured from.
"""
