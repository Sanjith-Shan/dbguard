# dbguard-node: MySQL 8.4 (Oracle Linux 9) + python3.12 + the dbguard package.
# Build from the repo root:  docker build -f deploy/Dockerfile -t dbguard-node .
FROM mysql:8.4

ARG TARGETARCH
ARG TINI_VERSION=v0.19.0

# OS packages. The OL9 appstream metadata is large (~250 MB, slow to fetch), so this layer
# comes first and keeps the dnf cache in a BuildKit cache mount. tini is not in the OL9
# repos (it is in EPEL), so the static release binary is fetched by TARGETARCH instead.
RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    microdnf -y --setopt=install_weak_deps=0 --setopt=keepcache=1 \
        --disablerepo='*' --enablerepo=ol9_baseos_latest --enablerepo=ol9_appstream \
        install python3.12 python3.12-pip iproute iproute-tc iptables-nft procps-ng \
 && curl -fsSL -o /usr/bin/tini \
        "https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static-${TARGETARCH}" \
 && chmod +x /usr/bin/tini \
 && ln -sf /usr/bin/python3.12 /usr/local/bin/python3 \
 && python3.12 -m venv /opt/dbguard \
 && tini --version && tc -V && iptables -V && ps --version

ENV PATH=/opt/dbguard/bin:$PATH

# Python deps layer: only pyproject changes invalidate it.
COPY pyproject.toml /src/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/pip \
    python3.12 -c "import tomllib;d=tomllib.load(open('/src/pyproject.toml','rb'));print('\n'.join(d['project']['dependencies']+['PyMySQL>=1.1']))" > /tmp/req.txt \
 && pip install -r /tmp/req.txt

# Source layer.
COPY dbguard /src/dbguard
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-deps /src

COPY deploy/mysql/my.cnf /etc/mysql/conf.d/dbguard-base.cnf
COPY deploy/mysql/init/ /docker-entrypoint-initdb.d/
COPY deploy/mysql/dbguard-initdb.sh /usr/local/bin/dbguard-initdb.sh
COPY deploy/entrypoint.sh /entrypoint.sh

ENV DBGUARD_SEMISYNC=1 \
    MYSQL_ROOT_PASSWORD=root
EXPOSE 3306 8080
ENTRYPOINT ["tini", "--", "/entrypoint.sh"]
