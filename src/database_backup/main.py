#!/usr/bin/env python3
import shlex
import sqlite3
from pathlib import Path

import yaml
from database_backup import database_backup_logger

#!/usr/bin/env python3
import argparse
import datetime
import logging
import os
import socket
import subprocess
import sys

import humanfriendly
# !/usr/bin/env python3
import argparse
import datetime
import logging
import os
import shlex
import socket
import sqlite3
import subprocess
import sys
from pathlib import Path

import humanfriendly
import yaml

from database_backup import database_backup_logger

"""Backup postgres databases based on config file"""


IONICE = '/usr/bin/ionice'
PG_DUMP = '/usr/bin/pg_dump'

class Backup:
    """A single backup, from config file, put last backup from state file"""

    def __init__(self, name,config,last:datetime.datetime):
        self.name = name
        self.human_interval = config['interval']
        self.interval = datetime.timedelta(seconds=humanfriendly.parse_timespan(self.human_interval))
        self.human_retain = config['retain']
        self.retain = datetime.timedelta(seconds=humanfriendly.parse_timespan(self.human_retain))
        self.database = config['database']
        self.schemas = []
        self.schemas = config.get('schemas',[])
        self.schemaonly = bool(config.get('schema only', False))
        self.last = last

    @property
    def next_backup(self) -> datetime.datetime:
        """time of next backup"""
        return self.last + self.interval

    def __str__(self):
        return f"{self.database} {self.name} every {self.human_interval} next {self.next_backup}"


class Manager:

    def __init__(self, config):
        # noinspection PyTypeChecker
        self.config = config
        self.dry_run = False
        self.demands = []



    def __enter__(self):
        host_backup = socket.gethostname()
        top = self.config['servers'][host_backup]
        self.location = Path(top['location'])
        if not self.location.is_dir():
            raise FileNotFoundError('{} is not a directory'.format(self.location))
        self.user = top['account']
        self.server = top['server']
        self.port = int(top.get('port', 5432))
        statfile = self.location / top['state data']
        self.state_db = sqlite3.connect(statfile)
        self.state_db.autocommit = True
        self.state_db.execute("""CREATE TABLE IF NOT EXISTS backups (
                label TEXT PRIMARY KEY,
                last_backup TEXT)""")
        ours  = top['set']
        our_set = self.config['sets'][ours]
        self.backups = []
        for key, data in our_set.items():
            row = self.state_db.execute("SELECT last_backup FROM backups WHERE label=?", (key,)).fetchone()
            if row:
                last_bu = datetime.datetime.fromisoformat(row[0])
            else:
                last_bu = datetime.datetime(year=1, month=1, day=1)
            bu = Backup(key,data,last_bu)
            self.backups.append(bu)


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.state_db.close()

    @property
    def base_dump(self):
        if self.server == 'peer':
            return ['pg_dump', '--username', self.user]
        else:
            return ['pg_dump', '--host', self.server, '--username', self.user, '--port', str(self.port)]

    def pgdump(self, backup: Backup,now:datetime.datetime)->None:
        """backup database via pg_dump"""
        name = now.strftime(f"{backup.name}-%Y-%m-%d-%H-%M")
        path = os.path.join(self.location, name)
        if self.server == 'peer':
            cmds = [IONICE,'-c','3',PG_DUMP, '--username', self.user, '--dbname', backup.database, '--file', path]
        else:
            cmds = [IONICE,'-c','3',PG_DUMP, '--host', self.server, '--username', self.user, '--port', str(self.port),
                    '--dbname', backup.database, '--file', path]
        for schema in backup.schemas:
            cmds.append('--schema')
            cmds.append(schema)
        if backup.schemaonly:
            cmds.append('--schema-only')

        database_backup_logger.info(shlex.join(cmds))

        if not self.dry_run:
            subprocess.run(cmds, check=True)
        else:
            print(shlex.join(cmds))

        backup.last = now
        values = (backup.name, now.isoformat())
        database_backup_logger.debug(values)

        self.state_db.execute("INSERT INTO backups(label,last_backup) VALUES(?,?) "
            "ON CONFLICT(label) DO UPDATE SET last_backup=excluded.last_backup",values)

    def dev_copy(self,name):
        spec = self.config[f'devcopy {name}']
        schemas = [s.strip( ) for s in spec['schemas'].split(',')]
        database_backup_logger.info(f"backing up {name} {','.join(schemas)}")

        cmds = self.base_dump + ['--dbname', name, '--file', f'{name}.sql']
        for schema in schemas:
            cmds.extend(('--schema',schema))
        subprocess.run(cmds, check=True)

    def clean(self, backup: Backup):
        """remove expired backups"""
        cuttime = datetime.datetime.now() - backup.retain
        database_backup_logger.debug(f"looking for {backup.name} files older than {cuttime} in {self.location}")
        for de_ in os.scandir(self.location):
            # noinspection PyTypeChecker
            de: os.DirEntry = de_
            if de.name.startswith(backup.name):
                mtime = datetime.datetime.fromtimestamp(de.stat().st_mtime)
                if mtime < cuttime:
                    database_backup_logger.info("deleting {}".format(de.name))
                    os.remove(de.path)
                else:
                    database_backup_logger.debug("{} mtime {} >= cuttime {}".format(de.name, mtime, cuttime))
            else:
                database_backup_logger.debug("Name {} doesn't startwith {}".format(de.name, backup.name))

    def backup(self):
        """Do backups that are currently due"""
        now = datetime.datetime.now(datetime.timezone.utc)
        demand_backup = len(self.demands) > 0
        if demand_backup:
            names = set(b.name for b in self.backups)
            for d in self.demands:
                if not d in names:
                    print(f'Invalid demand, no backup {d} on this server',file=sys.stderr)
                    print(f"Available to demand {','.join(names)}")
            for backup in self.backups:
                if backup.name in self.demands:
                    print(f"Demand backup {backup.name}", file=sys.stderr)
                    self.pgdump(backup)
        else:
            for backup in self.backups:
                database_backup_logger.info(backup)
    #            self.clean(backup)
                if backup.next_backup < now:
                    self.pgdump(backup,now)
        if database_backup_logger.isEnabledFor(logging.DEBUG):
            for backup in self.backups:
                database_backup_logger.debug(backup)

def main():
    logging.basicConfig()
    parser = argparse.ArgumentParser()
    parser.add_argument('yaml',help="configuration file")
    parser.add_argument('-l', '--loglevel', default='ACTION', help="Python logging level")
    parser.add_argument('--demand', action='append', default=[],help="Backup only this label, regardless of time stamp")
    parser.add_argument('--dry-run', action='store_true',help="Just print pgdump, but don't execute")
    parser.add_argument('--devcopy',help='Copy data from test database')

    args = parser.parse_args()
    database_backup_logger.setLevel(getattr(logging,args.loglevel))
    with open(args.yaml) as f:
        config = yaml.safe_load(f)
    mgr = Manager(config)
    mgr.dry_run = args.dry_run
    #mgr.testhost = args.testhost
    mgr.demands = args.demand
    with mgr:
        if not args.devcopy:
            mgr.backup()
        else:
            mgr.dev_copy(args.devcopy)


if __name__ == "__main__":
    _DEBUG = True
    if os.getuid() == 0:
        os.setuid(1002)  # service file specifies local db_backup account, uid == 1002 on data12
    main()
