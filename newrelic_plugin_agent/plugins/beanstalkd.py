"""
Beanstalkd support
Author: prgTW <tomasz.prgtw.wojcik@gmail.com>

"""
import logging

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)


class Beanstalkd(base.SocketStatsPlugin):

    GUID = 'com.meetme.newrelic_beanstalkd_agent'
    DEFAULT_PORT = 11300
    KEYS = [
            'current-jobs-urgent',
            'current-jobs-ready',
            'current-jobs-reserved',
            'current-jobs-delayed',
            'current-jobs-buried',
            'cmd-put',
            'cmd-peek',
            'cmd-peek-ready',
            'cmd-peek-delayed',
            'cmd-peek-buried',
            'cmd-reserve',
            'cmd-use',
            'cmd-watch',
            'cmd-ignore',
            'cmd-delete',
            'cmd-release',
            'cmd-bury',
            'cmd-kick',
            'cmd-stats',
            'cmd-stats-job',
            'cmd-stats-tube',
            'cmd-list-tubes',
            'cmd-list-tube-used',
            'cmd-list-tubes-watched',
            'cmd-pause-tube',
            'job-timeouts',
            'total-jobs',
#           'max-job-size',
            'current-tubes',
            'current-connections',
            'current-producers',
            'current-workers',
            'current-waiting',
            'total-connections',
#           'pid',
#           'version',
            'rusage-utime',
            'rusage-stime',
#           'uptime',
#           'binlog-oldest-index',
#           'binlog-current-index',
#           'binlog-max-size',
            'binlog-records-written',
            'binlog-records-migrated',
#           'id',
#           'hostname'
    ]

    SOCKET_RECV_MAX = 32768

    def add_datapoints(self, stats):
        """Add all of the data points for a node

        :param dict stats: all of the nodes

        """

        self.add_gauge_value('Binlog/Records/Written', 'records', stats['binlog-records-written'])
        self.add_gauge_value('Binlog/Records/Migrated', 'records', stats['binlog-records-migrated'])

        self.add_gauge_value('Current/Jobs/Urgent', 'jobs', stats['current-jobs-urgent'])
        self.add_gauge_value('Current/Jobs/Ready', 'jobs', stats['current-jobs-ready'])
        self.add_gauge_value('Current/Jobs/Reserved', 'jobs', stats['current-jobs-reserved'])
        self.add_gauge_value('Current/Jobs/Delayed', 'jobs', stats['current-jobs-delayed'])
        self.add_gauge_value('Current/Jobs/Buried', 'jobs', stats['current-jobs-buried'])

        self.add_gauge_value('Current/Tubes', 'tubes', stats['current-tubes'])
        self.add_gauge_value('Current/Connections', 'connections', stats['current-connections'])
        self.add_gauge_value('Current/Producers', 'producers', stats['current-producers'])
        self.add_gauge_value('Current/Workers', 'workers', stats['current-workers'])
        self.add_gauge_value('Current/Waiting', 'connections', stats['current-waiting'])

        self.add_gauge_value('Cmd/Put', 'commands', stats['cmd-put'])
        self.add_gauge_value('Cmd/Peek', 'commands', stats['cmd-peek'])
        self.add_gauge_value('Cmd/Peek/Ready', 'commands', stats['cmd-peek-ready'])
        self.add_gauge_value('Cmd/Peek/Delayed', 'commands', stats['cmd-peek-delayed'])
        self.add_gauge_value('Cmd/Peek/Buried', 'commands', stats['cmd-peek-buried'])
        self.add_gauge_value('Cmd/Reserve', 'commands', stats['cmd-reserve'])
        self.add_gauge_value('Cmd/Use', 'commands', stats['cmd-use'])
        self.add_gauge_value('Cmd/Watch', 'commands', stats['cmd-watch'])
        self.add_gauge_value('Cmd/Ignore', 'commands', stats['cmd-ignore'])
        self.add_gauge_value('Cmd/Delete', 'commands', stats['cmd-delete'])
        self.add_gauge_value('Cmd/Release', 'commands', stats['cmd-release'])
        self.add_gauge_value('Cmd/Bury', 'commands', stats['cmd-bury'])
        self.add_gauge_value('Cmd/Kick', 'commands', stats['cmd-kick'])
        self.add_gauge_value('Cmd/Stats', 'commands', stats['cmd-stats'])
        self.add_gauge_value('Cmd/Stats/Job', 'commands', stats['cmd-stats-job'])
        self.add_gauge_value('Cmd/Stats/Tube', 'commands', stats['cmd-stats-tube'])
        self.add_gauge_value('Cmd/List/Tubes', 'commands', stats['cmd-list-tubes'])
        self.add_gauge_value('Cmd/List/TubeUsed', 'commands', stats['cmd-list-tube-used'])
        self.add_gauge_value('Cmd/List/TubesWatched', 'commands', stats['cmd-list-tubes-watched'])
        self.add_gauge_value('Cmd/PauseTube', 'commands', stats['cmd-pause-tube'])

        self.add_gauge_value('Total/Jobs', 'jobs', stats['total-jobs'])
        self.add_gauge_value('Total/Connections', 'connections', stats['total-connections'])

        self.add_gauge_value('Rusage/UTime', '', stats['rusage-utime'])
        self.add_gauge_value('Rusage/STime', '', stats['rusage-stime'])

        self.add_gauge_value('Job/Timeouts', 'times', stats['job-timeouts'])

    def fetch_data(self, connection):
        """Loop in and read in all the data until we have received it all.

        :param  socket connection: The connection

        """
        connection.send("stats\r\n")
        data = super(Beanstalkd, self).fetch_data(connection)
        data_in = []
        for line in data.replace('\r', '').split('\n'):
            if line == '':
                return self.process_data(data_in)
            if line.find(':') != -1:
                data_in.append(line.strip())
        return None

    def process_data(self, data):
        """Loop through all the rows and parse each line, looking to see if it
        is in the data points we would like to process, adding the key => value
        pair to values if it is.

        :param list data: The list of rows
        :returns: dict

        """
        values = dict()
        for row in data:
            parts = row.split(': ')
            if parts[0] in self.KEYS:
                try:
                    values[parts[0]] = int(parts[1])
                except ValueError:
                    try:
                        values[parts[0]] = float(parts[1])
                    except ValueError:
                        LOGGER.warning('Could not parse line: %r', parts)
                        values[parts[0]] = 0

        # Back fill any missed data
        for key in self.KEYS:
            if key not in values:
                LOGGER.warning('Populating missing element: %s', key)
                values[key] = 0

        # Return the values dict
        return values
