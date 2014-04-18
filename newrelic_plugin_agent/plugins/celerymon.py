"""
Celerymon Support

Author: prgTW <tomasz.prgtw.wojcik@gmail.com>

"""
import logging
import time

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)

class Celerymon(base.JSONStatsPlugin):

    GUID = 'com.meetme.newrelic_celerymon_agent'

    DEFAULT_PATH = '/api/task'

    last_request = 0

    @property
    def stats_url(self):
        url = super(Celerymon, self).stats_url
        url = url + '?limit=0&since=' + str(self.last_request)
        LOGGER.debug('URL: %s', url)
        return url

    def http_get(self):
        self.last_request = int(time.time())
        return super(Celerymon, self).http_get()

    def add_datapoints(self, tasks):
        """Add all of the data points for a node

        :param dict tasks: Tasks JSON from Celerymon

        """

        counters = {
          'SUCCESS':  0,
          'PENDING':  0,
          'STARTED':  0,
          'FAILURE':  0,
          'RETRY'  :  0,
          'REVOKED':  0,
          'RECEIVED': 0,
        };

        for task in tasks:
            counters[task[1]['state']] += 1

        for counter in counters:
            self.add_gauge_value('Tasks/' + counter.capitalize(), 'tasks', counters.get(counter, 0))

