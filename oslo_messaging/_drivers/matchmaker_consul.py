#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import consulate
import logging
import random

from oslo.config import cfg

from oslo.messaging._drivers import matchmaker as mm

matchmaker_opts = [
    # Matchmaker consul config
    cfg.StrOpt('host',
               default='127.0.0.1',
               help='consul agent host to connect'),
    cfg.IntOpt('port',
               default=8500,
               help='consul port'),
]

CONF = cfg.CONF
CONF.register_opts(matchmaker_opts, 'matchmaker_consul')
LOG = logging.getLogger(__name__)


class ConsulExchange(mm.Exchange):
    """Match Maker which get hosts from consul"""
    def __init__(self, matchmaker):
        self.matchmaker =  matchmaker
        self.consul = matchmaker.consul
        super(ConsulExchange, self).__init__()


class ConsulTopicExchange(ConsulExchange):
    """A Topic Exchange based on consul services
    Exchange where all topic keys are split, sending to second half.
    i.e. "compute.host" sends a message to "compute" running on "host"
    """
    def run(self, topic):
        # Nodes can be empty for a topic
        nodes = [node['Node']['Node'] for node in
                      self.consul.health.service(topic)
                      if not any([ True for check in node['Checks']
                      if check['Status'] != 'passing'])]

        try:
            node = random.choice(nodes)
            LOG.debug("Casting to %s.%s", topic, node)
            return [(topic + '.' + node, node)]
        except:
            LOG.warn(
                    _("No active hosts for the topic '%s'") % (topic, )
                    )
            return []


class ConsulFanoutExchange(ConsulExchange):
    """Fanout Exchange based on consul services.
       Return all nodes for the topic
    """
    def run(self, topic):
        # Assume starts with "fanout~", strip it for lookup.
        topic = topic.split('~', 1)[1]
        nodes = [node['Node']['Node'] for node in 
                    self.consul.health.service(topic) 
                    if not any([ True for check in node['Checks'] 
                    if check['Status'] != 'passing'])]
        LOG.debug(
                _("Fanout for topic %s with nodes %s") % (topic, nodes)
                )
        return nodes


class MatchMakerConsul(mm.MatchMakerBase):
    """Match Maker which load the hosts from consul service and nodes.

      It expect consul services with healthchecks with names mentioned
      below

       nova cert:        cert
       nova scheduler:   scheduler
       nova conductor:   conductor
       nova consoleauth: consoleauth
       nova compute:     compute
    """
    def __init__(self):
        super(MatchMakerConsul, self).__init__()

        self.consul = consulate.Consulate(
                CONF.matchmaker_consul.host,
                CONF.matchmaker_consul.port)

        self.add_binding(mm.FanoutBinding(), ConsulFanoutExchange(self))
        self.add_binding(mm.DirectBinding(), mm.DirectExchange())
        self.add_binding(mm.TopicBinding(), ConsulTopicExchange(self))
