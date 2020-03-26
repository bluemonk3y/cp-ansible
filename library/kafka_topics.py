#!/usr/bin/python
# -*- coding: utf-8 -*-

from ansible.module_utils.basic import *

class TopicDetails:

    def __init__(self, name, partition_count, replication_factor, configs):
        self.name               = name
        self.partition_count    = partition_count
        self.replication_factor = replication_factor
        self.configs            = configs

    def __eq__(self, othr):
        if not isinstance(othr, TopicDetails):
            return False
        self_attributes = (self.name, self.partition_count, self.replication_factor, self.configs)
        othr_attributes = (othr.name, othr.partition_count, othr.replication_factor, othr.configs)
        return self_attributes == othr_attributes

    def __hash__(self):
        return hash((self.name, self.partition_count, self.replication_factor, self.configs))

    def __str__(self):
        return "TopicDetails(%s,%s,%s,%s)" % (self.name, self.partition_count, self.replication_factor, self.configs)

class KafkaTopics:

    def __init__(self, module, command, bootstrap_servers, command_config):
        self.module            = module
        self.command           = command
        self.bootstrap_servers = bootstrap_servers
        self.command_config    = command_config
    
    def describe(self, topic):

        args = list()

        args.append(self.command)

        args.append('--bootstrap-server')
        args.append(self.bootstrap_servers)

        if self.command_config:
            args.append('--command-config')
            args.append(self.command_config)

        args.append('--describe')

        args.append('--topic')
        args.append(topic)

        rc, out, err = self.module.run_command(args)

        if rc!=0:
            raise AssertionError('kafka-topics command failed: ' + (out if out else err))

        result = None
        for line in out.splitlines():

            m = re.match('Topic:(.+)\\s+PartitionCount:(\\d+)\\s+ReplicationFactor:(\\d+)\\s+Configs:(.*)\\s*', line)
            if m:
                name = m.group(1)
                partition_count = int(m.group(2))
                replication_factor = int(m.group(3))
                configs = dict()
                for entry in m.group(4).split(','):
                    k,v = entry.split("=", 2)
                    configs[k] = v
                result = TopicDetails(name, partition_count, replication_factor, configs)
                break

        return result

    def create(self, name, partition_count, replication_factor, configs):

        args = list()

        args.append(self.command)

        args.append('--bootstrap-server')
        args.append(self.bootstrap_servers)

        if self.command_config:
            args.append('--command-config')
            args.append(self.command_config)

        args.append('--create')

        args.append('--topic')
        args.append(name)

        args.append('--partitions')
        args.append(str(partition_count))

        args.append('--replication-factor')
        args.append(str(replication_factor))

        for k,v in configs.iteritems():
            args.append('--config')
            args.append('%s=%s' % (k,v))

        rc, out, err = self.module.run_command(args)

        if rc!=0:
            raise AssertionError('kafka-topics command failed: ' + (out if out else err))

    def delete(self, name):

        args = list()

        args.append(self.command)

        args.append('--bootstrap-server')
        args.append(self.bootstrap_servers)

        if self.command_config:
            args.append('--command-config')
            args.append(self.command_config)

        args.append('--delete')

        args.append('--topic')
        args.append(name)

        rc, out, err = self.module.run_command(args)

        if rc!=0:
            raise AssertionError('kafka-topics command failed: ' + (out if out else err))

    def alter(self, name, partition_count, configs):

        args = list()

        args.append(self.command)

        args.append('--bootstrap-server')
        args.append(self.bootstrap_servers)

        if self.command_config:
            args.append('--command-config')
            args.append(self.command_config)

        args.append('--alter')

        args.append('--topic')
        args.append(name)

        if partition_count is not None:
            args.append('--partitions')
            args.append(str(partition_count))

        for k,v in configs.iteritems():
            args.append('--config')
            args.append('%s=%s' % (k,v))

        rc, out, err = self.module.run_command(args)

        if rc!=0:
            raise AssertionError('kafka-topics command failed: ' + (out if out else err))

def is_subset(new_config, old_config):
    for k,v in new_config.iteritems():
        if k not in old_config:
            return False
        if v != old_config[k]:
            return False
    return True

def main():

    argument_spec = dict(
        bootstrap_servers=dict(type='str', required=True, aliases=['bootstrap_server', 'broker_list']),
        command_config=dict(type='str', default=None),
        name=dict(type='str', required=True, aliases=['topic']),
        replication_factor=dict(type='int', default=1),
        partition_count=dict(type='int', default=1),
        # configs=dict(type='dict', default=dict(), options=dict(second_level=dict(default='',type='str'))),
        configs=dict(type='dict', default=dict()),
        status=dict(type='str', default='present', choices=['present', 'absent']),
        style=dict(type='str', default='confluent', choices=['confluent', 'apache']),
        kafka_home=dict(type='str', default=''),
    )

    module = AnsibleModule(
        argument_spec=argument_spec
    )

    p = module.params

    bootstrap_servers = p.get('bootstrap_servers')
    command_config = p.get('command_config')
    name = p.get('name')
    partition_count = p.get('partition_count')
    replication_factor = p.get('replication_factor')
    configs = p.get('configs')
    status = p.get('status')
    style = p.get('style')
    kafka_home = p.get('kafka_home')

    try:

        # assume we are not on Windows

        # if kafka_home:

        command = 'kafka-topics' if style=='confluent' else 'kafka-topics.sh'

        kafka_topics = KafkaTopics(module, command, bootstrap_servers, command_config)

        if status == 'present':

            if name == '':
                raise AssertionError('Missing topic name')

            if partition_count < 1:
                raise AssertionError('Partition count must be at least 1')

            if replication_factor < 1:
                raise AssertionError('Replication factor must be at least 1')

            topic = kafka_topics.describe(name)
            
            if topic is None:

                kafka_topics.create(name, partition_count, replication_factor, configs)

                module.exit_json(
                    changed=True, 
                    msg="Topic created")

            elif topic.partition_count > partition_count:

                module.fail_json(
                    changed=True, 
                    msg="Cannot decrease partition count")

            elif topic.replication_factor != replication_factor:

                module.fail_json(
                    changed=True, 
                    msg="Cannot change replication factor")

            elif topic.partition_count == partition_count and is_subset(configs, topic.configs):

                module.exit_json(
                    changed=False, 
                    msg="Topic already exists")

            else:

                kafka_topics.alter(name, partition_count, configs)

                module.exit_json(
                    changed=True, 
                    msg="Topic modified")

        elif status == 'absent':

            if name == '':
                raise AssertionError('Missing topic name')

            topic = kafka_topics.describe(name)

            if topic is not None:

                kafka_topics.delete(name)

                module.exit_json(
                    changed=True, 
                    msg="Topic deleted")
            else:

                module.exit_json(
                    changed=False, 
                    msg="Topic does not exist")

        else:

            module.fail_json(
                msg="Invalid target state: %s" % state)

    except AssertionError as e:

        module.fail_json(
            msg=e.message)
            
    except Exception as e:
        module.fail_json(
            msg=e.message)

if __name__ == '__main__':
    main()