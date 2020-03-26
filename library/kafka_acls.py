#!/usr/bin/python
# -*- coding: utf-8 -*-

from ansible.module_utils.basic import *
import itertools

# shotcuts in kafka-acls:
# producer - on topics Describe, Create, Write
# consumer - on topics Describe, Read; on groups Read
# kafka-acls builds cartesian product of all the parameters

class AclEntry:

    def __init__(self, principal, host, operation, permission_type):
        self.principal       = principal
        self.host            = host
        self.operation       = operation
        self.permission_type = permission_type

    def __eq__(self, othr):
        if not isinstance(othr, AclEntry):
            return False
        self_attributes = (self.principal, self.host, self.operation, self.permission_type)
        othr_attributes = (othr.principal, othr.host, othr.operation, othr.permission_type)
        return self_attributes == othr_attributes

    def __hash__(self):
        return hash((self.principal, self.host, self.operation, self.permission_type))

    def __str__(self):
        return "AclEntry(%s,%s,%s,%s)" % (self.principal, self.host, self.operation, self.permission_type)

class AclResource:

    def __init__(self, resource_type, resource_name, pattern_type):
        self.resource_type   = resource_type
        self.resource_name   = resource_name
        self.pattern_type    = pattern_type

    def __eq__(self, othr):
        if not isinstance(othr, AclResource):
            return False
        if self.resource_type != othr.resource_type:
            return False
        if self.resource_name != othr.resource_name:
            return False
        if self.pattern_type != othr.pattern_type:
            return False
        return True

    def __hash__(self):
        return hash((self.resource_type, self.resource_name, self.pattern_type))

    def __str__(self):
        return "AclResource(%s,%s,%s)" % (self.resource_type, self.resource_name, self.pattern_type)

class KafkaAcls:

    def __init__(self, module, command, bootstrap_servers, command_config):
        self.module            = module
        self.command           = command
        self.bootstrap_servers = bootstrap_servers
        self.command_config    = command_config
    
    def list(self, principals, resources, pattern_type):

        args = list()

        args.append(self.command)

        args.append('--bootstrap-server')
        args.append(self.bootstrap_servers)

        if self.command_config:
            args.append('--command-config')
            args.append(self.command_config)

        args.append('--list')

        for principal in principals:
            args.append('--principal')
            args.append(principal)

        for resource_type, resource_name in resources:
            if resource_type == 'cluster':
                args.append('--cluster')
            elif resource_type == 'group':
                args.append('--group')
                args.append(resource_name)
            elif resource_type == 'topic':
                args.append('--topic')
                args.append(resource_name)
            elif resource_type == 'transactional_id':
                args.append('--transactional-id')
                args.append(resource_name)
            elif resource_type == 'delegation_token':
                args.append('--transactional-id')
                args.append(resource_name)
            else:
                raise AssertionError('Unsupported resource type: %s' % resource_type)

        if pattern_type:
            args.append('--resource-pattern-type')
            args.append(pattern_type)

        rc, out, err = self.module.run_command(args)

        if rc!=0:
            raise AssertionError('kafka-acls command failed:\n%s\n%s' % (out, err))

        result = dict()
        for line in out.splitlines():

            # Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=sample, patternType=LITERAL)`: 
            #         (principal=User:consumer, host=*, operation=DESCRIBE, permissionType=ALLOW)
            #         (principal=User:consumer, host=*, operation=READ, permissionType=ALLOW) 

            m = re.match('Current ACLs for resource `ResourcePattern\\(resourceType=(.*), name=(.*), patternType=(.*)\\)`:\\s*', line)
            if m:
                acl_resource = AclResource(m.group(1).lower(), m.group(2), m.group(3).lower())
                acl_entries = result.get(acl_resource, list())
                result[acl_resource] = acl_entries
                continue

            m = re.match('\\s*\\(principal=(.*), host=(.*), operation=(.*), permissionType=(.*)\\)\\s*', line)
            if m:
                acl_principal = m.group(1)
                acl_host = m.group(2)
                # the operations in ACL list are upper-case with underscore as word separator
                # the same operations in command are camel-case in the docs, but effectively
                # case-insensitive
                # https://docs.confluent.io/current/kafka/authorization.html#acl-format
                # the operations:
                # * ALTER_CONFIGS
                # * CLUSTER_ACTION
                # * DESCRIBE_CONFIGS
                # * IDEMPOTENT_WRITE
                acl_operation = m.group(3).lower().replace('_', '')
                acl_permission_type = m.group(4).lower()
                acl_entry = AclEntry(acl_principal, acl_host, acl_operation, acl_permission_type)
                acl_entries.append(acl_entry)
                continue

        return result

    def add(self, principals, hosts, operations, permission_type, resources, pattern_type):

        args = list()

        args.append(self.command)

        args.append('--bootstrap-server')
        args.append(self.bootstrap_servers)

        if self.command_config:
            args.append('--command-config')
            args.append(self.command_config)

        args.append('--add')

        if permission_type == 'allow':
            for principal in principals:
                args.append('--allow-principal')
                args.append(principal)
            for host in hosts:
                args.append('--allow-host')
                args.append(host)
        elif permission_type == 'deny':
            for principal in principals:
                args.append('--deny-principal')
                args.append(principal)
            for host in hosts:
                args.append('--deny-host')
                args.append(host)
        else:
            raise AssertionError('Unsupported permission type: %s' % permission_type)

        for operation in operations:
            args.append('--operation')
            args.append(operation)

        for resource_type, resource_name in resources:
            if resource_type == 'cluster':
                args.append('--cluster')
            elif resource_type == 'group':
                args.append('--group')
                args.append(resource_name)
            elif resource_type == 'topic':
                args.append('--topic')
                args.append(resource_name)
            elif resource_type == 'transactional_id':
                args.append('--transactional-id')
                args.append(resource_name)
            elif resource_type == 'delegation_token':
                args.append('--transactional-id')
                args.append(resource_name)
            else:
                raise AssertionError('Unsupported resource type: %s' % resource_type)

        if pattern_type:
            args.append('--resource-pattern-type')
            args.append(pattern_type)

        rc, out, err = self.module.run_command(args)

        if rc!=0:
            raise AssertionError('kafka-acls command failed:\n%s\n%s' % (out, err))

    def remove(self, principal, host, operation, permission_type, resource_type, resource_name, pattern_type):

        args = list()

        args.append(self.command)

        args.append('--bootstrap-server')
        args.append(self.bootstrap_servers)

        if self.command_config:
            args.append('--command-config')
            args.append(self.command_config)

        args.append('--remove')

        if permission_type == 'allow':
            for principal in principals:
                args.append('--allow-principal')
                args.append(principal)
            for host in hosts:
                args.append('--allow-host')
                args.append(host)
        elif permission_type == 'deny':
            for principal in principals:
                args.append('--deny-principal')
                args.append(principal)
            for host in hosts:
                args.append('--deny-host')
                args.append(host)
        else:
            raise AssertionError('Unsupported permission type: %s' % permission_type)

        for operation in operations:
            args.append('--operation')
            args.append(operation)

        for resource_type, resource_name in resources:
            if resource_type == 'cluster':
                args.append('--cluster')
            elif resource_type == 'group':
                args.append('--group')
                args.append(resource_name)
            elif resource_type == 'topic':
                args.append('--topic')
                args.append(resource_name)
            elif resource_type == 'transactional_id':
                args.append('--transactional-id')
                args.append(resource_name)
            elif resource_type == 'delegation_token':
                args.append('--transactional-id')
                args.append(resource_name)
            else:
                raise AssertionError('Unsupported resource type: %s' % resource_type)

        if pattern_type:
            args.append('--resource-pattern-type')
            args.append(pattern_type)

        rc, out, err = self.module.run_command(args)

        if rc!=0:
            raise AssertionError('kafka-acls command failed:\n%s\n%s' % (out, err))

def main():

    argument_spec = dict(
        bootstrap_servers=dict(type='str', required=True, aliases=['bootstrap_server', 'broker_list']),
        command_config=dict(type='str', default=None),
        principals=dict(type='list', elements='str', required=True),
        hosts=dict(type='list', elements='str', default=['*']),
        cluster=dict(type='str', default=None),
        groups=dict(type='list', elements='str', default=None),
        topics=dict(type='list', elements='str', default=None),
        transactional_ids=dict(type='list', elements='str', default=None),
        delegation_tokens=dict(type='list', elements='str', default=None),
        resources=dict(type='list', elements=dict(
            resource_type=dict(type='str', default=None, choices=['cluster', 'group', 'topic', 'transactional_id', 'delegation_token']),
            resource_name=dict(type='str', default=None)
            ), default=None),
        pattern_type=dict(type='str', default='literal', choices=['literal', 'prefixed']),
        permission_type=dict(type='str', default='allow'),
        operations=dict(type='list', elements='str', required=True),
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
    principals = p.get('principals')
    hosts = p.get('hosts') or ('*',)
    cluster = p.get('cluster')
    groups = p.get('groups') or ()
    topics = p.get('topics') or ()
    transactional_ids = p.get('transactional_ids') or ()
    delegation_tokens = p.get('delegation_tokens') or ()
    resources = list((r.get('resource_type'), r.get('resource_name')) for r in p.get('resources') or ())
    pattern_type = p.get('pattern_type')
    permission_type = p.get('permission_type')
    operations = p.get('operations') or ()
    status = p.get('status')
    style = p.get('style')
    kafka_home = p.get('kafka_home')

    try:

        if cluster:
            resources.append(('cluster', cluster))

        if groups:
            for group in groups:
                resources.append(('group', group))

        if topics:
            for topic in topics:
                resources.append(('topic', topic))

        if transactional_ids:
            for transactional_id in transactional_ids:
                resources.append(('transactional_id', transactional_id))

        if delegation_tokens:
            for delegation_token in delegation_tokens:
                resources.append(('delegation_token', delegation_token))

        if not resources:
            raise AssertionError('At last single resource required')

        command = 'kafka-acls' if style=='confluent' else 'kafka-acls.sh'

        kafka_acls = KafkaAcls(module, command, bootstrap_servers, command_config)

        if status == 'present':

            acls = kafka_acls.list(principals, resources, pattern_type)

            add_principals = set()
            add_hosts = set()
            add_operations = set()
            add_resources = set()

            for principal, host, operation, resource in itertools.product(principals, hosts, operations, resources):
                acl_resource = AclResource(resource[0], resource[1], pattern_type)
                acl_entry = AclEntry(principal, host, operation, permission_type)
                if acl_entry in acls.get(acl_resource, ()):
                    continue
                add_principals.add(principal)
                add_hosts.add(host)
                add_operations.add(operation)
                add_resources.add(resource)

            if not add_principals:
                module.exit_json(
                    changed=False, 
                    msg="ACL entries present")

            kafka_acls.add(
                add_principals, 
                add_hosts, 
                add_operations, 
                permission_type, 
                add_resources, 
                pattern_type)

            module.exit_json(
                changed=True, 
                msg="ACL entried created")

        elif status == 'absent':

            del_principals = set()
            del_hosts = set()
            del_operations = set()
            del_resources = set()

            for principal, host, operation, resource in itertools.product(principals, hosts, operations, resources):
                acl_resource = AclResource(resource[0], resource[1], pattern_type)
                acl_entry = AclEntry(principal, host, operation, permission_type)
                if acl_entry not in acls.get(acl_resource, list()):
                    continue
                del_principals.add(principal)
                del_hosts.add(host)
                del_operations.add(operation)
                del_resources.add(resource)

            if not del_principals:
                module.exit_json(
                    changed=False, 
                    msg="ACL entries absent")

            kafka_acls.remove(
                del_principals, 
                del_hosts, 
                del_operations, 
                permission_type, 
                del_resources, 
                pattern_type)

            module.exit_json(
                changed=True, 
                msg="ACL entried deleted")

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