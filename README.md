
# CP-Ansible


## Notes by Neil

### Playbooks

For localised testing I have created several hosts/inventory files. hosts-single targets a single machine, hosts-mregion uses 3 vms and deployed across them - allowing multi-region testing

#### Run against a single host - inv-hosts-single.yml

1. Update the host file to the hostname of the target vm
1. Run the playbook using: ansible-playbook 
1. >$ ansible-playbook  -i inv-hosts-single.yml book-all.yml --ask-pass --ask-become-pass<br>
Note: CC wont start due to too-few-brokers.
1. Switch it to dev mode:
1. >sudo systemctl status confluent-control-center
1. Look at the output from the 'status'  - it will show the location of the control-center.service file
1. Stop the service:<br> > sudo systemctl status confluent-control-center
1. Edit /lib/systemd/system/confluent-control-center.service
1. Change the properties from '/etc/confluent-control-center/control-center-production.properties' to '/etc/confluent-control-center/control-center-dev.properties'
1. >  systemctl start confluent-control-center
1. Check output
1. > tail -f /var/log/confluent/control-center/control-center.log
1. No errors? - browse to: http://MYSERVER:9021 


### 'Ping' the kafka_brokers
> $ ansible kafka_broker -i inv-hosts-mregion.yml -m ping  --ask-pass --ask-become-pass


#### Run multi-region across 3 servers - inv-hosts-mregion.yml
1. Update the host file to the hostname of the target vm
1. Run the playbook using: ansible-playbook 
1. >$ ansible-playbook  -i inv-hosts-mregion.yml book-all.yml --ask-pass --ask-become-pass<br>





## Introduction

Ansible provides a simple way to deploy, manage, and configure the Confluent Platform services. This repository provides playbooks and templates to easily spin up a Confluent Platform installation. Specifically this repository:

* Installs Confluent Platform packages.
* Starts services using systemd scripts.
* Provides configuration options for plaintext, SSL, SASL_SSL, and Kerberos.

The services that can be installed from this repository are:

* ZooKeeper
* Kafka
* Schema Registry
* REST Proxy
* Confluent Control Center
* Kafka Connect (distributed mode)
* KSQL Server

## Documentation

You can find the documentation for running CP-Ansible at https://docs.confluent.io/current/tutorials/cp-ansible/docs/index.html.

## Contributing


If you would like to contribute to the CP-Ansible project, please refer to the [CONTRIBUTE.md](https://github.com/confluentinc/cp-ansible/blob/5.4.x/CONTRIBUTING.md)


## License

[Apache 2.0](https://github.com/confluentinc/cp-ansible/blob/5.4.x/LICENSE.md)
