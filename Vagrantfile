# -*- mode: ruby -*-
# vi: set ft=ruby :

require 'etc'
require 'pathname'

Vagrant.configure("2") do |config|
    # define synced folders
    config.vm.synced_folder ".", "/home/vagrant/mp-ingester"

    config.vm.define "mp_ingester" do |mp_ingester|
        # define virtualization provider
        mp_ingester.vm.provider "virtualbox"
        # define box
        mp_ingester.vm.box = "bento/ubuntu-16.04"

        config.vm.provider "virtualbox" do |v|
            # define RAM in MBs
            v.memory = 2048
            # define number of vCPUs
            v.cpus = 2
        end
    end

    # forward SSH agent
    config.ssh.forward_agent = true
    config.ssh.insert_key = false

    config.vm.network :forwarded_port, guest: 22, host: 2408, id: "ssh", auto_correct: false
    config.vm.network :forwarded_port, guest: 5432, host: 5436, id: "postgres"

    # provision with Ansible
    config.vm.provision :ansible do |ansible|
        ansible.playbook = "app-mp-ingester.yml"
        ansible.host_key_checking = false
        ansible.vault_password_file = ".ansible-vault-password"
        if ENV['ANSIBLE_TAGS'] != ""
            ansible.tags = ENV['ANSIBLE_TAGS']
        end

        ansible.extra_vars = {
            is_vagrant: true,
        }
    end
end
