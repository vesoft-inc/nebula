from socket import MSG_PEEK
from time import time


import os
import time
import tempfile

import pytest
from pytest_bdd import scenarios, given, when, then, parsers

# import pdb; pdb.set_trace()

from tests.cluster.nebula_cluster import NebulaManager

# from selenium import webdriver
# from selenium.webdriver.common.keys import Keys

# Constants

DUCKDUCKGO_HOME = 'https://duckduckgo.com/'

# Scenarios
scenarios('features', 'cluster')

# Fixtures
@pytest.fixture
def nebula_cluster():
    build_dir = os.path.join(os.path.dirname(os.getcwd()), 'build')
    cluster_dir = tempfile.mkdtemp(prefix='nebula-cluster-')
    print('build dir: {}'.format(build_dir))
    print('cluster dir: {}'.format(cluster_dir))
    nebula_cluster = NebulaManager(build_dir, cluster_dir=cluster_dir, storage_num=1, graph_num=1, meta_num=1, local_ip='127.0.0.1')
    return nebula_cluster

# Given Steps
@given('a small nebula cluster')
def start_nebula_cluster(nebula_cluster):
    print('starting nebula cluster...')
    nebula_cluster.prepare()
    print('nebula cluster started...')
    return nebula_cluster

@when('the cluster was terminated')
def stop_nebula_cluster(nebula_cluster):
    print('stopping nebula cluster...')
    nebula_cluster.cleanup()
    print('nebula cluster stopped...')

@then('no service should still running after 4s')
def check_nebula_cluster_stop(nebula_cluster):
    print('check cluster status...')
    # print('check cluster stop: {}'.format(nebula_cluster))
    time.sleep(4)
    stopped = nebula_cluster.check_cluster_stopped()
    print('stopped: {}'.format(stopped))
    assert stopped
