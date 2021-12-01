# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import signal
import functools
import time

from pytest_bdd import scenarios, given, when, then, parsers

parse = functools.partial(parsers.parse)
scenarios('cluster')


@when('the cluster was terminated')
def stop_nebula_cluster(request, class_fixture_variables, pytestconfig):
    print('stopping nebula cluster...')
    cluster = class_fixture_variables["cluster"]
    cluster.kill_all(signal.SIGTERM)
    print('nebula cluster stopped...')


@then(parse('no service should still running after {duration}s'))
def check_nebula_cluster_stop(request, class_fixture_variables, duration, pytestconfig):
    print('check cluster status...')
    time.sleep(int(duration))
    cluster = class_fixture_variables["cluster"]
    stopped = not cluster.is_procs_alive()
    assert stopped, "the cluster is still running after SIGTERM"
