import functools
from pytest_bdd import given, parsers, then, when

from tests.common.utils import (
    resultset_to_dict_str,
    get_hosts_in_zones,
)
from tests.common.logger import logger

parse = functools.partial(parsers.parse)

@then(parse('verify the space partitions, space is "{space_name}"'))
def then_verify_space_parts(session, graph_spaces, space_name):
    """verify the space parts.
    1. leaders should be in zone hosts.
    2. peers should be in different zones.
    """
    res = session.execute("DESC SPACE {}".format(space_name))
    assert res.is_succeeded(), res.error_msg()
    res_dict = resultset_to_dict_str(res)
    part_num, replica_fator, zones = (
        res_dict["Partition Number"][0],
        res_dict["Replica Factor"][0],
        res_dict["Zones"][0],
    )
    logger.info(
        "part_num is {}, replica_fator is {}, zones is {}".format(
            part_num, replica_fator, zones
        )
    )
    hosts = get_hosts_in_zones(session, zones.replace('"', ""))
    res = session.execute("USE {};SHOW PARTS".format(space_name))
    assert res.is_succeeded(), res.error_msg()
    res_dict = resultset_to_dict_str(res)

    part_id_list, leader_list, peers_list = (
        res_dict["Partition ID"],
        res_dict["Leader"],
        res_dict["Peers"],
    )
    assert int(part_num) == len(
        part_id_list
    ), "expected partition number is {}, actual is {}".format(
        part_num, len(part_id_list)
    )
    for index, part_id in enumerate(part_id_list):
        leader = leader_list[index].replace('"', "")
        peers = peers_list[index].replace('"', "")
        peers = [x.strip() for x in peers.split(",")]
        zone_host = dict()
        if leader not in hosts:
            raise Exception(
                "leader is not in hosts, part id is {}, leader is {}, hosts is {}".format(
                    part_id, leader, ",".join(hosts.keys())
                )
            )
        # peers in hosts, and the zones are not same.
        for peer in peers:
            if peer not in hosts:
                raise Exception(
                    "peer is not in hosts, part id is {}, peer is {}, hosts is {}".format(
                        part_id, peer, ",".join(hosts.keys)
                    )
                )

            if hosts[peer] in zone_host:
                raise Exception(
                    "zones are in the same, zone is {}, peers are {}, {}".format(
                        hosts[peer], peer, zone_host[hosts[peer]]
                    )
                )
            zone_host[hosts[peer]] = peer
