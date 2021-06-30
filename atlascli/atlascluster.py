from typing import Dict

from atlascli.atlasinstance import AtlasInstance


class AtlasCluster(AtlasInstance):

    def __init__(self, project_id: str, name: str = None,
                 cluster_config: Dict = None):
        super().__init__(project_id=project_id, name=name,
                         instance_config=cluster_config)

    @classmethod
    def default_single_region_cluster(cls):
        return {
            # "name" : "DataStore",
            "diskSizeGB": 100,
            "numShards": 1,
            "providerSettings": {
                "providerName": "AWS",
                "diskIOPS": 300,
                "instanceSizeName": "M40",
                "regionName": "US_EAST_1"
            },
            "replicationFactor": 3,
            "autoScaling": {"diskGBEnabled": True},
        }

