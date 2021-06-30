from typing import Dict

from atlascli.atlasinstance import AtlasInstance


class AtlasServerless(AtlasInstance):

    def __init__(self, project_id: str, name: str = None,
                 serverless_config: Dict = None):
        super().__init__(project_id=project_id, name=name,
                         instance_config=serverless_config)

    @classmethod
    def default_serverless(cls):
        return {
            # name: "DataStore",
            "providerSettings": {
                "providerName": "SERVERLESS",
                "backingProviderName": "AWS",
                "regionName": "US_EAST_1"
            }
        }
