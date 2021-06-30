from __future__ import annotations

import pprint
from typing import Dict, List

from colorama import Fore

from atlascli.atlasresource import AtlasResource
from atlascli.clusterid import ClusterID, InstanceID


class AtlasInstance(AtlasResource):

    def __init__(self, project_id: str, name: str = None,
                 instance_config: Dict = None):

        super().__init__(instance_config)
        self._project_id = project_id
        self.name = name

    @staticmethod
    def strip(d: Dict, keys: List[str]):
        for k in keys:
            if k in d:
                del d[k]
        return d

    @staticmethod
    def strip_instance_dict(cluster: Dict) -> Dict:
        #
        # Strip out keys that cannot be used to create a cluster from an existing
        # configuration
        #
        d = AtlasInstance.strip(cluster, ["connectionStrings",
                                          "replicationSpecs",
                                          "mongoURI",
                                          "mongoURIWithOptions",
                                          "mongoURIUpdated",
                                          "paused",
                                          "stateName"])

        return d

    @staticmethod
    def strip_cluster(cluster: AtlasInstance) -> AtlasInstance:
        return AtlasInstance(cluster.project_id,
                             cluster.name,
                             AtlasInstance.strip_instance_dict(cluster.resource))

    @property
    def cluster_id(self):
        return self.id

    @property
    def project_id(self):
        return self._project_id

    def is_paused(self):
        return self.resource["paused"]

    @staticmethod
    def is_valid_instance_name(s: str) -> bool:
        for c in s:
            if c not in InstanceID.INSTANCE_NAME_CHARS:
                return False
        return True

    def __str__(self):
        return f"{pprint.pformat(self.resource)}"

    def status(self) -> str:
        if self.resource["stateName"] == "REPAIRING":
            if self.is_paused():
                return f"{Fore.LIGHTRED_EX}pausing...{Fore.RESET}"
            else:
                return f"{Fore.LIGHTRED_EX}resuming...{Fore.RESET}"
        elif self.resource["stateName"] == "CREATING":
            return f"{Fore.LIGHTRED_EX}creating...{Fore.RESET}"
        elif self.resource["stateName"] == "DELETING":
            return f"{Fore.LIGHTRED_EX}deleting...{Fore.RESET}"
        elif self.resource["stateName"] == "IDLE":
            if self.is_paused():
                return f"{Fore.LIGHTBLUE_EX}paused{Fore.RESET}"
            else:
                return f"{Fore.RED}running{Fore.RESET}"
        else:
            return f"{self.resource['stateName']}"

    @property
    def state(self):
        return self.resource['stateName']

    def short_name(self):
        return f"{self.project_id}:{self.name}"

    def instance_size(self):
        return self.resource["providerSettings"]["instanceSizeName"]

    def pretty_instance_size(self):
        return f"{Fore.LIGHTWHITE_EX}{self.instance_size()}{Fore.RESET}"

    def pretty_id(self):
        return f"{Fore.CYAN}{self.project_id}{Fore.RESET}"

    def disk_size(self):
        return self.resource["diskSizeGB"]

    def pretty_disk_size(self):
        return f"{Fore.LIGHTWHITE_EX}{self.disk_size()}{Fore.RESET}"

    def summary(self):
        return f"{self.pretty_id_name():65} instance size:{self.pretty_instance_size():>15} " \
               f" disk GB:{self.pretty_disk_size():>15} state: {self.status():20}"
