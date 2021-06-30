import inspect
import itertools
from typing import Dict, List, Generator, Callable, Optional, Union

from atlascli.atlasapi import AtlasAPI
from atlascli.atlascluster import AtlasCluster
from atlascli.atlaskey import AtlasKey
from atlascli.atlasorganization import AtlasOrganization
from atlascli.atlasproject import AtlasProject
from atlascli.atlasserverless import AtlasServerless
from atlascli.clusterid import ClusterID


class AtlasMap:
    #
    # Maintain view of the complete organization
    #
    # Each org can have multiple projects.
    # Each project can have multiple clusters.
    # Each cluster represents a group of machines/nodes. Clusters may be sharded.
    # Each serverless represents a serverless instance.
    #

    def __init__(self, org: AtlasOrganization = None, api: AtlasAPI = None,
                 populate: bool = False):

        self._org = org
        self._populate = populate
        self._clusters: List[AtlasCluster] = None
        self._serverless: List[AtlasServerless] = None
        # map of all project ids to projects
        self._project_map: Dict[str, Dict] = None

        # map from project id to a dict of clusters/serverless
        # because cluster/serverless names are not unique across an organization
        # we have to key each collection of clusters/serverless under a specific
        # project id.
        self._project_cluster_map: Dict[str, Dict[str, AtlasCluster]] = {}
        self._project_serverless_map: Dict[str, Dict[str, AtlasServerless]] = {}

        if api:
            self._api = api
        else:
            self._api = AtlasAPI()

        if self._populate:
            self.populate()

    def authenticate(self, k: AtlasKey = None):
        self._api.authenticate(k)

    @property
    def api(self):
        return self._api

    @property
    def projects(self):
        return list(self.project_map.values())

    def _instances(self, instances, project_instance_map,
                   populate_instances_map_meth, instances_map_attr_name,
                   instances_attr_name):
        if instances is None:
            _instances = []

            if len(project_instance_map) == 0:
                populate_instances_map_meth()
            project_instance_map = getattr(self, instances_map_attr_name)

            # return list(itertools.chain.from_iterable(self._project_cluster_map.values()))
            for project_id, instance_dict in project_instance_map.items():
                for instance_name, instance in instance_dict.items():
                    _instances.append(instance)

            setattr(self, instances_attr_name, _instances)

        return getattr(self, instances_attr_name)

    @property
    def clusters(self):
        return self._instances(self._clusters, self._project_cluster_map,
                               self.populate_cluster_map,
                               '_project_cluster_map', '_clusters')

    @property
    def serverless(self):
        return self._instances(self._serverless, self._project_serverless_map,
                               self.populate_serverless_map,
                               '_project_serverless_map', '_serverless')

    # TODO <GEB>

    @property
    def organization(self):
        return self._org

    def populate(self):
        raise NotImplementedError(inspect.currentframe().f_code.co_name)

    @property
    def project_map(self):
        if self._project_map is None:
            self.populate_project_map()
        return self._project_map

    @property
    def project_cluster_map(self):
        if len(self._project_cluster_map) == 0:
            self.populate_cluster_map()
        return self._project_cluster_map

    @property
    def project_serverless_map(self):
        if len(self._project_serverless_map) == 0:
            self.populate_serverless_map()
        return self._project_serverless_map

    def populate_project_map(self):
        new_projects_map = {}
        for project in self._api.get_projects():
            new_projects_map[project.id] = project

        self._project_map = new_projects_map

    def _populate_instance_map(self, get_all_instances_meth):
        new_project_instances_map = {}
        for project in self.project_map.values():
            new_project_instances_map[project.id] = {}
            for instance in get_all_instances_meth(project.id):
                new_project_instances_map[project.id][instance.name] = instance
        assert len(self.project_map) == len(new_project_instances_map)

        return new_project_instances_map

    def populate_cluster_map(self):
        self._project_cluster_map = self._populate_instance_map(
            self._api.get_all_clusters)

    def populate_serverless_map(self):
        self._project_serverless_map = self._populate_instance_map(
            self._api.get_all_serverless)

    def populate_instances_map(self):
        self.populate_project_map()
        self.populate_cluster_map()
        self.populate_serverless_map()

    def is_project_id(self, project_id: str) -> bool:
        return project_id in [x.id for x in self.projects]

    def is_cluster_name(self, cluster_name: str) -> bool:
        # print(f"is_cluster_name({cluster_name})")
        # pprint.pprint([x.name for x in self._clusters])
        return any([x.name == cluster_name for x in self.clusters])

    def is_serverless_name(self, serverless_name: str) -> bool:
        return any([x.name == serverless_name for x in self.serverless])

    @staticmethod
    def _is_unique_instance(instance_name: str,
                            get_instance_meth: Callable[
                                [str,
                                 Optional[object]], List[
                                    Union[AtlasCluster, AtlasServerless]]]) -> \
        Union[
            AtlasCluster, AtlasServerless, None]:
        l = get_instance_meth(instance_name)
        if len(l) == 1:
            return l[0]
        else:
            return None

    def is_unique_cluster(self, cluster_name: str) -> AtlasCluster:
        return AtlasMap._is_unique_instance(cluster_name, self.get_cluster)

    def is_unique_serverless(self, serverless_name: str) -> AtlasServerless:
        return AtlasMap._is_unique_instance(serverless_name,
                                            self.get_serverless)

    @staticmethod
    def _get_instance_names(
        instances: List[Union[AtlasCluster, AtlasServerless]]):
        for i in instances:
            yield i.name

    def get_cluster_names(self):
        return AtlasMap._get_instance_names(self.clusters)

    def get_serverless_names(self):
        return AtlasMap._get_instance_names(self.serverless)

    @staticmethod
    def _get_instance_project_ids(instance_name: str,
                                  project_instance_map: Dict[str, Dict[
                                      str, Union[
                                          AtlasCluster, AtlasServerless]]]):
        project_ids = []
        for project_id, instance_map in project_instance_map.items():
            for name, instance in instance_map.items():
                if name == instance_name:
                    project_ids.append(project_id)
        return project_ids

    def get_cluster_project_ids(self, cluster_name: str):
        return AtlasMap._get_instance_project_ids(cluster_name,
                                                  self.project_cluster_map)

    def get_serverless_project_ids(self, serverless_name: str):
        return AtlasMap._get_instance_project_ids(serverless_name,
                                                  self.project_serverless_map)

    def get_project_ids(self) -> List[str]:
        return [x.id for x in self.projects]

    def get_one_project(self, project_id: str) -> AtlasProject:
        return self._project_map[project_id]

    def get_projects(self) -> Dict[str, AtlasProject]:
        return self._project_map

    def get_project_id(self, project_name: str):
        for i in self.projects:
            # print(i.name)
            if i.name == project_name:
                return i.id
        return None

    def get_project_name(self, project_id: str):
        for i in self.projects:
            if i.id == project_id:
                return i.name
        return None

    @staticmethod
    def _get_instance(instance_name: str,
                      instances: List[Union[AtlasCluster, AtlasServerless]],
                      project_id: object = None) -> \
        List[Union[AtlasCluster, AtlasServerless]]:
        #
        # Cluster/Serverless names are not unique so we might get more than one
        # cluster when we request a cluster.
        #
        _instances = []
        for i in instances:
            if i.name == instance_name:
                if project_id is None:
                    _instances.append(i)
                elif project_id == i.project_id:
                    _instances.append(i)
        return _instances

    def get_cluster(self, cluster_name: str, project_id: object = None) -> \
        List[AtlasCluster]:
        return AtlasMap._get_instance(cluster_name, self.clusters, project_id)

    def get_serverless(self, cluster_name: str, project_id: object = None) -> \
        List[AtlasServerless]:
        return AtlasMap._get_instance(cluster_name, self.clusters, project_id)

    @staticmethod
    def _get_one_instance(project_id: str, instance_name: str,
                          get_instances_meth: Callable[[str, str], List[
                              Union[AtlasCluster, AtlasServerless]]]) -> Union[
        AtlasCluster, AtlasServerless]:
        instances = get_instances_meth(instance_name, project_id)
        if len(instances) == 0:
            raise ValueError(f"No such instance {project_id}:{instance_name}")
        else:
            assert len(instances) == 1
            return instances[0]

    def get_one_cluster(self, project_id: str,
                        cluster_name: str) -> AtlasCluster:
        return AtlasMap._get_one_instance(project_id, cluster_name,
                                          self.get_cluster)

    def get_one_serverless(self, project_id: str,
                           serverless_name: str) -> AtlasServerless:
        return AtlasMap._get_one_instance(project_id, serverless_name,
                                          self.get_serverless)

    @staticmethod
    def _get_all_instances(
        instances: List[Union[AtlasCluster, AtlasServerless]],
        project_id: str = None) -> Generator[
        Union[AtlasCluster, AtlasServerless], None, None]:
        for i in instances:
            if project_id is None:
                yield i
            elif project_id == i.project_id:
                yield i

    def get_all_clusters(self, project_id: str = None) -> Generator[
        AtlasCluster, None, None]:
        return AtlasMap._get_all_instances(self.clusters, project_id)

    def get_all_serverless(self, project_id: str = None) -> Generator[
        AtlasServerless, None, None]:
        return AtlasMap._get_all_instances(self.serverless, project_id)

    @staticmethod
    def _create_instance(project_id: str, instance_name: str,
                         project_instance_map: Dict[str, Dict[
                             str, Union[AtlasCluster, AtlasServerless]]],
                         api_create_meth: Callable[[str, str], Union[
                             AtlasCluster, AtlasServerless]]) -> Union[
        AtlasCluster, AtlasServerless]:
        i = api_create_meth(project_id, instance_name)
        project_instance_map.setdefault(project_id, {})[i.name] = i
        return i

    def create_cluster(self, project_id: str,
                       cluster_name: str) -> AtlasCluster:
        return AtlasMap._create_instance(project_id, cluster_name,
                                         self.project_cluster_map,
                                         self._api.create_cluster)

    def create_serverless(self, project_id: str,
                          serverless_name: str) -> AtlasServerless:
        return AtlasMap._create_instance(project_id, serverless_name,
                                         self.project_serverless_map,
                                         self._api.create_serverless)

    def parse_cluster_id(self, cluster_str: str) -> ClusterID:
        cluster_id = ClusterID.parse(cluster_str, throw_exception=True)
        if self.is_project_id(cluster_id.project_id):
            if self.is_cluster_name(cluster_id.name):
                return cluster_id
            else:
                return ValueError(
                    f"{cluster_id.name} is not a valid cluster name in this organization")
        else:
            return ValueError(
                f"{cluster_id.project_id}is not a valid project id in this organization")

    def pprint(self):
        print(self._org.summary())
        for project in self.projects:
            print(f" Project: {project.pretty_project_id():<40}")
            for v in self.project_cluster_map[project.id].values():
                print(f"  Cluster: {v.summary()}")
            for v in self.project_serverless_map[project.id].values():
                print(f"  Serverless: {v.summary()}")
