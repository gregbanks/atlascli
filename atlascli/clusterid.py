from __future__ import annotations

import string
from typing import Optional

from colorama import init, Fore

from atlascli.atlasresource import AtlasResource


class ProjectID:

    def __init__(self, id: str, throw_exception: bool = True):
        self._id = ProjectID.validate_project_id(id, throw_exception)

    @property
    def id(self):
        return self._id

    @staticmethod
    def validate_project_id(p: str, throw_exception: bool = True) -> Optional[str]:
        if p is None:
            raise ValueError("project_id cannot be None")
        if len(p) < 24:
            if throw_exception:
                raise ValueError(
                    f"Not a valid project ID, project ID cannot be less than 24 chars: '{p}'")
            else:
                return None
        elif len(p) > 24:
            if throw_exception:
                raise ValueError(
                    f"Not a valid project ID, project ID cannot be more than 24 chars: '{p}'")
            else:
                return None

        for c in p:
            if c not in string.hexdigits:
                if throw_exception:
                    raise ValueError(
                        f"Not a valid project ID, string is not hexadecimal: '{p}'")
                else:
                    return None
        return p

    @staticmethod
    def canonical_project_id(pid: str) -> str:
        try:
            return ProjectID.validate_project_id(pid, throw_exception=True)

        except ValueError as e:
            print(e)
            raise

    def __eq__(self, rhs):
        return self.id == rhs.id

    def __str__(self):
        return f"{self.id}"


class InstanceID:
    INSTANCE_NAME_CHARS = string.ascii_letters + string.digits + '-'

    # Valid characters in an Atlas instance name

    def __init__(self, project_id: str, instance_name: str,
                 throw_exception: bool = True):

        self._project_id = ProjectID.validate_project_id(project_id,
                                                         throw_exception)
        self._name = self.__class__.validate_instance_name(instance_name,
                                                       throw_exception)

    @classmethod
    def get_instance_type(cls) -> str:
        raise NotImplementedError()

    @property
    def project_id(self):
        return self._project_id

    @property
    def name(self):
        return self._name

    @classmethod
    def validate_instance_name(cls, instance_name: str,
                               throw_exception=True) -> str:
        for c in instance_name:
            if c not in InstanceID.INSTANCE_NAME_CHARS:
                if throw_exception:
                    print(
                        f"{Fore.RED}{instance_name}{Fore.RESET} is not a valid {cls.get_instance_type()} "
                        f"(ASCII letters, numbers and '-' only")
                    raise ValueError(
                        f"{instance_name} is not a valid {cls.get_instance_type()} (ASCII letters, numbers and '-' only")
                else:
                    return None
        return instance_name

    @classmethod
    def parse(cls, s: str) -> InstanceID:
        project_id, separator, instance_name = s.partition(":")

        return cls(ProjectID.validate_project_id(project_id),
                   cls.validate_instance_name(instance_name))

    @classmethod
    def parse_id_name(cls, instance_name: str) -> (str, str):
        id, sep, name = instance_name.partition(":")
        if len(sep) == 0:
            return None, id
        elif len(id) == 0:
            return None, name
        else:
            return id, name
        # raise ValueError(f"{instance_name} cannot be parsed as a {cls.get_instance_typ()} name of the form 'id:name'")

    @classmethod
    def canonical_name(cls, instance_name: str) -> str:
        #
        # check that the instance name is of the form
        # <project_id>:<instance-name> Used by argparse. The name
        # is tuned to fit the error message
        #
        project_id, sep, name = instance_name.partition(":")
        if len(sep) == 0:
            print(
                f"{instance_name} must have a project ID and a {cls.get_instance_type()} name seperated by a ':'")
            raise ValueError

        try:
            project_id = ProjectID.validate_project_id(project_id,
                                                       throw_exception=True)
            name = cls.validate_instance_name(name, throw_exception=True)
        except ValueError as e:
            print(e)
            raise

        return f"{project_id}:{name}"

    def __eq__(self, rhs):
        return (self.project_id == rhs.project_id) and (self.name == rhs.name)

    def __str__(self):
        return f"{self.project_id}:{self._name}"

    def __repr__(self):
        return f"{__name__}(project_id={self._project_id}, {self.__class__.get_instance_type()}_name={self._name})"

    def pretty(self):
        return f"{Fore.YELLOW}{self.project_id}:{Fore.GREEN}{self.name}{Fore.RESET}"


class ServerlessID(InstanceID):

    def __init__(self, project_id: str, cluster_name: str,
                 throw_exception: bool = True):
        super().__init__(project_id, cluster_name, throw_exception)

    @classmethod
    def get_instance_type(cls) -> str:
        return "serverless"


class ClusterID(InstanceID):

    def __init__(self, project_id: str, cluster_name: str,
                 throw_exception: bool = True):
        super().__init__(project_id, cluster_name, throw_exception)

    @classmethod
    def get_instance_type(cls) -> str:
        return "cluster"
