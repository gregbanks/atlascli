#!/usr/bin/env python3

"""
MongoDB Atlas API (atlascli)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Python API to MongoDB Atlas.

Author: Joe.Drumgoole@mongodb.com
"""
import argparse
import requests
import os
import pprint
import sys
import logging
import json
from enum import Enum
from typing import List

from atlascli.atlaskey import AtlasKey
from atlascli.errors import AtlasError, AtlasGetError
from atlascli.atlasapi import AtlasAPI
from atlascli.version import __VERSION__
from atlascli.atlasorganization import AtlasOrganization
from atlascli.atlascluster import AtlasCluster
class ParseError(ValueError):
    pass


def parse_id(s, sep=":"):
    """

    :param s: A sting of the form <id1>:<id2> used to specify an ID tuple
    typically used to specify a project and cluster_name.
    :param sep: Seperator string used for two parts of s. Default is ':'.
    :return: id1,id2

    Throws ParseError if strings do not split on a sep of ':'.

    >>> parse_id("xxxx:yyyy")
    ('xxxx', 'yyyy')
    >>>
    """

    id1, separator, id2 = s.partition(sep)

    if separator != sep:
        raise ParseError(f"Bad separator '{separator}' in {s}")
    return id1, id2


# class ClusterState(Enum):
#     PAUSE = "pause"
#     RESUME = "resume"

#     def __str__(self):
#         return self.value


# class HTTPOperationName(Enum):
#     GET = "get"
#     POST = "post"
#     PATCH = "patch"

#     def __str__(self):
#         return self.value


# class AtlasOperationName(Enum):
#     CREATE = "create"
#     PATCH = "patch"
#     DELETE = "delete"
#     LIST = "list"
#     PAUSE = "pause"
#     RESUME = "resume"

#     def __str__(self):
#         return self.value


# class AtlasResourceName(Enum):
#     ORGANIZATION = "organization"
#     PROJECT = "project"
#     CLUSTER = "cluster"

    def __str__(self):
        return self.value

def pause_cluster(c:AtlasCluster):
    if c.is_paused():
        print(f"Cluster '{c.name}' is already paused")
    else:
        print(f"Pausing '{c.name}'")
        c.pause()
        print(f"Paused cluster '{c.name}'")

def resume_cluster(c:AtlasCluster):
    if c.is_paused():
        print(f"Resuming '{c.name}'")
        c.resume()
        print(f"Resumed cluster '{c.name}'")
    else:
        print(f"Cluster '{c.name}' is already running")   
    print(f"Pausing '{cluster_name}'")
    cluster = api.get_one_cluster(project_id, cluster_name)
    if cluster['paused']:
        result = api.resume(project_id, cluster_name)
        print(f"Resumed cluster '{cluster_name}'")

    else:
        print(f"'{cluster_name}' is already running, nothing to do")

def pause_command(org:AtlasOrganization, arg_project_ids:List[str], arg_cluster_names:List[str]):
    for cluster_name in arg_cluster_names:
        clusters = org.get_cluster(cluster_name)
        if len(clusters) == 0:
            print(f"No such cluster '{cluster_name}'")
        elif len(clusters == 1):
            clusters[0].pause()
        elif len(arg_project_ids) == 1 :
            clusters = org.get_cluster(cluster_name, arg_project_ids[0])
            clusters[0].pause()
        elif len(arg_project_ids) < 1 :
            print("You must specify only one project ID when pausing multiple clusters")
            print("You specified none (use the --project_id argument)")
        else:
            print("You must specify at least one project ID when pausing multiple clusters")
            print(f"You specified several project IDs: {arg_project_ids}")       

            # pprint.pprint(result)

def resume_command(org:AtlasOrganization, arg_project_ids:List[str], arg_cluster_names:List[str]):
    for cluster_name in arg_cluster_names:
        clusters = org.get_cluster(cluster_name)
        if len(clusters) == 0:
            print(f"No such cluster '{cluster_name}'")
        elif len(clusters == 1):
            clusters[0].resume()
        elif len(arg_project_ids) == 1 :
            clusters = org.get_cluster(cluster_name, arg_project_ids[0])
            clusters[0].resume()
        elif len(arg_project_ids) < 1 :
            print("You must specify only one project ID when resuming multiple clusters")
            print("You specified none (use the --project_id argument)")
        else:
            print("You must specify at least one project ID when resuming multiple clusters")
            print(f"You specified several project IDs: {arg_project_ids}") 


def main():
    parser = argparse.ArgumentParser(description=
                                     f"A command line program to list organizations,"
                                     f"projects and clusters on a MongoDB Atlas organization."
                                     f"You need to enable programmatic keys for this program"
                                     f" to work. See https://docs.atlas.mongodb.com/reference/api/apiKeys/ ",
                                     epilog=f"Version: {__VERSION__}")

    parser.add_argument("--publickey", help="MongoDB Atlas public API key."
                                            "Can be read from the environment variable ATLAS_PUBLIC_KEY")
    parser.add_argument("--privatekey", help="MongoDB Atlas private API key."
                                             "Can be read from the environment variable ATLAS_PRIVATE_KEY")

    # parser.add_argument("--atlasop",
    #                     type=AtlasOperationName,
    #                     default=None,
    #                     choices=list(AtlasOperationName),
    #                     help="Which Atlas operation do you want to run create, modify, delete, list, pause, resume"
    #                     )
    #
    # parser.add_argument("--resource",
    #                     type=AtlasResourceName, default=None,
    #                     choices=list(AtlasResourceName),
    #                     help="Which resource type are we operating on:"
    #                          "organization, project or cluster?")

    # parser.add_argument("--data",
    #                     help="Arguments for create and modify arguments (Python dict)")

    # parser.add_argument("--orgid",
    #                     help="ID for an AtlasOrganization")

    # parser.add_argument("--projectname",
    #                     help="Project name for an AtlasProject")
    #
    # parser.add_argument("--clustername",
    #                     help="name  for an AltasCluster")

    # parser.add_argument("--org", action="store_true", default=False,
    #                     help="Get the organisation associated with the "
    #                          "current API key pair [default: %(default)s]")

    parser.add_argument("-p", "--pause", default=[], dest="pause_cluster",
                        action="append",
                        help="pause named cluster in project specified by project_id "
                             "Note that clusters that have been resumed cannot be paused "
                             "for the next 60 minutes")

    parser.add_argument("-r", "--resume", default=[],
                        dest="resume_cluster", action="append",
                        help="resume named cluster in project specified by project_id")

    parser.add_argument("-l", "--list",
                        action="store_true",
                        default=False,
                        help="List everything in the organization")

    parser.add_argument("-lp", "--listproj",
                        action="store_true",
                        default=False,
                        help="List all projects")

    parser.add_argument("-lc", "--listcluster",
                        action="store_true",
                        default=False,
                        help="List all clusters")

    parser.add_argument("-pid", "--project_id", default=[], dest="project_id_list",
                        action="append",
                        help="specify the project ID for cluster that is to be paused")
    # parser.add_argument('--format', type=OutputFormat,
    #                     default=OutputFormat.SUMMARY, choices=list(OutputFormat),
    #                     help="The format to output data either in a single line "
    #                          "summary or a full JSON document [default: %(default)s]")
    parser.add_argument("-d", "--debug", default=False, action="store_true",
                        help="Turn on logging at debug level")
    # parser.add_argument("--http", type=HTTPOperationName, choices=list(HTTPOperationName),
    #                     help="do a http operation")
    # parser.add_argument("--url", help="URL for HTTP operation")
    # parser.add_argument("--itemsperpage", type=int, default=100,
    #                     help="No of items to return per page [default: %(default)s]")
    # parser.add_argument("--pagenum", type=int, default=1,
    #                     help="Page to return [default: %(default)s]")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)

    logging.debug("logging is on at DEBUG level")

    # Stop noisy urllib3 info logs
    logging.getLogger("requests").setLevel(logging.WARNING)
    if args.publickey:
        public_key = args.publickey
    else:
        public_key = os.getenv("ATLAS_PUBLIC_KEY")
        if public_key is None:
            print("you must specify an ATLAS public key via --publickey arg "
                  "or the environment variable ATLAS_PUBLIC_KEY")
            sys.exit(10)

    if args.privatekey:
        private_key = args.privatekey
    else:
        private_key = os.getenv("ATLAS_PRIVATE_KEY")
        if private_key is None:
            print("you must specify an an ATLAS private key via --privatekey"
                  "arg or the environment variable ATLAS_PRIVATE_KEY")
            sys.exit(1)

    api = AtlasAPI(AtlasKey(public_key, private_key))
    org = AtlasOrganization(api)

    # data = None
    # if args.data:
    #     try:
    #         data = json.loads(args.data)
    #     except json.JSONDecodeError as e:
    #         print("Error decoding:")
    #         print(f"{args.data}")
    #         print(f"error:{e}")
    #         sys.exit(1)

    # if args.atlasop is AtlasOperationName.CREATE:
    #     if args.resource is AtlasResourceName.ORGANIZATION:
    #         print('No support for organization creation at the moment use the UI')
    #     elif args.resource is AtlasResourceName.PROJECT:
    #         if args.projectname:
    #             project = api.create_project(org.id, args.projectname)
    #             project.print_resource(args.format)
    #         else:
    #             print("You must specify a project name via --projectname")
    #     else:  # Cluster
    #         if args.projectid:
    #             if data:
    #                 cluster = api.create_cluster(args.project_id, args.data)
    #                 cluster.print_resource(args.format)
    #             else:
    #                 print("You must specify a JSON data object via --data")
    # elif args.atlasop is AtlasOperationName.PATCH:
    #     if args.resource is AtlasResourceName.ORGANIZATION:
    #         print("No support for modifying organizations in this release")
    #     elif args.resource is AtlasResourceName.PROJECT:
    #         print("There is no modify capability for projects in MongoDB Atlas")
    #     else:  # Cluster
    #         if args.projectid:
    #             if args.clustername:
    #                 cluster = api.modify_cluster(args.projectid,
    #                                              args.clustername,
    #                                              args.data)
    #                 cluster.print_resource(args.format)
    #             else:
    #                 print(f"You must specify a cluster name via --clustername")
    #         else:
    #             print(f"You must specify a project id via --projectid")
    # elif args.atlasop is AtlasOperationName.DELETE:
    #     if args.resource is AtlasResourceName.ORGANIZATION:
    #         print("You cannot delete organisations via atlascli at this time")
    #     elif args.resource is AtlasResourceName.PROJECT:
    #         if args.projectid:
    #             project = api.get_one_project(args.projectid)
    #             api.delete_project(args.projectid)
    #             print(f"Deleted project: {project.summary_string()}")
    #     elif args.resource is AtlasResourceName.CLUSTER:
    #         if args.projectid:
    #             if args.clustername:
    #                 cluster = api.get_one_cluster(args.project_id[0], args.clustername)
    #                 api.delete_cluster(args.projectid, args.clustername)
    #                 print(f"Deleted cluster: {cluster.summary_string()}")
    #             else:
    #                 print("You must specify a a cluster name via --clustername")
    #         else:
    #             print("You must specify a project id via --projectid")

        
    if args.list:
        #print(org)
        org.pprint()
    if args.listproj :
        if args.project_id_list:
            for project_id in args.project_id_list:
                print(api.get_one_project(project_id))
        else:
            for project in api.get_projects():
                print(f"\nProject: '{project.name}'")
                print(project)
    if args.listcluster:
        if args.project_id_list:
            for project_id in args.project_id_list:
                clusters = api.get_clusters(project_id)

                for cluster in clusters:
                    print(f"\nProject: '{project_id}' Cluster: '{cluster.name}'")
                    print(cluster)
        else:
            for project in api.get_projects():
                clusters = api.get_clusters(project.id)
                for cluster in clusters:
                    print(f"\nProject: '{project.id}' Cluster: '{cluster.name}'")
                    print(cluster)

    if args.pause_cluster or args.resume_cluster:
        if args.pause_cluster:
            pause_command(org, args.project_id_list, args.pause_cluster) 
        if args.resume_cluster:
            resume_command(org, args.project_id_list, args.resume_cluster)

if __name__ == "__main__":
    try:
        main()
    except AtlasError as e:
        print(f"AtlasError : '{e}'")
    except AtlasGetError as e:
        print(f"AtlasGetError : '{e}'")
    except KeyboardInterrupt:
        print("Ctrl-C...")
