"""Instance Type Validation interface."""

import re
import json
import os

from collections import defaultdict

import numpy as np
import pandas as pd

CFG_DIR = '{}/config'.format(os.path.dirname(__file__))


class AwsInstanceTypeValidationResult:
    """Instance Type Validation Result."""

    def __init__(self):
        """Initialize value that holds Result."""
        self.instance_type_validation = defaultdict(defaultdict)
        self.invalid_items = defaultdict(list)
        self.hosts = defaultdict(list)

    def add_recommendations(self, source_id, message):
        """Append conclusive Result message to Cluster Id.

        Only include wrong clusters
        """
        self.invalid_items[source_id].append(message)

    def set_hosts(self, hosts):
        """Assign per-host with recommendations dict."""
        self.hosts = hosts

    def to_dict(self):
        """Convert Instance Type Validation Result instance to dict."""
        self.instance_type_validation['clusters'] = self.invalid_items
        self.instance_type_validation['hosts'] = self.hosts
        return self.instance_type_validation


class AwsInstanceTypeValidation:
    """Instance Type Validation."""

    def __init__(self, dataframes):
        """Initialize values required to Instance Type Validation."""
        self.result = AwsInstanceTypeValidationResult()
        self.container_nodes = dataframes.get('container_nodes')
        self.container_nodes_tags = dataframes.get('container_nodes_tags')
        self.container_images = dataframes.get('container_images')
        self.container_images_tags = dataframes.get('container_images_tags')
        self.containers = dataframes.get('containers')
        self.container_groups = dataframes.get('container_groups')
        self.vms = dataframes.get('vms')
        self.sources = dataframes.get('sources')

        with open(f'{CFG_DIR}/instance_type_mappings.json') as json_file:
            instance_type_mappings = json.load(json_file)

        self.instance_type_mappings = instance_type_mappings

        ci_taggings = self.container_images_tags
        ioo_taggings = ci_taggings[
            ci_taggings['name'] == "io.openshift.expose-services"].copy()

        ioo_taggings.loc[:, 'value'] = ioo_taggings.value.dropna().apply(
            lambda x: x.split(",")
        )

        self.tags = pd.DataFrame(
            [
                (tup.container_image_id, d)
                for tup in ioo_taggings.dropna().itertuples()
                for d in tup.value
            ],
            columns=[
                'container_image_id',
                'tag'
            ]
        )

        self.compute_roles = self.container_nodes_tags[
            self.container_nodes_tags['name'] ==
            "node-role.kubernetes.io/compute"
            ].copy()

        self.master_roles = self.container_nodes_tags[
            self.container_nodes_tags['name'] ==
            "node-role.kubernetes.io/master"
            ].copy()

        self.infra_roles = self.container_nodes_tags[
            self.container_nodes_tags['name'] ==
            "node-role.kubernetes.io/infra"
            ].copy()

        self.type_roles = self.container_nodes_tags[
            self.container_nodes_tags['name'] == "type"
            ].copy()

        self.instance_types = self.container_nodes_tags[
            self.container_nodes_tags['name'] ==
            "beta.kubernetes.io/instance-type"
            ].copy()

    def validate(self):
        """Validate Instance Types."""
        # Load running containers
        tags_of_active_containers = self.tags_of_active_containers()

        # Load nodes having role compute
        compute_nodes = self.compute_nodes()

        # Group nodes by a cluster
        container_nodes_groups = compute_nodes[
            compute_nodes.lives_on_type == 'Vm'].groupby("source_id").groups

        for key in container_nodes_groups.keys():
            # Make recommendation for each cluster
            self._find_invalid(
                key,
                compute_nodes.loc[container_nodes_groups.get(key, [])],
                tags_of_active_containers
            )

        self._per_host_recommendations()

        return self.result

    def _per_host_recommendations(self):
        hosts = {}
        for _index, host in self.vms.iterrows():
            hosts[host.id] = {
                "inventory_id": host.host_inventory_uuid,
                "vm_id": host.id,
                "name": host['name'],
                "source_ref": host.source_ref,
                "recommendations": self._recommendations(host.id)
            }

        self.result.set_hosts(hosts)

    def _recommendations(self, host_id):
        recommendations = []

        for cluster in self.result.invalid_items.keys():
            try:
                recommendations = next(
                    item for item in self.result.invalid_items[cluster]
                    if item["vm_id"] == host_id
                )
                break
            except StopIteration:
                continue
        return recommendations

    def active_containers(self):
        """Return containers that are active/running."""
        return self.containers[
            self.containers['container_group_id'].isin(
                self.container_groups['id']
            )
        ]

    def normalize_tag(self, tag):
        """Normalize tag by removing port information.

        Example: changes 8080:http to http
        """
        match = re.match(r'\d*:?(.*):?\d*', tag)
        return match.group(1)

    def tags_of_active_containers(self):
        """Return tags dataframe of all running containers to table.

        with ['container_image_id', 'tag'] columns
        """
        tags_of_active = self.tags[
            self.tags['container_image_id'].isin(
                self.active_containers()['container_image_id']
            )
        ].copy()
        tags_of_active.loc[:, 'tag'] = tags_of_active.tag.apply(
            self.normalize_tag
        )

        return tags_of_active

    def compute_nodes(self):
        """Return nodes dataframe that have role compute.

        will also return nodes having multiple roles
        """
        nodes = self.container_nodes.copy()
        nodes.loc[:, 'instance_type'] = nodes.id.apply(self._get_instance_type)
        nodes.loc[:, 'role'] = nodes.id.apply(self._get_type)

        compute_container_nodes = nodes[nodes["role"].str.contains("compute")]
        return compute_container_nodes

    def _get_instance_type(self, x_container_node_id):
        instance_type = self.instance_types[
            self.instance_types["container_node_id"] == x_container_node_id]

        instance_type_value = ""
        if np.any(instance_type) and instance_type['value'].item():
            instance_type_value = instance_type['value'].item()
        else:
            vm_id = self.container_nodes[
                self.container_nodes['id'] == x_container_node_id
                ]

            if np.any(vm_id) or np.any(self.vms):
                vm = self.vms[self.vms["id"] == vm_id['lives_on_id'].item()]
                if np.any(vm):
                    flavor = self.instance_types[
                        self.instance_types["id"] == vm["flavor_id"].item()]
                    if np.any(flavor):
                        instance_type_value = flavor["source_ref"].item()

        return instance_type_value

    def _get_type(self, x_container_node_id):
        special_type = self.type_roles[
            self.type_roles["container_node_id"] == x_container_node_id]
        if np.any(special_type):
            return special_type['value'].item()

        compute_role = self.compute_roles[
            self.compute_roles["container_node_id"] == x_container_node_id]
        master_role = self.master_roles[
            self.master_roles["container_node_id"] == x_container_node_id]
        infra_role = self.infra_roles[
            self.infra_roles["container_node_id"] == x_container_node_id]

        roles = []
        if np.any(compute_role):
            roles.append("compute")
        if np.any(infra_role):
            roles.append("infra")
        if np.any(master_role):
            roles.append("master")

        return ",".join(roles)

    def _container_identifier(self, container):
        container_group = self.container_groups[
            self.container_groups['id'] == container.container_group_id
        ]
        return "{}/{}".format(
            container_group['source_ref'].item(), container['name']
        )

    def _find_invalid(
            self,
            source_id,
            container_nodes_group,
            tags_of_active_containers
    ):
        """Find invalid containers in a node.

        Find all containers in a node, where recommended instance type is
        different than the current instance_type. The recommendations are
        inserted as a table e.g.:

        instance_type_mappings = {
            'mongodb': 'i3,d1',
            'http,https': 'm3,m4',
        }

        Saying that best instance types for mongo are i3 or d1.

        """
        for _index, container_node in container_nodes_group.iterrows():
            # Fetch pods on the node
            container_groups = self.container_groups[
                self.container_groups['container_node_id']
                == container_node.id]
            # Fetch containers on the node
            containers = self.containers[
                self.containers['container_group_id'].isin(
                    container_groups['id']
                )
            ]

            self._find_invalid_for_node(
                source_id,
                container_node,
                containers,
                tags_of_active_containers
            )

    def _find_invalid_for_node(
            self,
            source_id,
            container_node,
            containers,
            tags_of_active_containers
    ):
        message = {
            "cluster_name":
                self.sources[(self.sources['id'] == source_id)].name.values[0],
            "vm_id": container_node.lives_on_id,
            "unfit_tags": {},
            "unfit_containers": {},
            "messages": {},
        }

        for _index, container in containers.iterrows():
            # Load all image tags of the container
            all_tags = tags_of_active_containers[
                tags_of_active_containers["container_image_id"]
                == container.container_image_id
            ]

            # Skip if container doesn't have any tags
            if not all_tags.any().any():
                continue

            # Take only the unique tags
            tag_values = all_tags['tag'].unique()
            for tags, instance_types in self.instance_type_mappings.items():
                # Finding if there is a recommendation by intersecting the
                # container tags with tags from the recommendation
                matched_tag = np.intersect1d(tag_values, tags.split(","))
                if not matched_tag.any():
                    continue

                current_instance_type = container_node.instance_type

                # Skip if the current instance type matches the recommendation
                if [
                        x for x in instance_types.split(",")
                        if current_instance_type.startswith(x)
                ]:
                    continue

                message['unfit_tags'][tags] = {
                    container_node.instance_type: instance_types
                }
                message['messages'][tags] = (
                    "A host is running tagged applications %(tags)s on "
                    "%(current_instance_type)s but recommended families are"
                    " %(instance_types)s") % locals()

                if not message['unfit_containers'].get(tags):
                    message['unfit_containers'][tags] = []
                message['unfit_containers'][tags].append(
                    self._container_identifier(container)
                )

                # We take just 1st recommendation, in a case container has
                # multiple tags
                break

        if message['unfit_tags']:
            self.result.add_recommendations(
                source_id=source_id,
                message=message
            )
