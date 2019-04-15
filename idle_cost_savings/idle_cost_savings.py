"""Idle Cost Savings interface."""

from collections import defaultdict

import numpy as np
import pandas as pd

import re


class AwsCostSavingsResult:
    """Idle Cost Savings Result."""

    def __init__(self):
        """Initialize value that holds Result."""
        self.cost_savings = defaultdict(defaultdict)
        self.invalid_items = defaultdict(list)
        self.hosts = defaultdict(list)

    def add_recommendations(self, source_id, recommendation_type, message):
        """Append conclusive Result message to Cluser Id.

        Only include clusters that need recommendation.
        """
        if not self.invalid_items[source_id]:
            self.invalid_items[source_id] = defaultdict(list)
        self.invalid_items[source_id][recommendation_type].append(message)

    def set_hosts(self, hosts):
        """Assign per-host with recommendations dict."""
        self.hosts = hosts

    def to_dict(self):
        """Convert Idle Cost Savings Result instance to dict."""
        self.cost_savings['clusters'] = self.invalid_items
        self.cost_savings['hosts'] = self.hosts
        return self.cost_savings


class AwsIdleCostSavings:   #noqa  #Too few public methods
    """Idle Cost Savings."""

    # TODO: #noqa
    # 1. We shouldn't mark reserved instances for shutdown, we'll need data for
    # recognizing reserved instances and maybe the reservation timeline.
    # 2. We need inventory of autoscaling groups, since majority of nodes must
    # be terminated via autoscaling group (otherwise the deleted Vm will just
    # jump back)
    # 4. We should process the project resource quotas and recommend ideal
    # min&max for the autoscaling group
    # 5. We should correlate the measurements with utilization in time (cpu,
    # memory) and recommend to change the requests/limits?

    def __init__(self,
                 dataframes,
                 min_utilization=70.0,
                 max_utilization=80.0):
        """Initialize values required to run idle cost savings."""
        self.result = AwsCostSavingsResult()
        self.container_nodes = dataframes.get('container_nodes')
        self.container_nodes_tags = dataframes.get('container_nodes_tags')
        self.containers = dataframes.get('containers')
        self.container_groups = dataframes.get('container_groups')
        self.container_projects = dataframes.get('container_projects')
        self.container_resource_quotas = dataframes.get(
            'container_resource_quotas'
        )
        self.flavors = dataframes.get('flavors')
        self.vms = dataframes.get('vms')
        self.sources = dataframes.get('sources')

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

        self.zones = self.container_nodes_tags[
            self.container_nodes_tags['name'] ==
            "failure-domain.beta.kubernetes.io/zone"
        ].copy()

        self.min_utilization = min_utilization
        self.max_utilization = max_utilization

    def savings(self):
        """Get Savings for shutting down idle nodes."""
        # Load running containers
        active_containers = self._active_containers()
        # Load nodes having role compute
        compute_nodes = self._compute_nodes()
        # Group nodes by a cluster
        container_nodes_groups = compute_nodes[
            compute_nodes.lives_on_type == 'Vm'].groupby("source_id").groups

        for key in container_nodes_groups.keys():
            # Make recommendation for each cluster
            self._recommend_cost_savings(
                key,
                compute_nodes.loc[container_nodes_groups.get(key, [])],
                active_containers
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
        recommendations = {}

        clusters = self.result.invalid_items.keys()

        for cluster in clusters:
            details = self.result.invalid_items[cluster][0]
            bad_hosts = details['current_state']['nodes']
            bad_host_match = [h for h in bad_hosts if h['id'] == host_id]
            if bad_host_match:
                recommendations['cluster_id'] = cluster
                recommendations['details'] = details

        return recommendations

    def _get_pod_uuid(self, pod_uuid):
        return self.container_groups[
            self.container_groups['id'] == pod_uuid]['source_ref'].item()

    def _get_container_node_id(self, container_node_id):
        return self.container_groups[
            self.container_groups['id'] == container_node_id
        ]['container_node_id'].item()

    def _get_project_name(self, project_name):
        return self.container_groups[
            self.container_groups['id'] == project_name
        ]['container_project_id'].item()

    def _active_containers(self):
        """Return containers that are active/running."""
        containers = self.containers

        containers.loc[:, 'pod_uuid'] = containers.container_group_id.apply(
            self._get_pod_uuid
        )
        containers.loc[
            :, 'container_node_id'] = containers.container_group_id.apply(
                self._get_container_node_id
            )
        containers.loc[
            :, 'project_name'] = containers.container_group_id.apply(
                self._get_project_name
            )
        containers.loc[:, 'cpu_limit_or_request'] = containers.id.apply(
            self._get_container_cpu_limit
        )
        containers.loc[:, 'memory_limit_or_request'] = containers.id.apply(
            self._get_container_memory_limit
        )

        return containers[
            containers['container_group_id'].isin(self.container_groups['id'])
        ]

    def _get_container_cpu_limit(self, container_cpu_limit):
        container = self.containers[
            self.containers.id == container_cpu_limit
        ].iloc[0]

        container_cpu_limit = container.cpu_request
        if container.cpu_limit and not pd.isnull(container.cpu_limit):
            container_cpu_limit = container.cpu_limit
        return container_cpu_limit

    def _get_container_memory_limit(self, container_memory_limit):
        container = self.containers[
            self.containers.id == container_memory_limit
        ].iloc[0]

        container_memory_limit = container.memory_request
        if container.memory_limit and not pd.isnull(container.memory_limit):
            container_memory_limit = container.memory_limit
        return container_memory_limit

    def _get_host_inventory_uuid(self, x_id):
        vm = self.vms[self.vms['id'] == x_id]

        host_inventory_uuid = ""
        if np.any(vm):
            host_inventory_uuid = vm['host_inventory_uuid'].item()
        return host_inventory_uuid

    def _get_host_name(self, x_id):
        vm = self.vms[self.vms['id'] == x_id]

        host_name = ""
        if np.any(vm):
            host_name = vm['name'].item()
        return host_name

    def _get_allocatable_memory_gb(self, x_memory):
        return x_memory / 1024 ** 3

    def _get_external_id(self, x):
        vm = self.vms[self.vms['id'] == x]
        if np.any(vm):
            return vm['source_ref'].item()

    def _get_zone(self, x):
        zone = self.zones[self.zones["container_node_id"] == x]
        if np.any(zone):
            return zone['value'].item()

    def _get_amount_of_pods(self, amount_of_pods):
        return len(self.container_groups[
            self.container_groups['container_node_id'] == amount_of_pods])

    def _compute_nodes(self):
        """Return nodes dataframe that have role compute.

        will also return nodes having multiple roles.
        """
        nodes = self.container_nodes.copy()
        nodes.loc[:, 'instance_type'] = nodes.id.apply(self._get_instance_type)
        nodes.loc[:, 'zone'] = nodes.id.apply(self._get_zone)
        nodes.loc[:, 'external_id'] = nodes.id.apply(self._get_external_id)
        nodes.loc[:, 'role'] = nodes.id.apply(self._get_type)
        nodes.loc[:, 'flavor_cpus'] = nodes.instance_type.apply(
            self._get_flavor_cpu
        )
        nodes.loc[:, 'flavor_memory'] = nodes.instance_type.apply(
            self._get_flavor_memory
        )

        nodes.loc[:, 'no_of_pods'] = nodes.id.apply(
            self._get_amount_of_pods
        )

        nodes.loc[:, 'host_inventory_uuid'] = nodes.lives_on_id.apply(
            self._get_host_inventory_uuid
        )

        nodes.loc[:, 'host_name'] = nodes.lives_on_id.apply(
            self._get_host_name
        )

        nodes.loc[:, 'allocatable_memory_gb'] = nodes.allocatable_memory.apply(
            self._get_allocatable_memory_gb
        )

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

    def _get_flavor_cpu(self, x_flavor_name):
        if not np.any(self.flavors):
            return None

        cpu_count = self.flavors[
            self.flavors['cpus'].notnull() &
            (self.flavors['name'] == x_flavor_name)
        ]

        flavor_cpu = None
        if np.any(cpu_count):
            flavor_cpu = cpu_count.iloc[0]['cpus'].item()
        return flavor_cpu

    def _get_flavor_memory(self, x_flavor_name):
        if not np.any(self.flavors):
            return None

        memory = self.flavors[
            self.flavors['memory'].notnull() &
            (self.flavors['name'] == x_flavor_name)
        ]

        flavor_memory = None
        if np.any(memory):
            flavor_memory = memory.iloc[0]['memory'].item()
        return flavor_memory

    def _cpu_utilization(self, container_nodes_group, containers):
        available = container_nodes_group['allocatable_cpus'].sum(
            axis=0,
            skipna=True
        )
        # TODO use request_or_limit, in a case that has only limit defined #noqa
        consumed = containers['cpu_request'].sum(
            axis=0,
            skipna=True
        )

        cpu_utilization = 0
        if available > 0:
            cpu_utilization = 100.0/available*consumed
        return cpu_utilization

    def _memory_utilization(self, container_nodes_group, containers):
        available = container_nodes_group['allocatable_memory'].sum(
            axis=0,
            skipna=True
        )
        consumed = containers['memory_limit_or_request'].sum(
            axis=0,
            skipna=True
        )

        memory_utilization = 0
        if available > 0:
            memory_utilization = 100.0/available*consumed
        return memory_utilization

    def _pods_utilization(self, remaining_nodes, all_nodes):
        available = remaining_nodes['allocatable_pods'].sum(
            axis=0,
            skipna=True
        )
        consumed = all_nodes['no_of_pods'].sum(
            axis=0,
            skipna=True
        )

        pods_utilization = 0
        if available > 0:
            pods_utilization = 100.0/available*consumed
        return pods_utilization

    def _utilization(self, remaining_nodes, all_nodes, containers):
        cpu_utilization = self._cpu_utilization(
            remaining_nodes,
            containers
        )
        memory_utilization = self._memory_utilization(
            remaining_nodes,
            containers
        )
        pods_utilization = self._pods_utilization(
            remaining_nodes,
            all_nodes
        )

        return max([cpu_utilization, memory_utilization, pods_utilization])

    def _format_node_list(self, nodes, algorithm=None):
        if not algorithm:
            return nodes.loc[:, [
                                    "id",
                                    "external_id",
                                    "zone",
                                    "host_inventory_uuid",
                                    "host_name",
                                    "allocatable_memory",
                                    "allocatable_memory_gb",
                                    "allocatable_cpus",
                                    "allocatable_pods",
                                    "no_of_pods"
                                ]].sort_values(
                by='no_of_pods',
                ascending=True
            )
        elif algorithm == "instance_type_cost_savings":
            return nodes.loc[:, [
                                    "id",
                                    "external_id",
                                    "zone",
                                    "host_inventory_uuid",
                                    "host_name",
                                    "instance_type",
                                    "no_of_pods"
                                ]].sort_values(
                by='no_of_pods',
                ascending=True
            )

    def _store_recommendation(
            self,
            source_id,
            all_nodes,
            remaining_nodes,
            containers
    ):
        cpu_utilization = self._cpu_utilization(all_nodes, containers)
        memory_utilization = self._memory_utilization(all_nodes, containers)
        pods_utilization = self._pods_utilization(all_nodes, all_nodes)

        optimized_cpu_utilization = self._cpu_utilization(
            remaining_nodes,
            containers
        )
        optimized_memory_utilization = self._memory_utilization(
            remaining_nodes,
            containers
        )
        optimized_pods_utilization = self._pods_utilization(
            remaining_nodes,
            all_nodes
        )

        shut_off_nodes = all_nodes[
            ~all_nodes['id'].isin(remaining_nodes['id'])]

        message = {}
        message['cluster_name'] = \
            self.sources[(self.sources['id'] == source_id)].name.values[0]
        message['message'] = \
            "For saving cost we can scale down nodes in a cluster"
        message['current_state'] = {
            "cpu_utilization": cpu_utilization,
            "memory_utilization": memory_utilization,
            "pods_utilization": pods_utilization,
            "nodes": self._format_node_list(all_nodes).to_dict('records')
        }
        message['after_scaledown'] = {
            "cpu_utilization": optimized_cpu_utilization,
            "memory_utilization": optimized_memory_utilization,
            "pods_utilization": optimized_pods_utilization,
            "nodes": self._format_node_list(remaining_nodes).to_dict('records')
        }
        message['recommended_nodes_for_shut_down'] = \
            self._format_node_list(shut_off_nodes).to_dict('records')

        self.result.add_recommendations(
            source_id,
            'idle_cost_savings',
            message
        )

    def _recommend_cost_savings(
            self,
            source_id,
            container_nodes_group,
            active_containers
    ):
        """Cost savings recommendations."""
        containers = \
            active_containers[
                active_containers["container_node_id"].isin(
                    container_nodes_group['id']
                )
            ]

        self._recommend_idle_cost_savings(
            source_id, container_nodes_group, containers
        )

        self._recommend_instance_type_cost_savings(
            source_id, container_nodes_group
        )

    def _recommend_idle_cost_savings(
            self,
            source_id,
            container_nodes_group,
            active_containers
    ):
        """Recommend nodes that can be shutoff."""
        containers = \
            active_containers[
                active_containers["container_node_id"].isin(
                    container_nodes_group['id']
                )
            ]

        if self._utilization(container_nodes_group, container_nodes_group,
                             containers) >= self.min_utilization:
            self.result.add_recommendations(source_id, 'idle_cost_savings', {})
            return

        shut_off_nodes = []

        for _index, node in container_nodes_group.sort_values(
                by='no_of_pods',
                ascending=True
        ).iterrows():

            # Look at the utilization if we shut off one more node
            shut_off_nodes.append(node.id)
            nodes = container_nodes_group[
                ~container_nodes_group['id'].isin(shut_off_nodes)
            ]
            utilization = self._utilization(
                nodes,
                container_nodes_group,
                containers
            )

            if utilization >= self.max_utilization:
                # We've removed too much nodes, util is over 100% now, lets
                # put back the last remove node and we should be in ideal state
                del shut_off_nodes[-1]
                nodes = container_nodes_group[
                    ~container_nodes_group['id'].isin(shut_off_nodes)
                ]

                self._store_recommendation(
                    source_id,
                    container_nodes_group,
                    nodes,
                    containers
                )

                return

            if len(nodes) <= 1:
                # We have only last node left, lets just keep that
                self._store_recommendation(
                    source_id,
                    container_nodes_group,
                    nodes,
                    containers
                )
                return

            if utilization >= self.min_utilization:
                # Utilization is over what we've specified, lets recommend
                # this state.
                self._store_recommendation(
                    source_id,
                    container_nodes_group,
                    nodes,
                    containers
                )
                return

    def _recommend_instance_type_cost_savings(
            self,
            source_id,
            container_nodes_group
    ):
        """Recommend instance type changes."""
        groups = container_nodes_group.groupby(
            ['zone', 'instance_type']
        ).groups

        for key in groups.keys():
            # Make recommendation for each cluster
            zone, instance_type = key
            nodes = container_nodes_group.loc[groups.get(key, [])]
            nodes_count = len(nodes)

            # instance_type = nodes.iloc[0].instance_type
            instance_type_family, multiplier = self._parse_instance_type(
                instance_type
            )

            if not instance_type_family:
                # Instance type family that is not supported now or not
                # recognized
                continue

            # Filter all flavors of a given family, but only ones that have
            # large name
            pattern = "^{}.".format(instance_type_family)
            filtered_flavors = self.flavors[
                self.flavors['cpus'].notnull() &
                (self.flavors['name'].str.contains(pattern)) &
                (self.flavors['name'].str.contains(".*?large$"))]['name']

            indexed_flavors = {}
            for flavor in filtered_flavors:
                _f, m = self._parse_instance_type(flavor)
                indexed_flavors[m] = flavor

            self._store_instance_type_recommendation(
                source_id,
                zone,
                nodes,
                indexed_flavors,
                instance_type,
                multiplier,
                nodes_count
            )

    def _parse_instance_type(self, instance_type):
        # We support only large instances now
        match = re.match(r'(.+?)\.(\d*)(x?)large', instance_type)
        if not match:
            return None, None

        instance_type_family = match.group(1)
        instance_type_multiplier = match.group(2)
        instance_type_x = match.group(3)

        if len(instance_type_x) == 0:
            multiplier = 1
        elif len(instance_type_x) > 0 and len(instance_type_multiplier) == 0:
            multiplier = 2
        else:
            multiplier = int(instance_type_multiplier) * 2

        return instance_type_family, multiplier

    def _store_instance_type_recommendation(
            self,
            source_id,
            zone,
            nodes,
            indexed_flavors,
            instance_type,
            multiplier,
            nodes_count
    ):
        """Store Instance Type Recommendation.

        Example: having m4.2xlarge the multiplier will be 4
            and indexed_flavors will be {8: 'm4.4xlarge',
            2: 'm4.xlarge', 1: 'm4.large', 32: 'm4.16xlarge',
            20: 'm4.10xlarge', 4: 'm4.2xlarge'}
        Take all flavors bigger than multiplier, then divide the value
        by multipliers.
        then 4xlarge will become 2
        then if number of nodes is divisible by 2, we can convert them
        all to 4xlarge.
        Leading to recommendation six m4.2xlarge can be turned to three
        m4.4xlarge or one m4.16xlarge.
        """
        recommendations = [
            [int(nodes_count / (key / multiplier)), value]
            for key, value in indexed_flavors.items() if
            key > multiplier and  # Only for instance_types bigger than current
            (key % multiplier) == 0 and
            nodes_count % (key / multiplier) == 0]

        if len(recommendations) == 0:
            self.logger.debug("No applicable combination found to recommend.")
            self.result.add_recommendations(
                source_id,
                'instance_type_cost_savings', {}
            )
            return

        formated_recommendations = []
        for key, value in recommendations:
            formated_recommendations.append({
                'instance_type_count': key,
                'instance_type_name': value,
                'message': "{} nodes with instance type '{}' can be replaced"
                           " with {} nodes with instance type '{}'.".format(
                                nodes_count, instance_type, key, value
                            )
            })

        message = {
            "message": "We can use bigger instance types, which can lead to "
                       "cost reduction.",
            "availability_zone": zone,
            "instance_type": instance_type,
            "recommended_nodes_for_shut_down": {
                "nodes": self._format_node_list(
                    nodes, "instance_type_cost_savings").to_dict('records')
            },
            "recommended_new_instance_types": formated_recommendations
        }
        self.result.add_recommendations(
            source_id,
            'instance_type_cost_savings',
            message
        )
