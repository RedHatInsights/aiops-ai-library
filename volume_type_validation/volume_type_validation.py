"""AWS Volume Type Validation interface."""
import operator

from collections import defaultdict


class VolumeTypeValidationResult:
    """Validation Result Processing."""

    def __init__(self):
        """Initialize value that holds Result."""
        self.volume_type_results = defaultdict(defaultdict)
        self.invalid_items = defaultdict(list)
        self.hosts = defaultdict(list)

    def add_recommendations(self, source_id, message):
        """Append conclusive Result message to Cluser Id. Only include wrong clusters"""
        self.invalid_items[source_id].append(message)

    def set_hosts(self, hosts):
        """Assign per-host with recommendations dict."""
        self.hosts = hosts

    def to_dict(self):
        """Convert Volume Type Results instance to dict."""
        self.volume_type_results['clusters'] = self.invalid_items
        self.volume_type_results['hosts'] = self.hosts
        return self.volume_type_results


class AwsVolumeTypeValidation:  #noqa for R0903 Too few public methods
    """Validation of AWS Volume Types."""

    def __init__(self, dataframes):
        """Initialize values required to run validation."""
        self.result = VolumeTypeValidationResult()
        self.container_nodes = dataframes.get('container_nodes')
        self.container_nodes_tags = dataframes.get('container_nodes_tags')
        self.volume_attachments = dataframes.get('volume_attachments')
        self.volumes = dataframes.get('volumes')
        self.vms = dataframes.get('vms')
        self.volume_types = dataframes.get('volume_types')
        self.sources = dataframes.get('sources')

    def validate(self):
        """Validate Volume Types."""
        container_nodes_groups = \
            self.container_nodes.groupby("source_id").groups

        for key in container_nodes_groups.keys():
            self._find_invalid_in_groups(
                key,
                container_nodes_groups.get(key, [])
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

    def _find_invalid_in_groups(self, source_id, group_ids):
        lives_on_vm = self.container_nodes.lives_on_type == 'Vm'
        container_nodes_group = \
            self.container_nodes.loc[group_ids][lives_on_vm]

        container_nodes_taggings = self.container_nodes_tags

        type_taggings = \
            container_nodes_taggings[container_nodes_taggings['name'] ==
                                     "type"].copy()

        if type_taggings.empty:
            self._find_invalid(source_id, container_nodes_group)
            return
        # TODO For master nodes, it can happen
        # that the nodes
        # running etcd have io1 and the rest
        # gp2. Lets ignore these for now, until we can
        # identify nodes running etcd
        non_master_nodes_ids = \
            type_taggings[type_taggings['value'] !=
                          "master"]['container_node_id']
        self._find_invalid(
            source_id,
            container_nodes_group[container_nodes_group['id'].isin(non_master_nodes_ids)]
        )

    def _find_invalid(self, source_id, container_nodes_group):
        # Select container nodes of 1 group, but only those deployed on Vm
        vms_ids = container_nodes_group['lives_on_id']

        # Select volume attachments of these container nodes
        group_volume_attachments = \
            self.volume_attachments[self.volume_attachments['vm_id'].isin(vms_ids)]
        group_volume_attachments_ids = group_volume_attachments["volume_id"]

        # Select volumes of these volume attachments
        group_volumes = \
            self.volumes[self.volumes['id'].isin(group_volume_attachments_ids)]

        # Asserts
        self._find_invalid_volume_types(source_id, group_volumes, group_volume_attachments, container_nodes_group)

    def _find_invalid_volume_types(
            self,
            source_id,
            group_volumes,
            group_volume_attachments,
            container_nodes_group
    ):
        # volume_type_id of the whole group of volumes should be the same, if not add error message
        if not len(group_volumes['volume_type_id'].unique()) > 1:
            return
        # Find what is recommended volume type, it will be the one used by majority of nodes
        volume_type_groups = group_volumes.groupby('volume_type_id').groups
        volume_type_groups_sizes = \
            [[key, len(group)] for key, group in volume_type_groups.items()]
        recommended_volume_type, recommended_volume_type_size = \
            max(volume_type_groups_sizes, key=operator.itemgetter(1))

        same_sizes = [key for key, value in volume_type_groups_sizes if value == recommended_volume_type_size]

        message = {}
        message['cluster_name'] = self.sources[(self.sources['id'] == source_id)].name.values[0]
        message['description'] = "Cluster contains one or more instances " \
                                 "with different volume types. "
        message['recommendations'] = []

        if len(same_sizes) > 1:
            message['description'] += "There are more alternative recommendations, " \
                                  "because there isn't a majority of volume types used."

        for recommended_volume_type in same_sizes:
            recommendation = {}
            bad_volumes, bad_hosts = self._find_bad_hosts(recommended_volume_type,
                                                          group_volumes,
                                                          group_volume_attachments,
                                                          container_nodes_group)

            recommendation['recommended_volume_type'] = \
                self._volume_type_id_to_str(recommended_volume_type)
            recommendation['wrong_volume_type_vms'] = bad_hosts
            recommendation['wrong_volume_type_volumes'] = bad_volumes
            message['recommendations'].append(recommendation)

        self.result.add_recommendations(source_id=str(source_id),
                                        message=message)

    def _find_bad_hosts(
            self,
            recommended_volume_type,
            group_volumes,
            group_volume_attachments,
            container_nodes_group
    ):
        # Indentify volumes and their hosts that should change the volume type
        bad_volumes = \
            group_volumes[group_volumes['volume_type_id'] != recommended_volume_type]
        bad_volume_attachments = \
            group_volume_attachments[group_volume_attachments['volume_id'].isin(bad_volumes['id'])]
        bad_nodes = \
            container_nodes_group[container_nodes_group['lives_on_id'].isin(bad_volume_attachments['vm_id'])]
        bad_hosts = self.vms[self.vms['id'].isin(bad_nodes['lives_on_id'])]

        bad_hosts = bad_hosts[['id', 'name', 'source_ref']].to_dict('records')
        bad_volumes = self._enrich_with_volume_type(
            bad_volumes[['id', 'name', 'source_ref', 'volume_type_id']].to_dict('records'))

        bad_volumes_ids = [v['id'] for v in bad_volumes]

        bad_vm_to_bad_volume_connection = \
            group_volume_attachments.loc[group_volume_attachments['volume_id'].isin(bad_volumes_ids)]

        for bad_volume in bad_volumes:
            bad_volume['vm_id'] = \
                bad_vm_to_bad_volume_connection.loc[bad_vm_to_bad_volume_connection['volume_id'] ==
                    bad_volume['id']].vm_id.values[0]


        return bad_volumes, bad_hosts

    def _enrich_with_volume_type(self, items):
        for value in items:
            value['volume_type'] = \
                self._volume_type_id_to_str(value['volume_type_id'])
        return items

    def _recommendations(self, host_id):
        recommendations = []

        for cluster in self.result.invalid_items.keys():
            for recommendation in self.result.invalid_items[cluster][0]['recommendations']:
                bad_hosts = recommendation['wrong_volume_type_vms']
                bad_volumes = recommendation['wrong_volume_type_volumes']
                bad_host_match = [h for h in bad_hosts if h['id'] == host_id]
                if not bad_host_match:
                    continue
                recommended_volume_type = recommendation['recommended_volume_type']
                cluster_name = self.result.invalid_items[cluster][0]['cluster_name']
                recommendations.append(
                    {
                        "cluster_id": cluster,
                        "cluster_name": cluster_name,
                        "recommended_volume_type": recommended_volume_type,
                        "volume_info": self._volumes_for_host(host_id, bad_volumes)
                    }
                )

        return recommendations

    def _volumes_for_host(self, host_id, bad_volumes):
        return [v for v in bad_volumes if v['vm_id'] == host_id]

    def _volume_type_id_to_str(self, volume_type_id):
        return self.volume_types[self.volume_types['id'] ==
                                 volume_type_id].to_dict('records')[0]['source_ref']
