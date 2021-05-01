import csv
from functools import reduce


SOURCES_FILE_PATH = "data/sources.csv"
IPA_GROUPS_FILE_PATH = "data/ipa_groups.csv"
REWRITTEN_GROUPS_FILE_PATH = "data/functional_subsystem_group.csv"


ACCESS_RIGHTS_DICT = { 1: 'RO', 2: 'RW', 3: 'RWA', 4: 'RWXCA', 5: 'RWX', 6: 'RX' }


def get_source_dict():
    def reduce_sources(source_dict, row_entity):
        source_dict[row_entity[0]] = row_entity[3]
        return source_dict

    with open(SOURCES_FILE_PATH, 'r', encoding="utf8") as sources:
        content = csv.reader(sources)
        return reduce(reduce_sources, content, {})


def get_access_group_prefix(access_rights_id):
    if access_rights_id == '1':
        return 'consumer'
    return 'owner'

def get_access_rights(access_rights_id):
    return ACCESS_RIGHTS_DICT.get(int(access_rights_id))

def get_group(obsolete_group):
    def get_subsystem_type_group_index(group, type):
        try:
            return group.index("_%s_" % type)
        except ValueError:
            return -1

    def get_access_rights_index(group, access_rights):
        try:
            return group.index("_%s" % access_rights.lower())
        except ValueError:
            return -1

    def get_group_without_prefixes(group):
        indexes = [
            get_subsystem_type_group_index(group, 'd'),
            get_subsystem_type_group_index(group, 'a'),
            get_subsystem_type_group_index(group, 's'),
            get_subsystem_type_group_index(group, 'h')
        ]
        index = list(filter(lambda a: a > -1, indexes))[0]
        return group[index + 3:]

    def get_group_without_access_rights(group):
        access_rights_values = ACCESS_RIGHTS_DICT.values()
        indexes = map(lambda ar: get_access_rights_index(group, ar), access_rights_values)
        filtered = list(filter(lambda a: a > -1, indexes))
        if len(filtered):
            return group[:filtered[0]]
        return group
    group_without_prefixes = get_group_without_prefixes(obsolete_group)
    group_without_access_rights = get_group_without_access_rights(get_group_without_access_rights(group_without_prefixes))
    print(group_without_access_rights)
    return group_without_access_rights


def get_role(obsolete_role):
    if obsolete_role:
        return 'true'
    return 'false'

if __name__ == '__main__':
    sources = get_source_dict()


    def reduce_groups(reduced_groups_cumulative, row_entity):
        reduced_groups_list = []
        # id -> id
        reduced_groups_list.append(row_entity[0])
        # hdfs_group_id -> hdfs_group_id
        reduced_groups_list.append(row_entity[5])
        # access_rights -> access_group_prefix
        reduced_groups_list.append(get_access_group_prefix(row_entity[1]))
        # source_id -> fs_prefix
        reduced_groups_list.append(sources[row_entity[4]])
        # access_rights -> access_rights
        reduced_groups_list.append(get_access_rights(row_entity[1]))
        # group_template -> group_template
        reduced_groups_list.append(get_group(row_entity[2]))
        # sentry_role_template -> has_role
        reduced_groups_list.append(get_role(row_entity[3]))
        # print(reduced_groups_list)
        reduced_groups_cumulative.append(reduced_groups_list)
        return reduced_groups_cumulative

    with open(IPA_GROUPS_FILE_PATH, 'r', encoding="utf8") as ipa_groups:
        obsolete_ipa_groups = csv.reader(ipa_groups)
        reduced_groups_cumulative = [['id', 'hdfs_group_id', 'access_group_prefix', 'fs_prefix', 'access_rights', 'group_template', 'has_role', 'tuz_name', 'access_type']]
        reduced_groups_cumulative = reduce(reduce_groups, obsolete_ipa_groups, reduced_groups_cumulative)
        with open(REWRITTEN_GROUPS_FILE_PATH, 'w+', encoding='utf-8', newline='') as new_file:
            writer = csv.writer(new_file, delimiter=',')
            for line in reduced_groups_cumulative:
                writer.writerow(line)