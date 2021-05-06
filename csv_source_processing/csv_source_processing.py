import csv
from functools import reduce
from functools import partial

SOURCES_FILE_PATH = "data/sources.csv"
IPA_GROUPS_FILE_PATH = "data/ipa_groups.csv"
REWRITTEN_GROUPS_FILE_PATH = "data/functional_subsystem_group.csv"
ACCESS_RIGHTS_DICT = { 1: 'RO', 2: 'RW', 3: 'RWA', 4: 'RWXCA', 5: 'RWX', 6: 'RX' }
LS_PREFIXES = ['3ls', '4ls']
USER_ACCESS_TYPE = 'USER'
TUZ_ACCESS_TYPE = 'TUZ'
TUZ_USER_ACCESS_TYPE = 'TUZ_USER'

def reduce_sources(target_row, source_dict, row_entity):
    source_dict[row_entity[0]] = row_entity[target_row]
    return source_dict

def process_sources(reduce_function):
    with open(SOURCES_FILE_PATH, 'r', encoding="utf8") as sources:
        content = csv.reader(sources)
        return reduce(reduce_function, content, {})

def get_source_dict():
    reduce_function = partial(reduce_sources, 3)
    return process_sources(reduce_function)

def get_tuz_name_dict():
    reduce_function = partial(reduce_sources, 4)
    return process_sources(reduce_function)

def get_support_groups_affinity(group_template):
    is_ls = any(ls_prefix in group_template for ls_prefix in LS_PREFIXES)
    is_pkd = 'pkd' in group_template
    return [is_ls, is_pkd]

def get_access_group_prefix(access_rights, group_template):
    support_groups_affinity = get_support_groups_affinity(group_template)
    get_group_is_in_special_groups = support_groups_affinity[0] or support_groups_affinity[1]
    if access_rights == 'RW' or get_group_is_in_special_groups:
        return 'owner'
    return 'consumer'

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

def get_tuz_name(tuz_name_from_source, group_template):
    if any(ls_prefix in group_template for ls_prefix in LS_PREFIXES):
        return ''
    if 'pkd' in group_template:
        return 'u_<cluster_prefix>_s_internal_qa'
    return tuz_name_from_source

def get_access_type(access_rights, group_template):
    is_ro = access_rights == 'RO'
    support_groups_affinity = get_support_groups_affinity(group_template)
    if support_groups_affinity[0]:
        return USER_ACCESS_TYPE
    if support_groups_affinity[1]:
        return TUZ_ACCESS_TYPE
    if is_ro:
        return TUZ_USER_ACCESS_TYPE
    return TUZ_ACCESS_TYPE

def get_consumer_tuz_allowed(access_type, has_obsolete_union_tuz_name):
    if access_type == USER_ACCESS_TYPE:
        return 'false'
    if access_type == TUZ_USER_ACCESS_TYPE:
        return 'true'
    if not has_obsolete_union_tuz_name:
        return 'true'
    return 'false'


if __name__ == '__main__':
    sources = get_source_dict()
    tuz_names = get_tuz_name_dict()
    print(sources)
    print(tuz_names)


    def reduce_groups(reduced_groups_cumulative, row_entity):
        reduced_groups_list = []
        source_id = row_entity[4]
        obsolete_group_template = row_entity[2]
        obsolete_tuz_name = tuz_names[source_id]
        access_rights = get_access_rights(row_entity[1])
        access_type = get_access_type(access_rights, obsolete_group_template)
        # id -> id
        reduced_groups_list.append(row_entity[0])
        # source_id -> fs_id
        reduced_groups_list.append(source_id)
        # hdfs_group_id -> hdfs_group_id
        reduced_groups_list.append(row_entity[5])
        # access_rights -> access_group_prefix
        reduced_groups_list.append(get_access_group_prefix(access_rights, obsolete_group_template))
        # access_rights -> access_rights
        reduced_groups_list.append(access_rights)
        # group_template -> group_template
        reduced_groups_list.append(get_group(obsolete_group_template))
        # sentry_role_template -> has_role
        reduced_groups_list.append(get_role(row_entity[3]))
        # account_template -> tuz_name
        reduced_groups_list.append(get_tuz_name(obsolete_tuz_name, obsolete_group_template))
        # generate access_type
        reduced_groups_list.append(access_type)
        # get is consumer tuz allowed
        reduced_groups_list.append(get_consumer_tuz_allowed(access_type, bool(obsolete_tuz_name)))
        # print(reduced_groups_list)
        reduced_groups_cumulative.append(reduced_groups_list)
        return reduced_groups_cumulative

    with open(IPA_GROUPS_FILE_PATH, 'r', encoding="utf8") as ipa_groups:
        obsolete_ipa_groups = csv.reader(ipa_groups)
        reduced_groups_cumulative = [['id', 'fs_id', 'hdfs_group_id', 'access_group_prefix', 'access_rights', 'group_template', 'has_role', 'tuz_name', 'access_type', 'consumer_tuz_allowed']]
        reduced_groups_cumulative = reduce(reduce_groups, obsolete_ipa_groups, reduced_groups_cumulative)
        with open(REWRITTEN_GROUPS_FILE_PATH, 'w+', encoding='utf-8', newline='') as new_file:
            writer = csv.writer(new_file, delimiter=',')
            for line in reduced_groups_cumulative:
                writer.writerow(line)