import csv
import psycopg2
import pdb


def filter_list(thelist, id_score_map, group_flags):
    cluster = []
    for k in thelist:
        if k in id_score_map.keys() and group_flags[thelist.index(k)] == 1:
            cluster.append(k)
    return cluster


def make_changes(thedict, id_group_map, group_flags, pivot):
    del1 = -1
    del2 = -1
    min1 = 1
    for key in thedict.keys():
        if thedict[key][1] == 1 and thedict[key][0] > del1:
            del1 = thedict[key][0]
            ele1 = key
        if thedict[key][1] == 0 and thedict[key][0] > del2:
            del2 = thedict[key][0]
            ele2 = key
        if thedict[key][1] == 1 and thedict[key][0] < min1:
            ele = key
            min1 = thedict[key][0]
    if del2 == -1 and len(thedict.keys()) > 1:
        for key in thedict.keys():
            if key != ele:
                group_flags[key][id_group_map[key].index(pivot)] = abs(
                    group_flags[key][id_group_map[key].index(pivot)] - 1)
        return 1
    else:
        if (del1 + del2) > 0:
            group_flags[ele1][id_group_map[ele1].index(pivot)] = abs(
                group_flags[ele1][id_group_map[ele1].index(pivot)] - 1)
            group_flags[ele2][id_group_map[ele2].index(pivot)] = abs(
                group_flags[ele2][id_group_map[ele2].index(pivot)] - 1)
            return 2
        return 0


# Compute cluster sum with or without pivot element
def cluster_stability(thelist, id_score_map, group_flags, pivot):
    sum = 0
    for k in thelist:
        if k in id_score_map.keys() and group_flags[thelist.index(k)] == 1:
            sum = sum + id_score_map[k]
    sum_delta = abs(round(sum) - sum)

    if group_flags[thelist.index(pivot)] == 1:
        sum = sum - id_score_map[pivot]
    else:
        sum = sum + id_score_map[pivot]
    return sum_delta - abs(round(sum) - sum)


def cluster_sum(thelist, id_score_map, group_flags):
    sum = 0
    for k in thelist:
        if k in id_score_map.keys() and group_flags[thelist.index(k)] == 1:
            sum = sum + id_score_map[k]
    return sum


def right_clusters():
    with open('rohilla_scores.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        cluster_sets = []
        setsum = 0
        right_cluster = dict()
        id_score_map = dict()
        id_group_map = dict()
        group_flags = dict()
        for row in spamreader:
            id_score_map[int(row[0])] = float(row[1])
        print "Total DC's available ", sum(id_score_map.values())
        con = psycopg2.connect(
            "dbname='housing_analytics' user='dsl_readonly' password='dsl' host=127.0.0.1 port = 5436")
        cur = con.cursor()
        cur.execute(
            'set search_path to public,postgis; select lo1.id,array_agg(lo2.id) from (select  id,boundary_polygon from localities where region_id = 2) lo1 cross join (select id, boundary_polygon from localities where region_id = 2) lo2  where  st_intersects(lo1.boundary_polygon, lo2.boundary_polygon) group by lo1.id;')
        data = cur.fetchall()

        for each in data:
            id_group_map[each[0]] = each[1]  # data in form of L1 G1, L2 G2 ..
            right_cluster[each[0]] = dict()
            group_flags[each[0]] = [1] * len(each[1])
        converge = 1
        before_converge = 0
        while converge:  # > before_converge:
            before_converge = converge
            converge = 0
            for each in data:
                    # Cluster Stability Problem
                for ele in each[1]:   # for each element in G1
                    if each[0] in id_score_map.keys():
                        if group_flags[ele][id_group_map[ele].index(each[0])] == 1:
                            right_cluster[each[0]][ele] = [
                                cluster_stability(id_group_map[ele], id_score_map, group_flags[ele], each[0]), 1]
                        else:
                            right_cluster[each[0]][ele] = [
                                cluster_stability(id_group_map[ele], id_score_map, group_flags[ele], each[0]), 0]
                converge = converge + \
                    make_changes(
                        right_cluster[each[0]], id_group_map, group_flags, each[0])
            print converge
            # pdb.set_trace()
    for ele in id_group_map.keys():
        csum = cluster_sum(id_group_map[ele], id_score_map, group_flags[ele])
        if csum > 0:
            cluster = filter_list(
                id_group_map[ele], id_score_map, group_flags[ele])
            print cluster, round(csum)
            cluster_sets = cluster_sets + cluster
            setsum = setsum + round(csum)
    setunion = set(cluster_sets)
    print "Total DC's allocated ", setsum
   # print setunion
   # pdb.set_trace()
right_clusters()
