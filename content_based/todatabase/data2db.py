import re
from content_based.db_client import dbclient
from pprint import pprint
import content_based.utils as utils
import sys
import os

project_root_path = sys.path[1]


def douban_type_1():
    video_list = []
    query = """
            WITH {batch} as video_list
            UNWIND video_list AS video
            MERGE (m:Video {name: video.name, category: video.category})
            ON CREATE SET m.score = video.score, m.source = 'xx', m.releaseDate = video.releaseDate
            ON MATCH SET m.score = video.score, m.releaseDate = video.releaseDate
            FOREACH (actor IN video.actor | MERGE(a:Actor {name: actor}) MERGE (m) <-[:ACTED_IN]- (a) )
            FOREACH (director IN video.director | MERGE(d:Director {name:director}) MERGE (d) -[:DIRECTED]-> (m) )
            FOREACH (tag IN video.plot | MERGE(t:Plot {name: tag}) MERGE (m) -[:IN_GENRE]-> (t) )
            FOREACH (region in video.region | MERGE(r:Region {name: region}) MERGE (m) -[:IN_REGION]-> (r) )
            """
    file_path = os.path.join(project_root_path, 'content_based/data/douban/video_douban_1.csv')
    for idx, line in enumerate(open(file_path, 'r', encoding='utf-8')):
        if idx == 0:
            continue
        doc = line.strip().split(',')
        v_name = doc[3]
        v_director = utils.clean_d_a_p(doc[4])
        v_actor = utils.clean_d_a_p(doc[6])
        v_plot = utils.clean_d_a_p(doc[7])
        v_region = utils.clean_region(doc[8])
        v_releaseDate = utils.clean_releaseDate(doc[10])
        v_other_name = doc[14]
        v_score = utils.clean_score(doc[15])

        cur_video = {
                     'name': v_name,
                     'category': '电影',
                     'director': v_director,
                     'actor': v_actor,
                     'plot': v_plot,
                     'region': v_region,
                     'releaseDate': v_releaseDate,
                     'score': v_score,
                     }

        video_list.append(cur_video)

        if idx % 1000 == 0:
            dbclient.run(query, batch=video_list)
            # pprint(video_list)
            print("---已存入{}部节目信息---".format(idx))
            video_list = []
    if len(video_list) != 0:
        dbclient.run(query, batch=video_list)
        print("---已存入全部节目信息---")


def douban_type_2():
    video_list = []
    query = """
            WITH {batch} as video_list
            UNWIND video_list AS video
            MERGE (m:Video {name: video.name, category: video.category}) 
            ON CREATE SET m.score = video.score, m.source = 'xx' 
            ON MATCH SET m.score = video.score 
            FOREACH (actor IN video.actor | MERGE(a:Actor {name: actor}) MERGE (m) <-[:ACTED_IN]- (a) )
            FOREACH (director IN video.director | MERGE(d:Director {name:director}) MERGE (d) -[:DIRECTED]-> (m) )
            FOREACH (tag IN video.plot | MERGE(t:Plot {name: tag}) MERGE (m) -[:IN_GENRE]-> (t) )
            FOREACH (region in video.region | MERGE(r:Region {name: region}) MERGE (m) -[:IN_REGION]-> (r) )
            """
    file_path = os.path.join(project_root_path, 'content_based/data/douban/video_douban_2.csv')

    for idx, line in enumerate(open(file_path, 'r', encoding='utf-8')):
        if idx == 0:
            continue
        doc = line.strip().split('#')

        v_source = doc[0]
        v_name = doc[1].split(' ')[0]
        v_othername = doc[2]
        v_category = doc[3]
        v_time = doc[4]
        v_plot = utils.clean_d_a_p(doc[5])
        v_num = doc[6]
        v_director = utils.clean_d_a_p(doc[7])
        v_actor = utils.clean_d_a_p(doc[8])
        v_tencentscore = doc[9]
        v_doubanscore = doc[10]
        v_screenwriter = doc[11]
        v_region = utils.clean_region(doc[12])
        v_language = doc[13]
        v_seasontime = doc[14]
        v_tags = doc[15]

        cur_video = {
            'name': v_name,
            'score': v_doubanscore,
            'category': v_category,
            'director': v_director,
            'actor': v_actor,
            'plot': v_plot,
            'region': v_region,
        }

        video_list.append(cur_video)

        if idx % 1000 == 0:
            dbclient.run(query, batch=video_list)
            print("---已存入{}部节目信息---".format(idx))
            video_list = []
    if len(video_list) != 0:
        dbclient.run(query, batch=video_list)
        print("---已存入全部节目信息---")


def yidong_type():
    video_list = []
    query = """
            WITH {batch} as video_list
            UNWIND video_list AS video
            MERGE (m:Video {name: video.name, category: video.category}) 
            ON CREATE SET m.score = video.score, m.source = 'xx' ON MATCH SET m.score = video.score 
            FOREACH (actor IN video.actor | MERGE(a:Actor {name: actor}) MERGE (m) <-[:ACTED_IN]- (a) )
            FOREACH (director IN video.director | MERGE(d:Director {name:director}) MERGE (d) -[:DIRECTED]-> (m) )
            FOREACH (tag IN video.plot | MERGE(t:Plot {name: tag}) MERGE (m) -[:IN_GENRE]-> (t) )
            FOREACH (region in video.region | MERGE(r:Region {name: region}) MERGE (m) -[:IN_REGION]-> (r) )
            """

    file_path = os.path.join(project_root_path, 'content_based/data/yidong/t_video.txt')

    for idx, line in enumerate(open(file_path, 'r', encoding='utf-8')):
        if idx == 0:
            continue
        doc = line.strip().split('#')

        videoid = doc[0]
        videoname = doc[1]
        category = doc[2]
        directname = doc[3]
        actorname = doc[4]
        videoplot = doc[5]
        videoscore = doc[6]
        videoregion = doc[7]


        video_name = utils.clean_video_name(videoname)
        director_list = utils.clean_d_a_p(directname)
        actor_list = utils.clean_d_a_p(actorname)
        plot_list = utils.clean_d_a_p(videoplot)
        region_list = utils.clean_region(videoregion)

        cur_video = {'name': video_name,
                     'score': videoscore,
                     'category': category,
                     'director': director_list,
                     'actor': actor_list,
                     'plot': plot_list,
                     'region': region_list}

        video_list.append(cur_video)

        if idx % 1000 == 0:
            dbclient.run(query, batch=video_list)
            print("---已存入{}部节目信息---".format(idx))
            video_list = []
    if len(video_list) != 0:
        dbclient.run(query, batch=video_list)
        print("---已存入全部节目信息---")

if __name__ == "__main__":
    douban_type_2()

