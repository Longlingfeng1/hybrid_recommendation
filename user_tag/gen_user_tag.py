import random
from collections import defaultdict
from pprint import pprint
# import user_tag.utils as utils
import utils
from collections import defaultdict


day_d = {
    '06': '工作日',
    '07': '工作日',
    '08': '周末',
    '09': '周末',
    '10': '工作日',
    '11': '工作日',
    '12': '工作日'
}


def dev_print():
    user_watch_dict = {}
    for _, line in enumerate(open('data/watch_log.txt', 'r', encoding='utf-8')):

        if _ > 1000:
            break
        docs = line.split('#')

        userId = None
        for idx, doc in enumerate(docs):
            if idx == 0:
                userId = doc
                user_watch_dict[userId] = defaultdict(lambda: 0)
            else:
                videoId, time_str, category = doc.split(',')
                d = time_str[6:8]
                h = int(time_str[8:])
                category = category.strip()
                if category in ['动画', '少儿', '教育']:
                    category = '儿童'
                else:
                    category = '成人'
                user_watch_dict[userId][category, day_d[d], h] += 1
    pprint(user_watch_dict)


def generate_time_tag():
    '''
    生成用户活跃时段标签
    :return:
    '''
    user_watch_dict = {}
    user_tag_dict = {}
    for _, line in enumerate(open('data/watch_log.txt', 'r', encoding='utf-8')):
        docs = line.split('#')
        userId = None
        for idx, doc in enumerate(docs):
            if idx == 0:
                userId = doc
                user_watch_dict[userId] = defaultdict(lambda: 0)
            else:
                videoId, time_str, category = doc.split(',')
                d = time_str[6:8]
                h = int(time_str[8:])
                category = category.strip()
                if category in ['动画', '少儿', '教育']:
                    category = '儿童'
                else:
                    category = '成人'
                user_watch_dict[userId][category, day_d[d], h] += 1

    """生成活跃时间段"""
    for item in user_watch_dict.items():
        userId = item[0]
        tag_set = set()
        logs = sorted(item[1].items(), key=lambda x: x[1], reverse=True)
        child = filter(lambda x: x[0][0] == '儿童', logs)
        man = filter(lambda x: x[0][0] == '成人', logs)
        for people in (child, man):
            for idx, log in enumerate(people):
                target = log[0][0]
                day_type = log[0][1]
                watch_time = log[0][2]
                if idx <= 2 or watch_time >= 7:
                    active_start = watch_time
                    active_end = watch_time + 1
                    tag_set.add((target, day_type, active_start, active_end))
        user_tag_dict[userId] = tag_set

    """连续时间段归并"""
    user_tag_dict_compress = {}
    for item in user_tag_dict.items():
        userId = item[0]
        user_tag_dict_compress[userId] = []
        sub = []
        for target in ['成人', '儿童']:
            for day_type in ['工作日', '周末']:
                man = filter(lambda x: x[0] == target and x[1] == day_type, item[1])
                sub.append(list(man))
        for s_list in sub:
            s_list = sorted(s_list, key=lambda x: x[2])
            active_zone = []
            pre_start = 0
            pre_end = 0
            begin_time = 0
            target = None
            day_type = None
            for idx, log in enumerate(s_list):
                target = log[0]
                day_type = log[1]
                start_time = log[2]
                end_time = log[3]
                if idx == 0:
                    begin_time = start_time
                    pre_start = start_time
                    pre_end = end_time
                if start_time - pre_start <= 2 and idx != len(s_list) - 1:
                    pre_start = start_time
                    pre_end = end_time
                elif start_time - pre_start > 2 and idx != len(s_list) - 1:
                    active_zone.append((begin_time, pre_end))
                    begin_time = start_time
                    pre_start = start_time
                    pre_end = end_time
                elif start_time - pre_start <= 2 and idx == len(s_list) - 1:
                    active_zone.append((begin_time, end_time))
                else:
                    active_zone.append((start_time, end_time))
            if target is not None:
                user_tag_dict_compress[userId].append((target, day_type, active_zone))

    with open('out/user_tag.txt', 'w', encoding='utf-8') as f:
        for item in user_tag_dict_compress.items():
            f.write(str(item)+'\n')


def generate_plot_tag():
    '''
    生成用户喜爱的情节标签
    :return:
    '''
    video_table = {}
    for idx, line in enumerate(open('data/one_month_data/videos_all.txt', 'r', encoding='utf-8')):
        doc = line.strip().split(',')
        video_id = int(doc[0])
        video_name = doc[1]
        category = doc[2]
        directors = doc[3]
        actors = doc[4]
        plots = doc[5]
        douban_score = doc[6]
        region = doc[7]
        length = round(float(doc[8]), 2)
        watch_num = int(doc[9])

        video_table[video_id] = {
            'video_name': video_name,
            'category': category,
            'directors': directors.split('|'),
            'actors': actors.split('|'),
            'plots': plots.split('|'),
            'douban_score': douban_score,
            'region': utils.clean_region(region),
            'length': length,
            'watch_num': watch_num,
        }

    user_watch_history = {}
    for idx, line in enumerate(open('data/one_month_data/score_adult.txt', 'r', encoding='utf-8')):
        user_id, video_id, score = line.strip().split(',')
        user_id = int(user_id)
        video_id = int(video_id)
        score = round(float(score), 4)
        if user_id not in user_watch_history:
            user_watch_history[user_id] = [(video_id, score)]
        else:
            user_watch_history[user_id].append((video_id, score))

     # print(len(user_watch_history))

    user_tag = {}
    for user in user_watch_history:
        user_tag[user] = {'category': defaultdict(int), 'plots': defaultdict(int), 'region': defaultdict(int)}
        watch_history = user_watch_history[user]
        for (video_id, score) in watch_history:
            if score < 0.5:
                continue
            video_info = video_table[video_id]
            user_tag[user]['category'][video_info['category']] += 1
            for plot in video_info['plots']:
                user_tag[user]['plots'][plot] += 1
            for region in video_info['region']:
                user_tag[user]['region'][region] += 1

    for item in user_tag.items():
        print('user_id:{}'.format(item[0]))
        print('category:{}'.format(item[1]['category'].items()))
        print('plots:{}'.format(item[1]['plots'].items()))
        print('region:{}'.format(item[1]['region'].items()))
        print()


if __name__ == "__main__":
    generate_plot_tag()

