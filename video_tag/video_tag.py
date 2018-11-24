### video的文本特征（plot,region,category） => onehot



from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
import numpy as np
import operator
import user_tag.utils as utils

def get_video():
    video_table = {}
    for idx, line in enumerate(open('../data/one_month_data/videos_all.txt', 'r', encoding='utf-8')):
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

        video_table[video_id] = [
             video_name,
            category,
            directors,
            actors,
            plots,
             douban_score,
             utils.clean_region(region),
             length,
            watch_num
        ]
    return video_table
"""
    video_info = {}
    data = open("../data/one_month_data/videos_all.txt")
    #data = f.read()
    print(data)

    for line in data:

        every = line.strip().split(',')
        #every = list(filter(None, every_1))  # 只能过滤空字符和None
        if len(every)>11:
            continue
        [id,name,category,directname,actorname,videoplot,videoscore,videoregion,videotimelength,watchedcount]=every
        if id not in video_info:
            video_info[id] = [name,category,directname,actorname,videoplot,videoscore,videoregion,videotimelength,watchedcount]
    return video_info   #dict key:id  value:name,category,directname,actorname,videoplot,videoscore,videoregion,videotimelength,videonum,watchedcount
"""

def get_region(video_info):
    #将video region所有的region提取出来放入video_region list中。

    video_region=[]
    for video_id in video_info:
        #print(video_info[video_id])
        video_feature = video_info[video_id]
        video_region_every = video_feature[6]
        #print(video_region_every)
        """video_region_every_split = video_region_every.split('|')
        for i in range(0,len(video_region_every_split)):
            if video_region_every_split[i] not in video_region:
               video_region.append(video_region_every_split[i])"""
        #for i in range(0,len(video_region_every)):
        if video_region_every:
           video_region.append(video_region_every)
        else:
            video_region.append("")

    #print(video_region)
    return video_region   #list 所有节目的region


def build_region(video_info,video_region):

    region_one_hot = {}
    vectorizer = CountVectorizer(min_df=1)
    X_1 = vectorizer.fit_transform(video_region)
    analyze = vectorizer.build_analyzer()
    #print(vectorizer.vocabulary_) # 词 顺序
    #print(X_1.toarray().sum(axis=0))#词 频
    num=0
    for i in video_info.keys():
       region_one_hot[i]=X_1[num].toarray()
       num = num+1
    # dict 每个 词汇出现的次数 key:region  value:次数
    region_count = {}
    count_list = X_1.toarray().sum(axis=0)
    for region in vectorizer.vocabulary_:
        region_count[region] = count_list[vectorizer.vocabulary_[region]]
    #print(region_count)
    #print(region_one_hot)
    return region_one_hot   #


def get_plot(video_info):
    #将video plot所有的plot提取出来放入video_plot list中。

    video_plot=[]
    for video_id in video_info:
        #print(video_id)
        video_feature = video_info[video_id]
        video_plot_every = video_feature[4]
        video_plot.append(video_plot_every)
    #print(video_plot)
    return video_plot   #list 所有节目的region


def build_plot(video_info,video_plot):
    plot_one_hot = {}

    #初始化 CountVectorizer
    vectorizer = CountVectorizer(min_df=1)
    X_2 = vectorizer.fit_transform(video_plot)
    analyze = vectorizer.build_analyzer()
    #print(vectorizer.vocabulary_) # 词频
    #print(X_2.toarray().sum(axis=0))

    # dict 每个 词汇出现的次数 key:plot  value:次数
    plot_count = {}
    count_list = X_2.toarray().sum(axis=0)
    for plot in vectorizer.vocabulary_:
        plot_count[plot] = count_list[vectorizer.vocabulary_[plot]]


    #将plot中出现次数低于 100 的统一归为"其他"
    sum = 0
    for plot in list(plot_count):
        if plot_count[plot]<100:
            sum = sum+plot_count[plot]
            plot_count.pop(plot)
    plot_count["其他"]=sum
    #按照value排序
    plot_count = sorted(plot_count.items(), key=operator.itemgetter(1),reverse=True)
    plot_count = dict(plot_count)
    #print(plot_count.keys())
    #print(plot_count["自然"])
    #print(video_plot)

    #把video_info 中plot词频低于100的plot 统一改为其他
    for id in video_info:
        every_plot_list = video_info[id][4].split("|")
        #print(every_plot_list)
        p = ''
        for every_plot in every_plot_list:

           if every_plot in list(plot_count.keys()):
               if not p:
                 p = p+every_plot
               else:
                 p = p+'|'+every_plot
        if not p: #p为空
           video_info[id][4]="其他"
        else:
            video_info[id][4]=p
   # for id in video_info:
       # print(id+":"+video_info[id][0]+":"+video_info[id][4])

    #更新完video_info后，重新获取video_plot ： list
    video_plot_1 = get_plot(video_info)
   # print(video_plot_1)

    #获取plot_one_hot
    X_2 = vectorizer.fit_transform(video_plot_1)
    num=0
    for i in video_info.keys():
        plot_one_hot[i]=X_2[num].toarray()
        num = num+1

    return plot_one_hot   #


def get_category(video_info):
    #将video plot所有的plot提取出来放入video_plot list中。

    video_category=[]
    for video_id in video_info:
        #print(video_id)
        video_feature = video_info[video_id]
        video_category_every = video_feature[1]
        video_category.append(video_category_every)
    #print(video_category)
    return video_category   #list 所有节目的region



def build_category(video_info,video_category):
    category_one_hot = {}
    vectorizer = CountVectorizer(min_df=1)
    X_1 = vectorizer.fit_transform(video_category)
    analyze = vectorizer.build_analyzer()
    #print(vectorizer.vocabulary_) # 词 顺序
    #print(X_1.toarray().sum(axis=0))#词 频
    num=0
    for i in video_info.keys():
        category_one_hot[i]=X_1[num].toarray()
        num = num+1
    # dict 每个 词汇出现的次数 key:region  value:次数
    category_count = {}
    count_list = X_1.toarray().sum(axis=0)
    for category in vectorizer.vocabulary_:
        category_count[category] = count_list[vectorizer.vocabulary_[category]]
    #print(category_count)

    return category_one_hot   #


def create_feature(video_info,region_one_hot,plot_one_hot,category_one_hot):
    f = open("video_feature.txt",'w')

    #生成feature
    video = []
    for video_id in video_info:
        video = list(region_one_hot[video_id][0]) + list(plot_one_hot[video_id][0]) + list(category_one_hot[video_id][0])
        video_str = " ".join([str(x) for x in video])
        f.writelines(str(video_id)+','+video_str+'\n')
       # region = "".join(region_one_hot[video_id].tolist())

        #f.writelines(region)


if __name__ == "__main__":
    video_info = get_video()

    video_region = get_region(video_info)
    region_one_hot = build_region(video_info,video_region)
    #print(region_one_hot)

    video_plot = get_plot(video_info)
    plot_one_hot = build_plot(video_info, video_plot)
    #print(plot_one_hot)
    #print(plot_one_hot["1"])


    video_category = get_category(video_info)
    category_one_hot = build_category(video_info,video_category)
    print(category_one_hot)

    create_feature(video_info,region_one_hot,plot_one_hot,category_one_hot)


"""onehot编码  不能将cold|warm 分成cold  warm ,并且顺序不确定
from numpy import array
from numpy import argmax
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
# define example
data = ['cold', 'cold', 'warm', 'cold', 'hot', 'hot', 'warm', 'cold', 'warm', 'hot']
values = array(data)
print(values)
# integer encode
label_encoder = LabelEncoder()
integer_encoded = label_encoder.fit_transform(values)
print(integer_encoded)
# binary encode
onehot_encoder = OneHotEncoder(sparse=False)
integer_encoded = integer_encoded.reshape(len(integer_encoded), 1)
onehot_encoded = onehot_encoder.fit_transform(integer_encoded)
print(onehot_encoded)
# invert first example
inverted = label_encoder.inverse_transform([argmax(onehot_encoded[0, :])])
print(inverted)
"""

