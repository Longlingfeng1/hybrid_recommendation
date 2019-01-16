
data = open("../user_tag/user_feature_lr.txt")
num0=0
for line in data:
    every = line.strip().split(',')
    [id,feature]=every
    if num0==0:
      print(len(feature))
    if id==str(130561):
        print('T')
    num0+=1
print(num0)

data = open("../collaborative_filtering/cleaned_data/score_adult.txt")
user ={}
count=0
for line in data:
    every = line.strip().split(',')
    [id,video,score]=every
    if id not in user:
        user[id]=[score]
    else:
        user[id].append(score)
for id in user:
    count+=1

print(count)

data_recommend = open("recommend_feature.txt")
num=1
for line in data_recommend:
    every = line.strip().split(',')
    [id,feature,f1,f2]=every
    if num == 1:
      print(len(f2))
    num += 1

from sklearn.feature_extraction.text import CountVectorizer
col=[
    '周杰伦',
    '范冰冰|周杰伦',
    '',
    '黄渤'
]
a=['']
col.append(a[0])
vectorizer = CountVectorizer(min_df=1)
X_2 = vectorizer.fit_transform(col)
print(X_2.toarray())




import user_tag.utils as utils
import math
def get_video():
    video_table = {}
    for idx, line in enumerate(open('/Users/maclc/hybrid_recommendation/collaborative_filtering/cleaned_data/videos_all.txt', 'r', encoding='utf-8')):
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
video_table = get_video()

watch_log = open('/Users/maclc/hybrid_recommendation/collaborative_filtering/cleaned_data/score_adult.txt')
watch_log_dict={}
for line in watch_log:
    doc = line.strip().split(',')
    userid = doc[0]
    videoid = int(doc[1])
    score = float(doc[2])
    if score<0.1:
        continue
    if userid not in watch_log_dict:
        watch_log_dict[userid] = [(score,video_table[videoid][0])]
    else:
        watch_log_dict[userid].append((score,video_table[videoid][0]))
print(watch_log_dict['6'])
for videoid in video_table:
    if video_table[videoid][0]=='勇敢的世界':
        print(videoid)
        print('true')
#print(video_table[16652])#22134
feature_all = open("feature_all_0.1_filter.txt")
feature_dict = {}
for line in feature_all:
    every = line.strip().split(",")
    score = every[0]
    userid =every[1]
    videoid=int(every[2])
    feature =every[3]
    if userid not in feature_dict:
      feature_dict[userid]=[(score,video_table[videoid][0],feature)]
    else:
      feature_dict[userid].append((score,video_table[videoid][0],feature))


print(feature_dict['6'])


