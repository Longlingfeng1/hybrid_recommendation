

def read_user_feature():
    data = open("../user_tag/user_feature.txt")
    # data = f.read()
    #print(data)
    user_feature = {}
    for line in data:

        every = line.strip().split(',')
        # every = list(filter(None, every_1))  # 只能过滤空字符和None
        [userid,feature]=every
        user_feature[userid]=feature
        #print(every)
    return user_feature;


def read_video_feature():
    data = open("../video_tag/video_feature.txt")
    # data = f.read()
    #print(data)
    video_feature = {}
    for line in data:
        every = line.strip().split(',')
        # every = list(filter(None, every_1))  # 只能过滤空字符和None
        [videoid, videoname,feature] = every
        video_feature[videoid] = feature
        # print(every)
    return video_feature;


def read_recommend_result():
    data = open("../log/watched-recommend_12-01 04:18.log")

    recommend_list={}
    num = 0
    for line in data:
        if num<7:
            num +=1
            continue
        every = line.strip().split("#")
        #print(every)
        userid = every[0]
        recommends = every[1]
        recommend = recommends.strip().split("|")
        for recom in recommend:
            rec = recom.strip().split(",")
            recom_name = rec[0]
            recom_score = rec[1]
            recommend_list[userid] = {}
            recommend_list[userid][recom_name]=recom_score

    return recommend_list


def read_watch_history():
    data = open("../collaborative_filtering/cleaned_data/score_adult.txt")
    watch_history = {}
    for line in data:
        every = line.strip().split(",")
        userid =every[0]
        videoid =every[1]
        score =every[2]
        if float(score) <0.1:
            continue
        watch_history[userid]={}
        watch_history[userid][videoid]=score
    return watch_history


def create_txt(user_feature,video_feature,recommend_list):

    f = open("user_feature.txt", 'w')
    # f.writelines(str(userid) + ',' + user_str + '\n')
    for userid in recommend_list:
        recommends = recommend_list[userid]
        for recommend in recommends:
            recommend_name = recommend
            recommend_score = recommend_list[userid][recommend]
            if float(recommend_score) > 0.5:
                recommend_score = 1
            else:
                recommend_score = 0

            f.writelines(str(recommend_score) + ',' + user_feature[userid] + video_feature[recommend_name] + '\n')


def create_txt2(user_feature,video_feature,watch_history):
    f=open("feature_all_maohao.txt","w")

    # for userid in watch_history:
    #     f.writelines()
    for userid in watch_history:
        watchhistory = watch_history[userid]
        for watch in watchhistory:
            watch_video_id = watch
            watch_video_rank = watch_history[userid][watch_video_id]
            if float(watch_video_rank) >0.8:
                watch_video_rank=1
            else:
                watch_video_rank=0

            f.writelines(str(watch_video_rank)+" "+video_feature[watch_video_id] + user_feature[userid]+'\n')






if __name__ == "__main__":

    user_feature = read_user_feature()
    #print(user_feature)
    video_feature = read_video_feature()
    #print(video_feature)
    #recommend_list = read_recommend_result()
    watch_history = read_watch_history()
    print(watch_history)
    create_txt2(user_feature,video_feature,watch_history)
    #print(recommend_list)
    #create_txt(user_feature,video_feature,recommend_list)




