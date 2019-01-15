
def read_watch_history():  #生成用户浏览记录 和 喜欢记录
    watch_history = {}
    watch_history_like = {}
    watched_history ={}   #video被观看次数
    watched_history_like = {} # video被喜欢次数
    data_douhao = open('/Users/maclc/hybrid_recommendation/collaborative_filtering/cleaned_data/score_adult.txt', 'r',
                       encoding='utf-8')

    for line in data_douhao:
        every = line.strip().split(',')
        (userid, videoid, rank) = every
        if rank > 0.8:#喜欢列表
            if userid not in watch_history_like:
                watch_history_like[userid] = [(videoid, rank)]

            else:
                watch_history_like[userid].append((videoid, rank))

            if videoid not in watched_history_like:
                watched_history_like[videoid]=[(userid,rank)]
            else:
                watched_history_like[videoid].append((userid,rank))
        #观看列表
        if userid not in watch_history:
            watch_history[userid] = [(videoid, rank)]
        else:
            watch_history[userid].append((videoid, rank))

        if videoid not in watched_history:
            watched_history[videoid]=[(userid,rank)]
        else:
            watched_history[videoid].append((userid,rank))

    return watch_history,watch_history_like,watched_history,watched_history_like


if __name__ == "__main__":
   watch_history,watch_history_like,watched_history,watched_history_like = read_watch_history() #生成用户浏览记录 和 喜欢记录 video被观看记录，被喜欢次数



