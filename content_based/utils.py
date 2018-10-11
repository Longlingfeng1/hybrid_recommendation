import re


def clean_region(text):
    if text == "":
        return []
    text = text.lstrip()
    text = text.lstrip()
    docs = re.split('[丨|/ ]', text)
    res = []
    DO_NOT_USE = ['Aruba', 'meiguo', '无', '']
    for doc in docs:
        doc = doc.lstrip()
        matchobj = re.match('[\u4E00-\u9FA5]+', doc)
        if matchobj is None:
            continue
        doc = matchobj.group()
        if doc in ['中国大陆', '内地', '中国内地', '中国', '大陆', '国语大陆']:
            doc = ['中国大陆']
        elif doc in ['香港', '中国香港']:
            doc = ['香港']
        elif doc in ['台湾', '中国台湾']:
            doc = ['台湾']
        elif doc in ['日语']:
            doc = ['日本']
        elif doc in ['日韩']:
            doc = ['日本', '韩国']
        elif doc in ['欧美']:
            doc = ['美国']
        elif doc in DO_NOT_USE:
            continue
        else:
            doc = [doc]
        res.extend(doc)
    return res


def clean_d_a_p(text):
    text = text.lstrip()
    text = text.lstrip()
    docs = re.split('[丨|/ ]', text)
    res = []
    for doc in docs:
        if doc == '':
            continue
        doc = doc.lstrip()
        doc = doc.rstrip()
        res.append(doc)
    return res

def clean_releaseDate(text):
    matchobj1 = re.match('[\d]+-[\d]+-[\d]+', text)
    matchobj2 = re.match('[\d]+-[\d]+', text)
    matchobj3 = re.match('[\d]+', text)
    if matchobj1 is not None:
        return '{}'.format(matchobj1.group())
    elif matchobj2 is not None:
        return '{}-01'.format(matchobj2.group())
    elif matchobj3 is not None:
        return '{}-01-01'.format(matchobj3.group())
    else:
        return ''


def clean_score(text):
    matchobj = re.match("\d\.\d", text)
    if matchobj is not None:
        return matchobj.group()
    else:
        return ''


def clean_video_name(text):
    p = re.compile('（片段|\(片段|\(.*版\)|（.*版）|\(限时|（限时|（4K|（4k|\(4K|\(4k|\.3D|（1080|\(1080|预告|高清')
    return re.split(p, text)[0].strip()

