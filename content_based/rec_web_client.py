from web_service.http_server import MyHTTPServer
import json
from db_client import dbclient
from datetime import datetime


def recommendation_based_on_content(params):
    try:
        assert "videoName" in params
    except:
        rlt_json = {
            'code': 500,
            'error': 'error parameters:{}'.format(params)
        }
        return json.dumps(rlt_json, ensure_ascii=True)

    video_name = params['videoName'][0]

    beging_t = datetime.now()

    query = """
            MATCH (m:Video) WHERE m.name = {videoName}     
            MATCH (m)-[:IN_GENRE]->(p:Plot)<-[:IN_GENRE]-(rec:Video)
            WHERE rec.source = {source}
            WITH m, rec, COUNT(*) AS ps
            OPTIONAL MATCH (m)<-[:ACTED_IN]-(a:Actor)-[:ACTED_IN]->(rec)
            WITH m, rec, ps, COUNT(a) AS acs
            OPTIONAL MATCH (m)<-[:DIRECTED]-(d:Director)-[:DIRECTED]->(rec)
            WITH m, rec, ps, acs, COUNT(d) AS ds            
            OPTIONAL MATCH (m)-[:IN_REGION]->(r:Region)<-[:IN_REGION]-(rec:video)
            WITH m, rec, ps, acs, ds, COUNT(r) AS rs
            RETURN rec.name AS name, rec.source AS source, 
            (10*ps)+(4*acs)+(3*ds)+(2*rs) AS score ORDER BY score DESC LIMIT 100
            """
    query_res = dbclient.run(query, videoName=video_name, source='福建移动').to_data_frame()

    response_list = []
    for idx, row in query_res.iterrows():
        response_list.append({
            'videoName': row['name'],
            'source': row['source'],
            'score': row['score']
        })

    cost = (datetime.now() - beging_t).total_seconds() * 1000

    rlt_json = {
        'code': 200,
        'data': response_list,
        'time_cost': cost
    }
    return json.dumps(rlt_json, ensure_ascii=True)


def get_video_detail(params):
    try:
        assert "videoName" in params
    except:
        rlt_json = {
            'code': 500,
            'error': 'error parameters:{}'.format(params)
        }
        return json.dumps(rlt_json, ensure_ascii=True)

    video_name = params['videoName'][0]

    beging_t = datetime.now()

    query = """
            MATCH (m:Video {name: {videoName}})
            WITH m
            OPTIONAL MATCH (m)<-[:ACTED_IN]-(a)
            WITH m, collect(a.name) AS actor
            OPTIONAL MATCH (m)<-[:DIRECTED]-(b)
            WITH m, actor, collect(b.name) AS director
            OPTIONAL MATCH (m)-[:IN_GENRE]->(c)
            WITH m, actor, director, collect(c.name) AS plot
            OPTIONAL MATCH (m)-[:IN_REGION]->(d)
            RETURN m.name AS name, m.category AS category, m.source AS source,
            actor, director, plot, collect(d.name) AS region
            """
    query_res = dbclient.run(query, videoName=video_name).to_data_frame()

    response_list = []
    for idx, row in query_res.iterrows():
        response_list.append({
            'videoName': row['name'],
            'category': row['category'],
            'source': row['source'],
            'actor': row['actor'],
            'director': row['director'],
            'plot': row['plot'],
            'region': row['region']
        })

    cost = (datetime.now() - beging_t).total_seconds() * 1000

    rlt_json = {
        'code': 200,
        'data': response_list,
        'time_cost': cost
    }
    return json.dumps(rlt_json, ensure_ascii=True)


if __name__ == "__main__":
    print('starting server...')
    host = ""
    port = 9094
    httpd = MyHTTPServer(host, port)
    httpd.Register('/recommend', recommendation_based_on_content)
    httpd.Register('/detail', get_video_detail)
    print('running server...')
    httpd.serve_forever()
