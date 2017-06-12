#!flask/bin/python
from flask import Flask, jsonify, request
#from flask.ext.cors import CORS
import pyhs2
import subprocess
from subprocess import call
import json
app = Flask(__name__)
#CORS(app)
def import_external_table(host,port,table_name,import_path):
  try:
    with pyhs2.connect(host=host,port=port,authMechanism="PLAIN",user='hadoo',password='hadoop',database='default') as conn:
      with conn.cursor() as cur:
        #Show databases
        print cur.getDatabases()
	query="import external table "+table_name+" from \""+import_path+"\""
	print(query)
        cur.execute(query)
        return 0
  except:
    return 1
		

@app.route('/dataquality/api/loadDateFromPortal', methods=['GET'])
def loadDateFromProtal():
    #response.headers['Access-Control-Allow-Origin'] = '*'
    #response.headers['Access-Control-Allow-Methods'] = 'POST'
    #response.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type' 
    hive_table_list = json.loads(request.args.get('hive_table_list'))
    print(hive_table_list)
    print(len(hive_table_list))
    for index,temp in enumerate(hive_table_list):
	print(str(index)+":"+temp['table_name'])
  	print(str(index)+":"+temp['hdfs_source_data_block_url'])
	print(str(index)+":"+temp['hive_table_meta'])
    return jsonify({'status':"okay"}), 200
    info=[]
    for table in Request_json['hive_table_list']:
	temp = {
          'table_name':table['table_name'],
	  'source_url':table['source_url']
        }
	info.append(temp)
    
    sandbox_ip=Request_json['sandbox_ip']
    source_url=info[0]["source_url"]
    dist_url="hdfs://"+sandbox_ip+":9000"+"/Project/"+project_name
    ret_val=call(["hadoop", "distcp",source_url,dist_url])

    table_name=info[0]['table_name']
    #import_external_table("10.141.220.60",10000,"wyp","\"hdfs://10.141.220.60:9000/Project/wyp\"")
    ret_val2=import_external_table(host=sandbox_ip,port=10000,table_name=table_name,import_path=dist_url)
    if ret_val==0 and ret_val==0:
    	return jsonify({'status': "okay"}), 200
    else:
	return jsonify({'status':"bad"}),300
    #else:
    #return 0

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')
