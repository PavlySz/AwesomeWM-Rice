# QA test
import sys
import redshift_connector
import psycopg2
import time
import pandas as pd
import string
import pdb
import boto3

sys.exit()

# connect to both DBs

# Than to postgress
# 172.16.28.245 db02
# 172.20.14.77 stagedb01
# 172.20.24.220  stagedb02
# 172.20.20.90 stagedb03
# PROD  source: 172.16.24.143  user cluster pass yNRfz0Bko78sLsiWwDsCXvBZMw1KuYNo
pgconn = psycopg2.connect(dbname="flo_stage_us", user="flo", password="pvHZ&5y3A&nT8TQN", host="172.20.20.90")
pgcur = pgconn.cursor()
print("Connected to Postgres..")

redshift = boto3.client("redshift", region_name="us-west-1")
rs_endpoint = "rs-ds-prod"
rs_host = "54.177.154.150"  # 'rs-ds-prod.cebwpov4juzc.us-west-1.redshift.amazonaws.com'
rs_database = "dev"
rs_user = "ds"
rs_password = "x#prdsATrsdsjsz1212"

# Connects to Redshift cluster using AWS credentials
rsconn = redshift_connector.connect(host=rs_host, database=rs_database, user=rs_user, password=rs_password)
rsconn.autocommit = True
rscursor: redshift_connector.Cursor = rsconn.cursor()
print("Connected to RS..")

# print("For debug I need SQLAlchemy as well")
# from sqlalchemy import create_wengine
# engine = create_engine('redshift+redshift_connector://ds:ATrsdsjsz1212@rs-ds-stage.cebwpov4juzc.us-west-1.redshift.amazonaws.com:5439/dev')


pg_template = {}
rs_template = {}
# read templates from rs
read_template = """
select * from unit_test_qa_templates 
where 1=1
   and reason='reporting-psql-rs'  
	 and ( indepth like 'booking%' or  indepth='table-level-match') """  # and indepth!='booking-by-earlydep'"
rscursor.execute(read_template)
sql_templates = rscursor.fetchall()
for t in sql_templates:
    rs_template[t[1]] = t[2]
    pg_template[t[1]] = t[3]

# pdb.set_trace()

# read unit test parameters from db
print("QA templates read from DB")
ua_test = """select * from unit_test_qa_instances
where reason='reporting-psql-rs' 
and ( indepth like 'booking%' or  indepth='table-level-match') """

rscursor.execute(ua_test)
test_instances = rscursor.fetchall()
test_params = {}
for t in test_instances:
    if t[2] in test_params:
        test_params[t[2]].append({"param1": t[3], "param2": t[4], "param3": t[5], "id": t[0]})
    else:
        test_params[t[2]] = []
        test_params[t[2]].append({"param1": t[3], "param2": t[4], "param3": t[5], "id": t[0]})


# execure tests
pg_results = {}
rs_results = {}
# print(rs_template.keys())
# print (test_params)
for r in rs_template.keys():
    #    print(r)
    pg_results[r] = {}
    rs_results[r] = {}
    if r not in test_params:
        continue
    #    print("run for ",r)
    for t in test_params[r]:
        qt = string.Template(pg_template[r])
        pg_results[r][t["id"]] = pd.read_sql(qt.safe_substitute(t), pgconn)
        print("QA - PG: " + r + " param:" + str(t) + " executed")
        #       print("PG ",qt.safe_substitute(t))
        # pdb.set_trace()
        #        pg_results[r][t['id']].to_sql('pg_result_debug_'+str(t['id']),engine,if_exists='replace',index=False)

        #        print("PG result stored at RS pg_result_debug_"+str(t['id']))
        qt = string.Template(rs_template[r])
        #        print("RS ",qt.safe_substitute(t))
        rs_results[r][t["id"]] = pd.read_sql(qt.safe_substitute(t), rsconn)
        print("QA - RS: " + r + " param:" + str(t) + " executed ")


# comapre outputs
cmp = {}
totalerrcnt = 0
totalchkcnt = 0
for r in rs_template.keys():  # r is the report identifier
    cmp[r] = {}
    errcnt = 0
    chkcnt = 0
    for test_id in rs_results[r].keys():
        cmp[r][test_id] = {}
        # print("Evaluating ",r," test_id: ",test_id)
        for f in range(len(rs_results[r][test_id].columns)):  # f as field
            # print("Field: ",f)
            chkcnt = chkcnt + 1
            totalchkcnt = totalchkcnt + 1
            #            pdb.set_trace()
            if (rs_results[r][test_id].iloc[0, [f]].values[0] is None) or (
                pg_results[r][test_id].iloc[0, [f]].values[0] is None
            ):
                continue
            if (
                abs(rs_results[r][test_id].iloc[0, [f]].values - pg_results[r][test_id].iloc[0, [f]].values)
                <= 0.1 * pg_results[r][test_id].iloc[0, [f]].values
            ):
                cmp[r][test_id][f] = "match"
            else:
                print(
                    "Mismatch test_id:",
                    test_id,
                    " - ",
                    f,
                    ": ",
                    rs_results[r][test_id].columns[f],
                    "pg: ",
                    pg_results[r][test_id].iloc[0, [f]],
                    " rs: ",
                    rs_results[r][test_id].iloc[0, [f]],
                )
                cmp[r][test_id][f] = "missmatch"
                field = "f_" + str(f)
                if field in cmp[r]:
                    cmp[r][field] = cmp[r][field] + 1
                else:
                    cmp[r][field] = 1
                errcnt = errcnt + 1
                totalerrcnt = totalerrcnt + 1
        print("QA outcome - ", r, ": id ", test_id, " check/failed: ", str(chkcnt), "/", str(errcnt))

print("QA outcome - Final: check/failed: ", str(totalchkcnt), "/", str(totalerrcnt))
# pdb.set_trace()


def pause_instance(instance_id):
    try:
        response = redshift.pause_cluster(ClusterIdentifier=instance_id)
        logger.info("Started instance %s.", instance_id)
    except ClientError:
        logger.exception("Couldn't pause instance %s.", instance_id)
        raise
    else:
        return response


# resume instance
print("Pause redshift..")
try:
    res = pause_instance(rs_endpoint)
except:
    print("Pause failed")

