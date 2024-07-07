""" 
# Purpose: Get source and category level information from database
# Author: Indrajeet Gour(igour)
"""
import sys
import psycopg2
import psycopg2.extras


class get_source_infra_dtls:
    def __init__(self, **kwargs):
        self.user = kwargs['postgre_user']
        self.password = kwargs['postgre_pass']
        self.host = kwargs['postgre_host']
        self.port = kwargs['postgre_port']
        self.dbname = kwargs['postgre_dbname']
        self.ssr_id = kwargs['ssr_id'].lower()
        self.schedule_group = kwargs['schedule_group'].lower()
        self.category = kwargs['category'].lower()

    def get_source_dtl(self):
        print("=======================| Get Infra and Source info: START |======================")
        sql_template = """select * from
                            (select b.ssr_id,b.schedule_group,b.source_name,b.source_type,b.category,b.resource_cpty_ind,
                            a.num_of_cpu,a.memory,a.disk_size,a.disk_type,b.num_of_instances
                            from s2hingestionconfig.vm_config_dtl a 
                            join s2hingestionconfig.job_info_dtl b 
                            on a.resource_cpty_ind = b.resource_cpty_ind) tbl
                            where ssr_id = %s
                            and schedule_group = %s
                            and category = %s"""
        conn = None
        row = None
        try:
            # read database configuration
            params = dict(dbname=self.dbname, user=self.user,
                          password=self.password, host=self.host, port=self.port)

            # connect to the PostgreSQL database
            conn = psycopg2.connect(**params)
            # create a new cursor
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # execute the INSERT statement
            cur.execute(sql_template, (self.ssr_id,
                        self.schedule_group, self.category))

            # get the generated id back
            rows = cur.fetchall()
            row_dict = [{key: value for key, value in record.items()}
                        for record in rows]
            if not row_dict:
                print(
                    "[WARN] No records found from postgre metadata tables, check the metadata tables.")
                print("source_infra_info =", None)
            else:
                print("Successfully fetched records from postgre metadata tables...")
                print("source_infra_info =", row_dict[0])

            print(
                "=======================| Get Infra and Source info: END |======================")

        except (Exception, psycopg2.DatabaseError) as error:
            print(
                "Failed to fetched record from postgre metadata tables.", error)
            exit(-1)
        finally:
            if conn:
                cur.close()
                conn.close()
                print("PostgreSQL connection is closed")
        return row


# postgres information
postgre_user = sys.argv[1]
postgre_pass = sys.argv[2]
postgre_host = sys.argv[3]
postgre_port = sys.argv[4]
postgre_dbname = sys.argv[5]
ssr_id = sys.argv[6]
schedule_group = sys.argv[7]
category = sys.argv[8]


print("=======================| Get Source Info from DB: START |======================")
infra_dtls = get_source_infra_dtls(postgre_user=postgre_user,
                                   postgre_pass=postgre_pass,
                                   postgre_host=postgre_host,
                                   postgre_port=postgre_port,
                                   postgre_dbname=postgre_dbname,
                                   ssr_id=ssr_id,
                                   schedule_group=schedule_group,
                                   category=category)

infra_dtls.get_source_dtl()
print("=======================| Get Source Info from DB: END |======================")
 