import os
import datetime as dt
import pendulum
from urllib import parse
from airflow import DAG
from airflow.operators.bash import BashOperator


pjoin = os.path.join
data = "tolldata.tgz"
url = parse.urlunparse(("https", "cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud",
                        f"IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/{data}", "", "", ""))
target_dir = pjoin(os.getenv("AIRFLOW_HOME"), "data", "tolldata")
target = pjoin(target_dir, data)
vehicle_data = pjoin(target_dir, "vehicle-data.csv")
csv_data = pjoin(target_dir, "csv_data.csv")
tollplaza_data = pjoin(target_dir, "tollplaza-data.tsv")
tsv_data = pjoin(target_dir, "tsv_data.csv")
payment_data = pjoin(target_dir, "payment-data.txt")
fixed_width_data = pjoin(target_dir, "fixed_width_data.csv")
extracted_data_tmp = pjoin(target_dir, "extracted_data_tmp.csv")
extracted_data = pjoin(target_dir, "extracted_data.csv")


default_args = {"owner": "Joe User",
                "start_date": pendulum.today("UTC").add(days=0),
                "email": "firtname.lastname@gmail.com",
                "email_on_failure": True,
                "email_on_retry": True,
                "retries": 1,
                "retry_delay": dt.timedelta(minutes=5),
                }

dag = DAG(
    dag_id="ET_toll_data",
    default_args=default_args,
    description="Apache Airflow Final Assignment",
    schedule=dt.timedelta(days=1),
)

clean_all = BashOperator(
    task_id="clean_all", bash_command=f"rm -rf {pjoin(target_dir, '''{*,.*}''')}", dag=dag,)

download = BashOperator(
    task_id="download", bash_command=f"wget {url} -P {target_dir}", dag=dag,)

change_perms = BashOperator(
    task_id="change_perms", bash_command=f"chmod -R g+rw {target}", dag=dag,)

unzip = BashOperator(
    task_id="unzip", bash_command=f"tar zxvf {target} -C {target_dir}", dag=dag,)

cleanup = BashOperator(
    task_id="cleanup", bash_command=f"rm -f {target}", dag=dag,)

extract_data_from_csv = BashOperator(
    task_id="extract_csv", bash_command=f"cat {vehicle_data} | cut -d ''',''' -f1-4 > {csv_data}", dag=dag,)

extract_data_from_tsv = BashOperator(
    task_id="extract_tsv",
    bash_command=f"cat {tollplaza_data} | tr '''\\t''' ''',''' | cut -d ''',''' -f5-7 | tr -d '''\\r''' > {tsv_data}", dag=dag,)

extract_data_from_fixed_width = BashOperator(
    task_id="extract_fixed_width",
    bash_command=f"cat {payment_data} | sed '''s/^[ \t]*//''' | sed '''s/[ \t]\\{{2,\\}}/ /g''' | cut -d ''' ''' -f10-11 | tr  ''' ''' ''',''' > {fixed_width_data}", dag=dag,)

consolidate_data = BashOperator(
    task_id="consolidate_data", bash_command=f"paste -d ''',,''' {csv_data} {tsv_data} {fixed_width_data} > {extracted_data_tmp}", dag=dag,)

column_cmd = "echo -e '''Rowid,Timestamp,Anonymized Vehicle number,Vehicle type,Number of axles,Tollplaza id,Tollplaza code,Type of payment code,Vehicle code'''"
add_columns = BashOperator(
    task_id="add_columns", bash_command=f"{column_cmd} | cat - {extracted_data_tmp} > {extracted_data};rm {extracted_data_tmp}", dag=dag,)

transform_column = f"cat {extracted_data} | cut -d ''',''' -f4 | tr '''a-z''' '''A-Z''' > new_column"
save_left_column = f"cat {extracted_data} | cut -d ''',''' -f1-3 > left_columns"
save_right_column = f"cat {extracted_data} | cut -d ''',''' -f5- > right_columns"
combine_columns = f"paste -d ''',,''' left_columns new_column right_columns > {extracted_data}"
remove_files = "rm new_column left_columns right_columns"
transform = BashOperator(
    task_id="transform", bash_command=f"{transform_column};{save_left_column};{save_right_column};{combine_columns};{remove_files}", dag=dag,)

clean_all >> download >> change_perms >> unzip >> cleanup >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> add_columns >> transform
