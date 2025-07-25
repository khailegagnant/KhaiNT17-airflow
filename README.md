I set up a folder called run-here to use docker-compose.
The python DAG file is at run-here/dags/step12345.py.
Below is the connection set up.
![alt text](image.png)


I connected to the psql through terminal to check
![alt text](image-1.png)


The task is done
![alt text](image-2.png)

**_NOTE_**: The module psycopg2 is installed through docker.




The final run-id and its log can be found at ![final_run_logs](run-here/logs/dag_id=tunestream_full_etl_dag/run_id=manual__2025-07-24T191659.317059+0000)
