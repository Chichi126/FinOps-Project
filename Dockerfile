FROM apache/airflow:3.1.5

ARG AIRFLOW_HOME_ARG=/opt/airflow
ENV AIRFLOW_HOME=${AIRFLOW_HOME_ARG}

USER airflow
# each folder in AIRFLOW_HOME can be used as import in python
ENV PYTHONPATH=${AIRFLOW_HOME}:$PYTHONPATH

COPY requirements.txt /
RUN pip install  -r /requirements.txt

# copy dags, plugins and utils after install requirementsto avoid to install requirements on each code change
# add always chwown to each COPY to make airflow the owner
COPY --chown=airflow:airflow dags /opt/airflow/dags
COPY --chown=airflow:airflow plugins /opt/airflow/plugins
COPY --chown=airflow:airflow business_logic /opt/airflow/business_logic

WORKDIR /opt/airflow