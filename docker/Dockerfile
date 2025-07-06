FROM apache/airflow:2.9.3-python3.10

WORKDIR /opt/airflow

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV PYTHONPATH=/opt/airflow/scripts:$PYTHONPATH
