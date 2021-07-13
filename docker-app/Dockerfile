FROM python:3.8
WORKDIR ./bdtproject
ENV FLASK_APP=webapp.py
ENV FLASK_RUN_HOST=0.0.0.0
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
RUN gunzip ./model/rf.joblib.gz
EXPOSE 5000
CMD ["flask", "run"]