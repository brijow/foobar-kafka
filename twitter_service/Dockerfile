FROM quay.io/bitnami/python:3.8.6-prod-debian-10-r81
WORKDIR /usr/app
ADD ./requirements.txt ./
RUN pip install -r requirements.txt
CMD ["python3", "twitter_api.py"]