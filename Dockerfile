FROM continuumio/miniconda3:latest
RUN apt-get update -y

RUN apt-get idonstall -y vim screen htop rsync build-essential

RUN pip install boto3 black python-dotenv pandas spotipy requests

RUN echo 'root:flippr' | chpasswd
# RUN sed -ri 's/^#PerminRootLogin\s+.*/PerminRootLogin yes/' /etc/ssh/sshd_config
# RUN sed -ri 's/UsePAM yes/#UsePAM yes/g' /etc/ssh/sshd_config

COPY . flippr
WORKDIR flippr
RUN pip install -e .

ENTRYPOINT ["python", "scripts/etl.py"]