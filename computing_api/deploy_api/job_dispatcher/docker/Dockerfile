from ubuntu:20.04
MAINTAINER MLEXCHANGE-TEAM

RUN apt-get update && apt-get install -y curl\
    ca-certificates\
    amqp-tools\
    dnsutils\
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

RUN  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

RUN echo \
  "deb [arch=arm64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

RUN apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io


RUN apt-get install -y python3-pip\
    python3
COPY docker/requirements.txt requirements.txt
RUN pip3 install --upgrade pip &&\
    pip3 install -r requirements.txt 

WORKDIR /app/work
ENV HOME /app/work
COPY job_dispatcher.py job_dispatcher.py
COPY model.py model.py
ENV PYTHONUNBUFFERED=1

CMD ["bash"]