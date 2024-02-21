FROM python:3.9

RUN ls
COPY docker/requirements.txt requirements.txt

RUN pip3 install --upgrade pip &&\
    pip3 install -r requirements.txt 

WORKDIR /app/work
ENV HOME /app/work
COPY api api
COPY pytest.ini pytest.ini
ENV PYTHONUNBUFFERED=1

CMD ["bash"]