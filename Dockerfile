FROM python:2-onbuild

ADD . /oracle

RUN python -m textblob.download_corpora

WORKDIR /oracle

CMD [ "python", "__main__.py" ]
