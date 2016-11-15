FROM scivm/scientific-python-2.7

ADD . /oracle

RUN pip install -r /oracle/requirements.txt

RUN python -m textblob.download_corpora

WORKDIR /oracle

CMD [ "python", "__main__.py" ]
