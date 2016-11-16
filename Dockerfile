FROM python:2-onbuild

RUN python -m textblob.download_corpora

CMD [ "python", "__main__.py" ]
