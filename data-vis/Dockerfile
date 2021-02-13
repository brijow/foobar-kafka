FROM python:3.8

ARG CONTAINER_HOSTNAME

WORKDIR /app

ADD ./requirements.txt ./
RUN pip install -r requirements.txt

ADD ./ ./
RUN jupyter trust blog-visuals.ipynb
CMD ["jupyter", "notebook", "blog-visuals.ipynb", "--port=8888", "--no-browser", "--ip=datavis", "--NotebookApp.token=''", "--NotebookApp.password=''","--allow-root"]