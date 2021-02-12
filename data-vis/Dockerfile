FROM python:3.8

WORKDIR /app

ADD ./requirements.txt ./
RUN pip install -r requirements.txt

ADD ./ ./
CMD ["jupyter", "notebook", "blog-visuals.ipynb", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root"]
