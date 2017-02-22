FROM ubuntu:latest

# install flask server
RUN apt-get update -y
RUN apt-get install -y python-pip python-dev build-essential
RUN pip install flask

# install ruby
RUN \
  apt-get install -y ruby ruby-dev ruby-bundler && \
  rm -rf /var/lib/apt/lists/*

# install lua
RUN apt-get update -y && apt-get install -y luajit luarocks

# Define default command.
COPY app.py /
COPY index.html /
CMD ["python", "app.py"]