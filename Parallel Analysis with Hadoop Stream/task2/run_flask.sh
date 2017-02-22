#!/bin/bash

# For task 3 you will want to change
# playground.py to playground-ecs.py

# for Python 3
# export FLASK_APP=playground.py
# python -m flask run


# pre-build all images that might need

# build python image
docker build -f python/Dockerfile -t python_image .

# build perl image
docker build -f perl/Dockerfile -t perl_image .

# build ruby image
docker build -f ruby/Dockerfile -t ruby_image .

# build lua image
docker build -f lua/Dockerfile -t lua_image .

# for Python 2
python playground.py
