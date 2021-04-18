#Create a base image.
FROM python:latest AS base

#Set work directory
WORKDIR /usr/src/bot3

#Update pip
RUN pip install --upgrade pip

#Install dependencies using a requirements file
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
