FROM ubuntu:latest
LABEL authors="zhangkanghan"


RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list
RUN apt-get clean

RUN DEBIAN_FRONTEND=noninteractive apt-get update -y

RUN DEBIAN_FRONTEND=noninteractive apt-get install git -y
RUN git --version

RUN DEBIAN_FRONTEND=noninteractive apt-get install golang  -y
RUN go version

RUN DEBIAN_FRONTEND=noninteractive apt-get install redis -y

RUN cd /var/local
RUN git clone https://github.com/zkanghan/Gedis.git
