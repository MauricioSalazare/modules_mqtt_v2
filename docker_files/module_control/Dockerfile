FROM python:3.8.6-slim

MAINTAINER MauricioSalazar e.m.salazar.duque@tue.nl

# =========== Basic Configuration ======================================================
# Update the system
RUN apt-get -y update \
    && apt-get install -y sudo make cmake \
    gcc g++ gfortran git patch wget pkg-config \
    liblapack-dev libmetis-dev

# =========== Folder before after installation for ipopt ===============================
# Update the system
RUN mkdir -p /home/docker/to_install/Ipopt \
    && mkdir -p /home/docker/installed/Ipopt

# ======== Install BLAS and LAPACK =====================================================
RUN  apt-get -y update \
   && apt-get install -y apt-utils \
   && apt-get install -y libblas-dev liblapack-dev

# ======== Start IPOPT installation ====================================================
# Retrieve and copy all the dependencies needed by Ipopt
WORKDIR /home/docker/to_install/Ipopt
RUN wget http://www.coin-or.org/download/source/Ipopt/Ipopt-3.12.4.tgz
RUN tar xvf ./Ipopt-3.12.4.tgz
WORKDIR /home/docker/to_install/Ipopt/Ipopt-3.12.4/ThirdParty/Blas
RUN ./get.Blas
WORKDIR /home/docker/to_install/Ipopt/Ipopt-3.12.4/ThirdParty/Lapack
RUN ./get.Lapack
WORKDIR /home/docker/to_install/Ipopt/Ipopt-3.12.4/ThirdParty/Mumps
RUN ./get.Mumps
WORKDIR /home/docker/to_install/Ipopt/Ipopt-3.12.4/ThirdParty/Metis
RUN ./get.Metis
WORKDIR /home/docker/to_install/Ipopt/Ipopt-3.12.4/ThirdParty/ASL
RUN ./get.ASL

# Configure and compile Ipopt
WORKDIR /home/docker/to_install/Ipopt/Ipopt-3.12.4/
RUN mkdir build
WORKDIR /home/docker/to_install/Ipopt/Ipopt-3.12.4/build
RUN ../configure --prefix=/home/docker/installed/Ipopt \
    && make \
    && make install

# Add folder to the path
#RUN export PATH=/home/docker/installed/Ipopt/bin:$PATH
#RUN export PATH="/home/docker/installed/Ipopt/bin":$PATH
ENV PATH="/home/docker/installed/Ipopt/bin:${PATH}"

WORKDIR /usr/docker/app

COPY /docker_files/module_control/requirements_control.txt .
COPY /commons/influxDB_to_icarus.py ./commons/
COPY /commons/parameters.py ./commons/
COPY control_mqtt.py .

RUN pip install --no-cache-dir -r requirements_control.txt

EXPOSE 1883

ENTRYPOINT ["python", "control_mqtt.py"]
CMD ["-H localhost", "-P 1883", "-c CONTROL_1", "-C 1", "-B 1", "-S gebouw", "-L l1"]