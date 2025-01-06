FROM continuumio/miniconda3:4.12.0
RUN apt update && apt upgrade -y
RUN apt-get update
RUN apt-get install -y curl unzip

RUN mkdir /repository
RUN mkdir /local_tiff
RUN mkdir /local_tiff_cropped
RUN mkdir /local_metadata

RUN curl https://rclone.org/install.sh | bash
COPY ./rclone.conf /
ENV RCLONE_CONFIG=/rclone.conf

COPY ./environment.yml /
RUN conda env create -f /environment.yml

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
