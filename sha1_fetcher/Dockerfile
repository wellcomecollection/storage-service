FROM alpine

RUN apk add --update python3
RUN pip3 install boto3
RUN apk add --update bash

COPY runner.py /
COPY run.sh /

CMD ["/run.sh"]
