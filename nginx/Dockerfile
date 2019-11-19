FROM nginx:1.15.8-alpine

COPY nginx.conf /etc/nginx/nginx.conf.template

RUN apk add --update bash gettext
CMD /bin/bash -c "envsubst '\$APP_HOST \$APP_PORT' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf && cat /etc/nginx/nginx.conf && nginx -g 'daemon off;'"
