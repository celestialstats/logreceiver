FROM golang:onbuild

MAINTAINER Caesar Kabalan <caesar.kabalan@gmail.com>

ADD . /go/src/github.com/celestialstats/logreceiver

RUN go install github.com/celestialstats/logreceiver

CMD [ "/go/bin/logreceiver" ]
