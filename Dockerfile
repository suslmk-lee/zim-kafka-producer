FROM golang:1.22-alpine AS builder

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /build

COPY go.mod go.sum main.go ./
COPY common ./common
COPY *.properties ./

RUN ls -al
RUN go mod download
RUN go mod tidy

#COPY --from=itinance/swag /root/swag /usr/local/bin

#RUN swag init

RUN go build -o main .

WORKDIR /dist

RUN cp /build/main .

RUN cp /build/*.properties .

FROM scratch

COPY --from=builder /dist/main .

COPY --from=builder /dist/*.properties .

ENV PROFILE=prod \
    DATABASE_URL=${DATABASE_URL} \
    DATABASE_NAME=cp \
    DATABASE_TERRAMAN_ID=${DATABASE_TERRAMAN_ID} \
    DATABASE_TERRAMAN_PASSWORD=${DATABASE_TERRAMAN_PASSWORD}

ENTRYPOINT ["/main"]
