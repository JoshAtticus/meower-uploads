# Build stage
FROM golang AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o Meower-Uploads

# Production stage
FROM alpine
WORKDIR /app
RUN apk add --no-cache coreutils file imagemagick ffmpeg
COPY policy.xml /etc/ImageMagick-7/policy.xml
COPY --from=builder /app/Meower-Uploads /app/Meower-Uploads
ENTRYPOINT ["/app/Meower-Uploads"]
