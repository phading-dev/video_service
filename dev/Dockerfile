FROM node:20.12.1

RUN apt-get update && apt-get install -y ffmpeg unzip && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json .
COPY package-lock.json .
COPY bin/ .
RUN npm ci --omit=dev

EXPOSE 8080
CMD ["node", "main_bin"]
