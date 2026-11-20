FROM node:24-bookworm-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --no-audit --no-fund
COPY tsconfig.json ./
COPY src ./src
COPY tests ./tests
COPY fixtures ./fixtures
RUN npm run check
USER node
EXPOSE 8080
CMD ["npm", "start"]
