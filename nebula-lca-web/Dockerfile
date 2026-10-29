FROM node:20-alpine AS builder

WORKDIR /app
ARG VITE_NEBULA_ONE_RETURN_ORIGIN
ENV VITE_NEBULA_ONE_RETURN_ORIGIN=$VITE_NEBULA_ONE_RETURN_ORIGIN
ARG VITE_WEB_ASSET_BASE=/
ENV VITE_WEB_ASSET_BASE=$VITE_WEB_ASSET_BASE

RUN npm config set registry https://registry.npmmirror.com

RUN npm install --global pnpm@10.27.0

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

COPY . .
RUN pnpm run build

FROM nginx:alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80
