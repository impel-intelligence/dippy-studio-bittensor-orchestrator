FROM arigaio/atlas:0.37.0-alpine

WORKDIR /workspace

# Copy Atlas configuration and migrations into the image
COPY atlas.hcl /workspace/atlas.hcl
COPY migrations /workspace/migrations

ENTRYPOINT ["/bin/sh", "-c"]
CMD ["atlas migrate hash --dir file:///workspace/migrations && atlas migrate apply --dir file:///workspace/migrations --url \"$DATABASE_URL\""]
