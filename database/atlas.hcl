env "default" {
  src = "file://migrations"
  # Atlas spins up a temporary postgres:18 container for migration planning/validation
  # This is separate from the actual database
  # To apply migrations to your real DB, use: atlas migrate apply --url "postgres://..."
  dev = "docker://postgres/18/dev?search_path=public"
}

migrate {
  dir = "file://migrations"
  format = atlas
}
