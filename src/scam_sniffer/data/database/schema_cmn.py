MIGRATION_LOCK = "SELECT pg_advisory_lock(hashtext('scam_sniffer_migrations'))"
MIGRATION_UNLOCK = "SELECT pg_advisory_unlock(hashtext('scam_sniffer_migrations'))"

MIGRATION_TABLE_CREATE = ("\n"
                          "CREATE TABLE IF NOT EXISTS database_migrations (\n"
                          "    version TEXT PRIMARY KEY,\n"
                          "    applied_time TIMESTAMPTZ NOT NULL DEFAULT NOW()\n"
                          ")\n")

MIGRATION_VERSION_READ = ("\n"
                          "SELECT EXISTS (\n"
                          "    SELECT 1\n"
                          "    FROM database_migrations\n"
                          "    WHERE version = $1\n"
                          ")\n")

MIGRATION_VERSION_CREATE = ("\n"
                            "INSERT INTO database_migrations (version)\n"
                            "VALUES ($1)\n")