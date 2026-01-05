# contract-gen CLI

Generates data contracts from CSV, JSON, and databases (PostgreSQL/MySQL/SQLite). Auto-extracts schemas, types, and quality metrics.

**Examples:**

- `contract-gen source csv data.csv --id sales --output source.json --pretty`
- `contract-gen source database list --conn "postgresql://user:pass@host/db" --type postgresql`
- `contract-gen destination database --conn "postgresql://..." --table my_table --id dwh_sales --type postgresql --output dest.json`

**Key flags:** `--output` (save to file), `--pretty` (formatted JSON), `--id` (contract identifier)

**Commands:** `source csv|json|database`, `destination csv|database|api`, `validate`
