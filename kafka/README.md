## Kafka Topic Naming Convention

Topics follow the pattern: `<namespace>.<entity>.<operation>`

### Format Rules
- **Case:** snake_case
- **Separator:** `.` (dot)
- **Structure:** `ecommerce.<entity>.<operation>`

### Current Topics

| Topic | Entity | Operation | Description | Data Source |
|-------|--------|-----------|-------------|-------------|
| `ecommerce.users.ingest` | Users | ingest | New user records | CSV |
| `ecommerce.products.ingest` | Products | ingest | Product catalog updates (full reload) | CSV |
| `ecommerce.orders.ingest` | Orders | ingest | New orders | CSV |
| `ecommerce.events.ingest` | Events | ingest | User behavioral events | JSON |
| `ecommerce.users.migrate` | Users | migrate | Users extracted from PostgreSQL | PostgreSQL |
| `ecommerce.products.migrate` | Products | migrate | Products extracted from PostgreSQL | PostgreSQL |
| `ecommerce.orders.migrate` | Orders | migrate | Orders extracted from PostgreSQL | PostgreSQL |
| `ecommerce.events.migrate` | Events | migrate | Events extracted from MongoDB | MongoDB |

### Naming Breakdown

| Part | Description | Example |
|------|-------------|---------|
| `ecommerce` | Project namespace | Prevents collisions with other projects |
| `users` / `products` / `orders` / `events` | Data entity | What the data represents |
| `ingest` / `migrate` | Operation | `ingest` = raw data into system, `migrate` = extracted for downstream |
