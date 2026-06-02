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

### Naming Breakdown

| Part | Description | Example |
|------|-------------|---------|
| `ecommerce` | Project namespace | Prevents collisions with other projects |
| `users` / `products` / `orders` / `events` | Data entity | What the data represents |
| `ingest` | Operation | Data being brought into the system |

