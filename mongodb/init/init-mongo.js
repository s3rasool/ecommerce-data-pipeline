db = db.getSiblingDB('store_behavior');

db.createCollection('behavioral_events', {
   validator: {
      $jsonSchema: {
         bsonType: 'object',
         required: ['timestamp', 'user_id', 'event_type', 'session_id'],
         properties: {
            user_id: { bsonType: 'string' },
            session_id: { bsonType: 'string' },
            timestamp: { bsonType: 'date' },
            device: { bsonType: 'string' },
            event_type: {
               bsonType: 'string',
               enum: [
                  'page_view', 'product_search', 'add_to_cart',
                  'remove_from_cart', 'cart_view', 'wishlist_add',
                  'checkout_start', 'payment_attempt', 'order_complete',
                  'review_submit'
               ]
            }
         }
      }
   }
});

db.behavioral_events.createIndex({ "user_id": 1, "timestamp": -1 });
db.behavioral_events.createIndex({ "session_id": 1, "timestamp": 1 });
db.behavioral_events.createIndex({ "event_type": 1, "timestamp": -1 });
db.behavioral_events.createIndex({ "timestamp": -1 });

print("MongoDB initialized successfully!");
