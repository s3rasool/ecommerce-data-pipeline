db = db.getSiblingDB('behavioral');

db.createCollection('events', {
   validator: {
      $jsonSchema: {
         bsonType: 'object',
         required: ['timestamp', 'user_id', 'event_type', 'session_id'],
         properties: {
            // ---- Common Fields ----
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
            },

            // ---- page_view fields ----
            url_path: { bsonType: 'string' },
            duration_sec: { bsonType: 'int' },
            http_status: { bsonType: 'int' },

            // ---- product_search fields ----
            query: { bsonType: 'string' },
            results_count: { bsonType: 'int' },
            clicked_position: { bsonType: 'int' },

            // ---- add_to_cart / remove_from_cart fields ----
            product_id: { bsonType: 'string' },
            quantity: { bsonType: 'int' },
            cart_total_items: { bsonType: 'int' },

            // ---- cart_view fields ----
            cart_items: { bsonType: 'array' },
            cart_value: { bsonType: 'double' },

            // ---- wishlist_add fields ----
            wishlist_name: { bsonType: 'string' },

            // ---- checkout_start fields ----
            shipping_method: { bsonType: 'string' },

            // ---- payment_attempt fields ----
            payment_type: { bsonType: 'string' },
            success: { bsonType: 'bool' },
            error_code: { bsonType: ['string', 'null'] },

            // ---- order_complete fields ----
            order_id: { bsonType: 'string' },
            fulfillment_speed: { bsonType: 'string' },

            // ---- review_submit fields ----
            rating: { bsonType: 'int' },
            text_length: { bsonType: 'int' }
         }
      }
   }
});

db.events.createIndex({ "user_id": 1, "timestamp": -1 });
db.events.createIndex({ "session_id": 1, "timestamp": 1 });
db.events.createIndex({ "event_type": 1, "timestamp": -1 });
db.events.createIndex({ "timestamp": -1 });

print("MongoDB initialized successfully!");
