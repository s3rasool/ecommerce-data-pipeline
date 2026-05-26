// Switch to the events database
db = db.getSiblingDB('events');

// ============================================
// 1. page_view
// ============================================
db.createCollection('page_views', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['timestamp', 'user_id', 'event_type'],
      properties: {
        event_type: { enum: ['page_view'] }
      }
    }
  }
});

// ============================================
// 2. product_search
// ============================================
db.createCollection('product_searches', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['timestamp', 'user_id', 'event_type'],
      properties: {
        event_type: { enum: ['product_search'] }
      }
    }
  }
});

// ============================================
// 3. add_to_cart
// ============================================
db.createCollection('add_to_cart', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['timestamp', 'user_id', 'event_type', 'product_id'],
      properties: {
        event_type: { enum: ['add_to_cart'] }
      }
    }
  }
});

// ============================================
// 4. remove_from_cart
// ============================================
db.createCollection('remove_from_cart', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['timestamp', 'user_id', 'event_type', 'product_id'],
      properties: {
        event_type: { enum: ['remove_from_cart'] }
      }
    }
  }
});

// ============================================
// 5. cart_view
// ============================================
db.createCollection('cart_views', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['timestamp', 'user_id', 'event_type'],
      properties: {
        event_type: { enum: ['cart_view'] }
      }
    }
  }
});

// ============================================
// 6. wishlist_add
// ============================================
db.createCollection('wishlist_adds', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['timestamp', 'user_id', 'event_type', 'product_id'],
      properties: {
        event_type: { enum: ['wishlist_add'] }
      }
    }
  }
});

// ============================================
// 7. checkout_start
// ============================================
db.createCollection('checkout_starts', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['timestamp', 'user_id', 'event_type'],
      properties: {
        event_type: { enum: ['checkout_start'] }
      }
    }
  }
});

// ============================================
// 8. payment_attempt
// ============================================
db.createCollection('payment_attempts', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['timestamp', 'user_id', 'event_type'],
      properties: {
        event_type: { enum: ['payment_attempt'] }
      }
    }
  }
});

// ============================================
// 9. order_complete
// ============================================
db.createCollection('order_completes', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['timestamp', 'user_id', 'event_type', 'order_id'],
      properties: {
        event_type: { enum: ['order_complete'] }
      }
    }
  }
});

// ============================================
// 10. review_submit
// ============================================
db.createCollection('review_submits', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['timestamp', 'user_id', 'event_type', 'product_id'],
      properties: {
        event_type: { enum: ['review_submit'] }
      }
    }
  }
});

// ============================================
// Create indexes for common queries
// ============================================
db.page_views.createIndex({ timestamp: -1 });

db.product_searches.createIndex({ timestamp: -1 });

db.add_to_cart.createIndex({ timestamp: -1 });

db.remove_from_cart.createIndex({ timestamp: -1 });

db.cart_views.createIndex({ timestamp: -1 });

db.wishlist_adds.createIndex({ timestamp: -1 });

db.checkout_starts.createIndex({ timestamp: -1 });

db.payment_attempts.createIndex({ timestamp: -1 });

db.order_completes.createIndex({ timestamp: -1 });

db.review_submits.createIndex({ timestamp: -1 });

print('All collections created with validators and indexes!');
