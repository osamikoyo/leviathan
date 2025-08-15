# RabbitMQ Broadcasting Architecture

## Overview

This system uses RabbitMQ's **fanout exchange** pattern to broadcast write requests to all available consumer nodes. This ensures that every consumer receives and processes the same write request, which is essential for maintaining data consistency across all database nodes.

## Architecture Components

### Producer (Heart)
- **Purpose**: Receives write requests and broadcasts them to all consumers
- **Exchange**: Uses fanout exchange `write_requests_fanout`
- **Behavior**: Publishes messages to the exchange with empty routing key
- **Result**: Every bound queue receives a copy of every message

### Consumers (Nodes)
- **Purpose**: Process write requests from the broadcast
- **Queue Pattern**: Each consumer creates a unique queue `{base_queue_name}_node_{node_id}`
- **Binding**: All queues are bound to the same fanout exchange
- **Processing**: Each consumer processes every message independently

## Message Flow

```
Producer → Fanout Exchange → Queue 1 (node_1) → Consumer 1
                          → Queue 2 (node_2) → Consumer 2  
                          → Queue 3 (node_3) → Consumer 3
                          → ...
```

## Configuration

### Heart Configuration (heart.yaml)
```yaml
exchange: "write_requests_fanout"  # Fanout exchange name
queue_name: "write_requests"       # Base queue name prefix
```

### Node Configuration (node.yaml)
```yaml
node_id: 1                        # Unique node identifier
queue_name: "write_requests"       # Base queue name
```

The actual queue name becomes: `write_requests_node_1`

## Key Benefits

1. **Guaranteed Delivery**: Every active consumer receives every message
2. **Fault Tolerance**: If one consumer fails, others continue processing
3. **Scalability**: Easy to add/remove consumers without affecting others
4. **Data Consistency**: All nodes process the same write operations

## Important Notes

- Each consumer must have a unique `node_id` to avoid queue naming conflicts
- All consumers should connect to the same RabbitMQ instance
- The exchange is declared as durable to survive RabbitMQ restarts
- Individual queues are also durable to prevent message loss

## Error Handling

- **Malformed messages**: Rejected without requeuing (likely JSON parsing errors)
- **Processing failures**: Rejected with requeuing (temporary database issues)
- **Connection failures**: Consumer will automatically reconnect and resume processing

## Monitoring

Monitor the following metrics:
- Exchange message rate (should match producer rate)
- Queue depths (should be similar across all consumers)
- Consumer lag (time between message arrival and processing)
- Failed message rates per consumer
