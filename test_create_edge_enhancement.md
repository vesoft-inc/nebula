# CREATE EDGE IF NOT EXISTS Enhancement Test

This document describes the expected behavior of the enhanced CREATE EDGE IF NOT EXISTS functionality.

## Changes Made

1. **Modified ExecResp structure** in `meta.thrift` to include an optional `skipped` boolean field
2. **Updated CreateEdgeProcessor** to set the `skipped` flag when edge creation is skipped due to IF NOT EXISTS
3. **Enhanced MetaClient::createEdgeSchema** to check the `skipped` flag and return -1 as a sentinel value when creation is skipped
4. **Improved CreateEdgeExecutor** to provide different user-friendly messages for created vs skipped edges

## Expected Behavior

### Before Enhancement
```sql
CREATE EDGE IF NOT EXISTS follow(degree int);
-- Result: "Execution succeeded" (even when the edge already exists)
```

### After Enhancement
```sql
-- First execution (edge doesn't exist)
CREATE EDGE IF NOT EXISTS follow(degree int);
-- Result: 
-- +----------------------------------+
-- | Result                           |
-- +----------------------------------+
-- | Edge 'follow' created successfully |
-- +----------------------------------+

-- Second execution (edge already exists)
CREATE EDGE IF NOT EXISTS follow(degree int);
-- Result:
-- +------------------------------------------------+
-- | Result                                         |
-- +------------------------------------------------+
-- | Edge 'follow' already exists, creation skipped |
-- +------------------------------------------------+
```

## Technical Details

- The `skipped` flag in `ExecResp` indicates whether an operation was skipped due to IF NOT EXISTS
- MetaClient returns -1 as a sentinel value when creation is skipped
- CreateEdgeExecutor provides different result messages based on the response value
- The enhancement maintains backward compatibility while providing clearer feedback to users

## Testing Scenarios

1. **Create new edge with IF NOT EXISTS**
   - Should display "Edge 'name' created successfully"

2. **Create existing edge with IF NOT EXISTS**
   - Should display "Edge 'name' already exists, creation skipped"

3. **Create existing edge without IF NOT EXISTS**
   - Should fail with appropriate error message (existing behavior)

4. **Create new edge without IF NOT EXISTS**
   - Should display "Edge 'name' created successfully"