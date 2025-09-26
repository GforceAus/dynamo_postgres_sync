import boto3
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any


def print_table_indexes(table_names):
    """Print indexed fields for each DynamoDB table."""
    dynamodb = boto3.client('dynamodb')
    
    for table_name in table_names:
        print(f"\n=== {table_name} ===")
        try:
            response = dynamodb.describe_table(TableName=table_name)
            table = response['Table']
            
            # Print hash key and range key
            key_schema = table['KeySchema']
            for key in key_schema:
                key_type = key['KeyType']
                attr_name = key['AttributeName']
                print(f"{key_type}: {attr_name}")
            
            # Print Global Secondary Indexes
            if 'GlobalSecondaryIndexes' in table:
                print("Global Secondary Indexes:")
                for gsi in table['GlobalSecondaryIndexes']:
                    print(f"  Index: {gsi['IndexName']}")
                    for key in gsi['KeySchema']:
                        key_type = key['KeyType']
                        attr_name = key['AttributeName']
                        print(f"    {key_type}: {attr_name}")
            
            # Print Local Secondary Indexes
            if 'LocalSecondaryIndexes' in table:
                print("Local Secondary Indexes:")
                for lsi in table['LocalSecondaryIndexes']:
                    print(f"  Index: {lsi['IndexName']}")
                    for key in lsi['KeySchema']:
                        key_type = key['KeyType']
                        attr_name = key['AttributeName']
                        print(f"    {key_type}: {attr_name}")
                        
        except Exception as e:
            print(f"Error describing table {table_name}: {e}")


def scan_table_segment(table_name: str, segment: int, total_segments: int) -> List[Dict[str, Any]]:
    """Scan a segment of a DynamoDB table."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    items = []
    scan_count = 0
    scan_kwargs = {
        'Segment': segment,
        'TotalSegments': total_segments
    }
    
    try:
        while True:
            response = table.scan(**scan_kwargs)
            batch_items = response['Items']
            items.extend(batch_items)
            scan_count += 1
            
            # Progress indicator every 10 scans (roughly every 10MB of data)
            if scan_count % 10 == 0:
                print(f"  Segment {segment}: {len(items):,} items ({scan_count} scans)")
            
            if 'LastEvaluatedKey' not in response:
                break
                
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            
    except Exception as e:
        print(f"Error scanning segment {segment} of table {table_name}: {e}")
        
    return items


def dump_table_data(table_name: str, max_workers: int = 4) -> List[Dict[str, Any]]:
    """Efficiently dump all data from a DynamoDB table using parallel scanning."""
    start_time = time.time()
    print(f"Starting parallel scan of table: {table_name} ({max_workers} segments)")
    
    # Use parallel scanning for better performance
    total_segments = max_workers
    all_items = []
    completed_segments = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit scan tasks for each segment
        future_to_segment = {
            executor.submit(scan_table_segment, table_name, segment, total_segments): segment
            for segment in range(total_segments)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_segment):
            segment = future_to_segment[future]
            try:
                items = future.result()
                all_items.extend(items)
                completed_segments += 1
                
                elapsed = time.time() - start_time
                items_per_sec = len(all_items) / elapsed if elapsed > 0 else 0
                
                print(f"✓ Segment {segment} complete: {len(items):,} items | "
                      f"Progress: {completed_segments}/{total_segments} | "
                      f"Total: {len(all_items):,} items | "
                      f"Rate: {items_per_sec:.0f} items/sec")
                      
            except Exception as e:
                print(f"✗ Segment {segment} failed: {e}")
    
    elapsed_total = time.time() - start_time
    avg_rate = len(all_items) / elapsed_total if elapsed_total > 0 else 0
    
    print(f"📊 Scan completed in {elapsed_total:.1f}s | "
          f"Total: {len(all_items):,} items | "
          f"Average rate: {avg_rate:.0f} items/sec")
    
    return all_items


def save_table_data(table_name: str, items: List[Dict[str, Any]], output_file: str = None):
    """Save table data to a JSON file."""
    if output_file is None:
        output_file = f"{table_name}_data.json"
    
    # Create directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # Convert Decimal types to float for JSON serialization
    def decimal_default(obj):
        if hasattr(obj, '__float__'):
            return float(obj)
        raise TypeError
    
    with open(output_file, 'w') as f:
        json.dump(items, f, default=decimal_default, indent=2)
    
    print(f"Saved {len(items)} items to {output_file}")