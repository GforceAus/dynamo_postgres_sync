import boto3


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