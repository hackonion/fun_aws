import boto3
import os

def create_dynamo_table(table_name_value, enable_streams=False,
                        read_capacity=1,write_capacity=1,region=os.getenv("REGION")):
    
    table_name = table_name_value
    print(f"Creating table: {table_name}")
    try:
        client = boto3.client(service_name='dynamodb',
                              region_name=region)
        
        print(client.create_table(TableName=table_name,
                                  AttributeDefinitions=[{'AttributeName': 'EventId',
                                                         'AttributeType': 'S'},
                                                        {'AttributeName': 'EventDay',
                                                         'AttributeType': 'N'}],
                                  KeySchema=[{'AttributeName': 'EventId',
                                              'KeyType': 'HASH'},
                                             {'AttributeName': 'EventDay',
                                              'KeyType': 'RANGE'},],
                                  
                                  ProvisionedThroughput={'ReadCapacityUnits': read_capacity,
                                                         'WriteCapacityUnits': write_capacity}))
    except Exception as e:
        print(str(type(e)))
        print(e.__doc__)


def main():
    table_name = os.getenv('TABLE')
    create_dynamo_table(table_name, False, 1, 1)
    
if __name__ == '__main__':
    main()
    