from boto3 import resource
import os
class DynamoRepository:
    
    def __init__(self, target_dynamo_table, region = os.getenv('REGION'),):
        self.dynamodb = resource(service_name='dynamodb',
                                 region_name=region)
        
        self.target_dynamo_table = target_dynamo_table
        self.table = self.dynamodb.Table(self.target_dynamo_table)
        
    def update_dynamo_event_counter(self, event_name,
                                    event_datetime, event_count=1):
       return self.table.update_item(
           Key={
               'EventId': event_name,
               'EventDay': event_datetime
               },
           ExpressionAttributeValues={":eventCount": event_count},
           UpdateExpression="ADD EventCount :eventCount")

def main():
    table_name = os.getenv('TABLE')
    dynamo_repo = DynamoRepository(table_name)
    print(dynamo_repo.update_dynamo_event_counter('324', 20171001))
    print(dynamo_repo.update_dynamo_event_counter('324', 20171001, 2))
    print(dynamo_repo.update_dynamo_event_counter('324', 20171002, 5))
    
    
if __name__ == '__main__':
    main()