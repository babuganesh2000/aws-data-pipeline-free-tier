import boto3
import json
import logging
from datetime import datetime, time
from typing import List, Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
rds_client = boto3.client('rds')
cloudwatch_client = boto3.client('cloudwatch')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Auto-shutdown Lambda function to stop RDS instances during off-hours
    to minimize costs. Triggered by CloudWatch Events/EventBridge.
    """
    
    try:
        # Get all RDS instances with AutoShutdown tag
        db_instances = get_auto_shutdown_instances()
        
        if not db_instances:
            logger.info("No instances found with AutoShutdown tag")
            return create_response(200, "No instances to process")
        
        # Check current time and determine action
        current_hour = datetime.utcnow().hour
        action = determine_action(current_hour)
        
        results = []
        
        for instance in db_instances:
            try:
                instance_id = instance['DBInstanceIdentifier']
                current_status = instance['DBInstanceStatus']
                
                if action == 'stop' and current_status == 'available':
                    result = stop_instance(instance_id)
                    results.append(result)
                    
                elif action == 'start' and current_status == 'stopped':
                    result = start_instance(instance_id)
                    results.append(result)
                    
                else:
                    logger.info(f"No action needed for {instance_id} (status: {current_status})")
                    
            except Exception as e:
                logger.error(f"Failed to process instance {instance_id}: {str(e)}")
                results.append({
                    'instance_id': instance_id,
                    'action': 'failed',
                    'error': str(e)
                })
        
        # Send metrics to CloudWatch
        send_metrics(results)
        
        return create_response(200, {
            'action': action,
            'processed_instances': len(results),
            'results': results,
            'timestamp': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Auto-shutdown function failed: {str(e)}")
        return create_response(500, {'error': str(e)})

def get_auto_shutdown_instances() -> List[Dict[str, Any]]:
    """
    Get all RDS instances tagged for auto-shutdown.
    """
    
    try:
        response = rds_client.describe_db_instances()
        auto_shutdown_instances = []
        
        for instance in response['DBInstances']:
            instance_arn = instance['DBInstanceArn']
            
            # Get tags for the instance
            tags_response = rds_client.list_tags_for_resource(ResourceName=instance_arn)
            tags = {tag['Key']: tag['Value'] for tag in tags_response['TagList']}
            
            # Check if instance has AutoShutdown tag
            if tags.get('AutoShutdown', '').lower() == 'true':
                auto_shutdown_instances.append(instance)
                
        return auto_shutdown_instances
        
    except Exception as e:
        logger.error(f"Failed to get auto-shutdown instances: {str(e)}")
        raise

def determine_action(current_hour: int) -> str:
    """
    Determine whether to start or stop instances based on time.
    Stop between 6 PM - 8 AM UTC (off-hours)
    Start at 8 AM UTC (business hours)
    """
    
    # Off-hours: 18:00 (6 PM) to 08:00 (8 AM) UTC
    if current_hour >= 18 or current_hour < 8:
        return 'stop'
    else:
        return 'start'

def stop_instance(instance_id: str) -> Dict[str, Any]:
    """
    Stop an RDS instance.
    """
    
    try:
        logger.info(f"Stopping RDS instance: {instance_id}")
        
        response = rds_client.stop_db_instance(
            DBInstanceIdentifier=instance_id,
            DBSnapshotIdentifier=f"{instance_id}-auto-shutdown-{int(time.time())}"
        )
        
        return {
            'instance_id': instance_id,
            'action': 'stopped',
            'status': 'success',
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to stop instance {instance_id}: {str(e)}")
        raise

def start_instance(instance_id: str) -> Dict[str, Any]:
    """
    Start an RDS instance.
    """
    
    try:
        logger.info(f"Starting RDS instance: {instance_id}")
        
        response = rds_client.start_db_instance(
            DBInstanceIdentifier=instance_id
        )
        
        return {
            'instance_id': instance_id,
            'action': 'started',
            'status': 'success',
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to start instance {instance_id}: {str(e)}")
        raise

def send_metrics(results: List[Dict[str, Any]]) -> None:
    """
    Send custom metrics to CloudWatch for monitoring.
    """
    
    try:
        stopped_count = len([r for r in results if r.get('action') == 'stopped'])
        started_count = len([r for r in results if r.get('action') == 'started'])
        failed_count = len([r for r in results if r.get('action') == 'failed'])
        
        # Send metrics
        cloudwatch_client.put_metric_data(
            Namespace='DataPipeline/AutoShutdown',
            MetricData=[
                {
                    'MetricName': 'InstancesStopped',
                    'Value': stopped_count,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'InstancesStarted',
                    'Value': started_count,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'FailedOperations',
                    'Value': failed_count,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                }
            ]
        )
        
        logger.info(f"Sent metrics: {stopped_count} stopped, {started_count} started, {failed_count} failed")
        
    except Exception as e:
        logger.error(f"Failed to send metrics: {str(e)}")

def create_response(status_code: int, body: Any) -> Dict[str, Any]:
    """
    Create standardized Lambda response.
    """
    
    return {
        'statusCode': status_code,
        'body': json.dumps(body) if isinstance(body, (dict, list)) else body
    }