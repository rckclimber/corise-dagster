from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    graph,
    op,
    schedule,
    String,
    sensor,
    static_partitioned_config,
)
from workspaces.config import REDIS, S3
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    description="Returns data from S3 bucket",
    config_schema={"s3_key": String}, 
    required_resource_keys={"s3"},
    out={"stocks":Out(dagster_type=List[Stock], description="List of Stocks")}
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    ''' Get in data, process into custom data type, input via config_schema'''
    stocks_strings = list(context.resources.s3.get_data(context.op_config["s3_key"]))
    return [Stock.from_list(x) for x in stocks_strings]


@op(
    description="Returns the Aggregation object with date and highest stock value for that date ",
    ins={"stocks":In(dagster_type=List[Stock])},
    out={"Highest_Stock_value_per_day":Out(dagster_type=Aggregation)},
)
def process_data(context: OpExecutionContext, stocks: List[Stock]) -> Aggregation:
    sorted_stocks = sorted(stocks, key=lambda s: s.high, reverse=True)
    max_stock = sorted_stocks[0]
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(
    description="Uploads highest stock value per day (Aggregation class type) to Redis",
    required_resource_keys={"redis"},
    ins={"aggregation":In(dagster_type=Aggregation)},
)
def put_redis_data(context, aggregation):
    """Uploading an aggregation to Redis"""
    return context.resources.redis.put_data(str(aggregation.date),str(aggregation.high))


@op(
    description="Uploads highest stock value per day (Aggregation class type) to S3 data lake",
    ins={"aggregation":In(dagster_type=Aggregation)},
    required_resource_keys={"s3"},
)
def put_s3_data(context, aggregation):
    return context.resources.s3.put_data(key_name=aggregation.date.strftime("%Y%m%d"),data=aggregation)

@graph
def machine_learning_graph():
    #stocks = get_s3_data()
    aggregation = process_data(get_s3_data())
    put_redis_data(aggregation)
    put_s3_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys=[str(pk) for pk in range(1,11)])
def docker_config(partition_key:int):
    return {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
}


machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={"s3":mock_s3_resource,"redis":ResourceDefinition.mock_resource()},
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker_config,
    resource_defs={"s3":s3_resource,"redis":redis_resource},
    op_retry_policy = RetryPolicy(max_retries=10, delay=1)
)



machine_learning_schedule_local = ScheduleDefinition(job=machine_learning_job_local,cron_schedule="*/15 * * * *")


@schedule(job=machine_learning_job_docker,cron_schedule="0 * * * *")
def machine_learning_schedule_docker():
    for pk in docker_config.get_partition_keys():
        yield RunRequest(run_key=pk, run_config=docker_config.get_run_config(pk))


@sensor(job=machine_learning_job_docker, minimum_interval_seconds=30)
def machine_learning_sensor_docker():
    s3_keys = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://localstack:4566")

    if not s3_keys:
        yield SkipReason("No new s3 files found in bucket.")
    else:
        for k in s3_keys:
            yield RunRequest(
                run_key=k,
                run_config={
                    "resources": {
                        "s3": {"config": S3},
                        "redis": {"config": REDIS},
                    },
                    "ops": {"get_s3_data": {"config": {"s3_key": k}}},
                })
