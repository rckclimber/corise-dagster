from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
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
    ins={"Stocks":In(dagster_type=Stock)},
    out={"Highest Stock value per day":out(dagster_type=Aggregation)}
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
    return context.resources.redis.put_data(str(aggregation.date),aggregation)


@op(
    description="Uploads highest stock value per day (Aggregation class type) to S3 data lake",
    ins={"aggregation":In(dagster_type=Aggregation)},
    required_resource_keys={"s3"},
)
def put_s3_data(context, aggregation):
    return context.resources.s3.put_data(str(aggregation.date),aggregation.high)

@graph
def machine_learning_graph():
    #stocks = get_s3_data()
    aggregation = process_data(get_s3_data())
    put_redis_data(aggregation)
    put_s3_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
    #"ops": {"put_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={"s3":mock_s3_resource,"redis":ResourceDefinition.mock_resource()},
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker,
    resource_defs={"s3":s3_resource,"redis":redis_resource},
)
