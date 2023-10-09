from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO, StringIO
from typing import List
import boto3
import boto3.s3.transfer as s3transfer
import botocore
import botocore.exceptions as botocore_exceptions
import random
import datetime as dt
import pandas as pd
import logging as log
from dotenv import dotenv_values

log.basicConfig(
    level=log.INFO,
)

env = dotenv_values()
BUCKET = str(env.get("BUCKET", ""))
KEY = env["KEY"]
SECRET = env["SECRET"]
SESSION = env["SESSION"]


def transfer_upload(
    data: pd.DataFrame,
    split_cols: List[str],
    manager: s3transfer.TransferManager = None,
    run_prefix: str = "",
) -> dt.timedelta:
    """splits a dataframe by the column(s) provided and uploads using a transfer manager
    Single threaded execution, but manager underneath might be multithreaded.
    """
    groups = data.groupby(split_cols)
    t0 = dt.datetime.utcnow()
    futures = []
    for vars, group in groups:
        filename = "/".join([f"{k}={v}" for k, v in zip(split_cols, vars)])
        filename += "/data.csv"
        filename = f"run={run_prefix}/" + "method=manager/" + filename
        csvio = BytesIO()
        group.to_csv(csvio)
        futures.append(manager.upload(fileobj=csvio, bucket=BUCKET, key=filename))
    manager.shutdown()

    t1 = dt.datetime.utcnow()
    return t1 - t0


def multithreaded_upload(
    data: pd.DataFrame,
    split_cols: List[str],
    session: boto3.Session,
    conc: int = 8,
    multi_clients: bool = False,
    run_prefix: str = "",
):
    if multi_clients:
        creds = session.get_credentials()
        sessions = [
            boto3.Session(creds.access_key, creds.secret_key, creds.token)
            for _ in range(conc)
        ]
        clients = [session.client("s3") for session in sessions]
    else:
        clients = [session.client("s3")]

    groups = data.groupby(split_cols)
    t0 = dt.datetime.utcnow()
    futures = []
    with ThreadPoolExecutor(conc) as executor:
        for vars, group in groups:
            filename = "/".join([f"{k}={v}" for k, v in zip(split_cols, vars)])
            filename += "/data.csv"
            filename = (
                f"run={run_prefix}/"
                + f"method=client{f's[{len(clients)}]' if len(clients) > 1 else ''}/"
                + filename
            )
            futures.append(
                executor.submit(
                    _upload_using_client,
                    client=random.choice(clients),
                    data=group,
                    bucket=BUCKET,
                    key=filename,
                )
            )
        for res in as_completed(futures):
            _ = res.result()

    t1 = dt.datetime.utcnow()
    return t1 - t0


def _upload_using_client(
    client: boto3.client, data: pd.DataFrame, key: str, bucket: str
):
    csvio = StringIO()
    data.to_csv(csvio)
    client.put_object(
        Body=csvio.getvalue().encode(),
        Bucket=bucket,
        Key=key,
    )


def main(concurrency: int = 20):
    data = pd.read_csv("https://github.com/mwaskom/seaborn-data/raw/master/taxis.csv")
    grouping_cols = ["color", "payment", "pickup_zone"]

    CONCURRENCY = concurrency
    try:
        session = boto3.Session(
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET,
            aws_session_token=SESSION,
            region_name="eu-west-1",
        )
    except botocore_exceptions.ProfileNotFound:
        session = boto3.Session(
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET,
            aws_session_token=SESSION,
            region_name="eu-west-1",
        )

    botocore_config = botocore.config.Config(max_pool_connections=CONCURRENCY)
    s3client = session.client("s3", config=botocore_config)
    log.info(f"Running with concurrency {concurrency}")
    transfer_config = s3transfer.TransferConfig(
        use_threads=True,
        max_concurrency=CONCURRENCY,
    )
    s3t = s3transfer.create_transfer_manager(s3client, transfer_config)
    run_prefix = "".join(str(random.randint(0, 255)) for i in range(4))
    bench_transfer = transfer_upload(
        data, grouping_cols, manager=s3t, run_prefix=run_prefix
    )
    bench_multithread_client = multithreaded_upload(
        data,
        grouping_cols,
        session=session,
        conc=CONCURRENCY,
        multi_clients=True,
        run_prefix=run_prefix,
    )
    bench_multithread_single_client = multithreaded_upload(
        data,
        grouping_cols,
        session=session,
        conc=CONCURRENCY,
        multi_clients=False,
        run_prefix=run_prefix,
    )

    for method, result in [
        ("transfer", bench_transfer),
        ("multithread_client", bench_multithread_client),
        ("multithread_shared_client", bench_multithread_single_client),
    ]:
        log.info(f"Method {method} took {result} to complete")


if __name__ == "__main__":
    main(4)
    main(8)
    main(20)
