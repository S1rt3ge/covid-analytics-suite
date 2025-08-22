import snowflake.connector
import pandas as pd
import numpy as np
from typing import Tuple
from app.config import SF_CFG, REQUIRED_SF
import logging

logger = logging.getLogger(__name__)

def _sf_conn():
    missing = [k for k in REQUIRED_SF if not SF_CFG.get(k)]
    if missing:
        raise RuntimeError(f"Snowflake ENV is incomplete: missing {missing}")
    return snowflake.connector.connect(
        account=SF_CFG["account"],
        user=SF_CFG["user"],
        password=SF_CFG["password"],
        warehouse=SF_CFG["warehouse"],
        database=SF_CFG["database"],
        schema=SF_CFG["schema"],
        role=SF_CFG.get("role"),
        autocommit=True,
    )

def sf_query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    with _sf_conn() as con:
        cur = con.cursor()
        try:
            cur.execute(sql, params)
            try:
                df = cur.fetch_pandas_all()
            except Exception as e:
                logger.warning(f"fetch_pandas_all failed: {e}. Falling back to manual DataFrame creation.")
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=cols)
        finally:
            cur.close()
    return df