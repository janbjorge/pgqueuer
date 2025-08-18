import uuid

import pytest
from pydantic import BaseModel, ValidationError

from pgqueuer.db import AsyncpgDriver, PsycopgDriver


class PayloadModel(BaseModel):
    payload: bytes


@pytest.mark.asyncio
async def test_psycopg_driver_bytea_type(
    apgdriver: AsyncpgDriver,
    psycopg_driver: PsycopgDriver,
) -> None:
    table = "_pgq_probe_fixture"
    await apgdriver.execute(f"DROP TABLE IF EXISTS {table}")
    await apgdriver.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            payload bytea NOT NULL
        );
        """
    )

    inserted_id = uuid.uuid4()
    payload = b'{"hello":"world"}'
    await apgdriver.execute(
        f"INSERT INTO {table} (id, payload) VALUES ($1, $2)",
        inserted_id,
        payload,
    )

    # Fetch with PsycopgDriver to replicate ECS behavior
    rows = await psycopg_driver.fetch(f"SELECT payload FROM {table} WHERE id = $1", inserted_id)
    assert len(rows) == 1
    row = dict(rows[0])
    value = row["payload"]

    print(f"[bytea probe] psycopg_driver returned: {type(value).__name__}")

    if isinstance(value, (bytes, bytearray, str)):
        # Should pass validation
        m = PayloadModel.model_validate({"payload": value})
        assert m.payload == payload
    else:
        assert isinstance(value, memoryview), f"Unexpected type: {type(value)}"
        # Should fail validation exactly like in ECS logs
        with pytest.raises(ValidationError) as ei:
            PayloadModel.model_validate({"payload": value})
        assert "Expected bytes, bytearray or str" in str(ei.value)
