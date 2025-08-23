"""
utils/ssm_params.py
Helpers to pull config/secrets from AWS Systems Manager Parameter Store.
"""
from __future__ import annotations
import os
from typing import Dict
import boto3


def _ssm():
    return boto3.client("ssm")


def load_env_from_ssm(param_map: Dict[str, str], with_decryption: bool = True) -> Dict[str, str]:
    """
    Fetch multiple parameters from SSM and set them into os.environ.

    param_map: mapping of ENV_VAR_NAME -> SSM_PARAMETER_NAME
      e.g., {
        "PROVIDER": "/jobs/PROVIDER",
        "OPENAI_API_KEY": "/jobs/OPENAI_API_KEY",
      }

    Returns:
      dict of ENV_VAR_NAME -> value that were successfully fetched.
    """
    if not param_map:
        return {}

    names = list(param_map.values())
    result: Dict[str, str] = {}

    # SSM can fetch up to 10 at a time
    for i in range(0, len(names), 10):
        chunk = names[i : i + 10]
        resp = _ssm().get_parameters(Names=chunk, WithDecryption=with_decryption)
        found = {p["Name"]: p["Value"] for p in resp.get("Parameters", [])}

        for env_var, ssm_name in param_map.items():
            if ssm_name in found:
                val = found[ssm_name]
                os.environ[env_var] = val
                result[env_var] = val
    print("keysss",result[0])
    return result
