#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

try:
    from obs import ObsClient
except ImportError:
    print("Error: 'esdk-obs-python' is not installed.")
    print("Please install it via: pip install esdk-obs-python")
    sys.exit(1)

def get_credentials():
    passwd_file = os.path.expanduser("~/.passwd-obsfs")
    if not os.path.exists(passwd_file):
        print(f"Error: Credentials file {passwd_file} not found.")
        sys.exit(1)
    
    with open(passwd_file, "r") as f:
        line = f.readline().strip()
        if ":" not in line:
            print("Error: Invalid credentials format in ~/.passwd-obsfs. Expected AK:SK")
            sys.exit(1)
        return line.split(":", 1)

def upload_package():
    # 配置信息
    bucket_name = "csdk-perf"
    file_path = os.path.abspath("../obsa_test_framework_offline.tar.gz")
    object_key = os.path.basename(file_path)

    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found. Please run packaging workflow first.")
        sys.exit(1)

    ak, sk = get_credentials()

    # 首先探测桶所在的 region
    # 默认使用一个全局可达的 endpoint 来探测
    probe_endpoint = "obs.myhuaweicloud.com"
    probe_client = ObsClient(access_key_id=ak, secret_access_key=sk, server=probe_endpoint)
    
    print(f"Probing location for bucket: {bucket_name}...")
    loc_res = probe_client.getBucketLocation(bucket_name)
    probe_client.close()

    if loc_res.status >= 300:
        print(f"Failed to get bucket location. Status: {loc_res.status}")
        # 如果探测失败，尝试默认 endpoint
        region = "cn-north-4"
        print(f"Falling back to default region: {region}")
    else:
        region = loc_res.body.location
        print(f"Bucket location detected: {region}")

    endpoint = f"obs.{region}.myhuaweicloud.com"
    
    # 重新初始化 ObsClient 使用正确的 endpoint
    obs_client = ObsClient(access_key_id=ak, secret_access_key=sk, server=endpoint)

    try:
        print(f"Starting upload: {file_path} -> obs://{bucket_name}/{object_key} (Endpoint: {endpoint})")
        
        # 1. 上传文件并设置 ACL (通过 headers)
        headers = {'x-obs-acl': 'public-read'}
        res = obs_client.putFile(bucket_name, object_key, file_path, headers=headers)

        if res.status < 300:
            print("Upload Successful! Setting permissions...")
            
            # 2. 显式设置对象 ACL 为 public-read
            # 某些版本的 SDK 建议使用 aclControl 参数来传递 canned ACL 字符串
            acl_res = obs_client.setObjectAcl(bucket_name, object_key, aclControl='public-read')
            
            if acl_res.status < 300:
                print("Granting 'public-read' via aclControl succeeded.")
            else:
                print(f"Failed to set ACL via aclControl. Status: {acl_res.status}")
                # 备选方案：尝试直接设置 headers
                print("Trying alternative: setObjectAcl with extensionHeaders...")
                acl_res = obs_client.setObjectAcl(bucket_name, object_key, extensionHeaders={'x-obs-acl': 'public-read'})

            # 3. 最终核对此刻的 ACL 状态
            final_acl = obs_client.getObjectAcl(bucket_name, object_key)
            if final_acl.status < 300:
                print(f"Final Object Grants: {final_acl.body.grants}")
                print(f"Public URL: https://{bucket_name}.{endpoint}/{object_key}")
            else:
                print(f"Final verify failed. Status: {final_acl.status}")
        else:
            print(f"Upload Failed. Status: {res.status}")
            print(f"Error Code: {res.errorCode}")
            print(f"Error Message: {res.errorMessage}")
            
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        obs_client.close()

if __name__ == "__main__":
    upload_package()
