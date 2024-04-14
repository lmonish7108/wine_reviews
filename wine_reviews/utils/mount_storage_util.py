# Databricks notebook source
TENANTID = dbutils.secrets.get(scope='winereviews-secrets', key='TENANTID')
CLIENTID = dbutils.secrets.get(scope='winereviews-secrets', key='CLIENTID')
CLIENTSECRET = dbutils.secrets.get(scope='winereviews-secrets', key='CLIENTSECRET')
STORAGEACCOUNT = dbutils.secrets.get(scope='winereviews-secrets', key='STORAGEACCOUNT')


def mount_container(container):
    mount_configs = {
        'fs.azure.account.auth.type': 'OAuth',
        'fs.azure.account.oauth.provider.type': 'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
        'fs.azure.account.oauth2.client.id': CLIENTID,
        'fs.azure.account.oauth2.client.secret': CLIENTSECRET,
        'fs.azure.account.oauth2.client.endpoint': f'https://login.microsoftonline.com/{TENANTID}/oauth2/token'
    }

    if any(mount.mountPoint == f'/mnt/{STORAGEACCOUNT}/{container}' for mount in dbutils.fs.mounts()):
        return 'Already Mounted'

    dbutils.fs.mount(
        source=f'abfss://{container}@{STORAGEACCOUNT}.dfs.core.windows.net',
        mount_point=f'/mnt/{STORAGEACCOUNT}/{container}',
        extra_configs=mount_configs
    )
    return True
