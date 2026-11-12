
from datetime import datetime
import secrets
import shlex

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator

from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.core.exceptions import ResourceNotFoundError


# Settings
RESOURCE_GROUP = "rg-pipeline"
LOCATION = "northcentralus"
VM_NAME = "pipeline-vm"
VM_SIZE = "Standard_B2as_v2"

REPO_URL = "https://github.com/ParkerButterfield/ephemeral-data-pipeline.git"
REPO_SUBDIR = "."


def get_azure():
    """Connect to Azure using the Airflow VM's managed identity."""
    subscription_id = Variable.get("AZURE_SUBSCRIPTION_ID")
    credential = DefaultAzureCredential()

    compute = ComputeManagementClient(
        credential, subscription_id
    )

    return compute, subscription_id


def create_resource_group():
    """Create the resource group if it does not already exist."""
    subscription_id = Variable.get("AZURE_SUBSCRIPTION_ID")

    resources = ResourceManagementClient(
        DefaultAzureCredential(), subscription_id
    )

    if resources.resource_groups.check_existence(RESOURCE_GROUP):
        print(f"Resource group {RESOURCE_GROUP} already exists")
    else:
        resources.resource_groups.create_or_update(
            RESOURCE_GROUP,
            {"location": LOCATION}
        )
        print(f"Created resource group {RESOURCE_GROUP} in {LOCATION}")


def create_network():
    """Create the network resources needed by the pipeline VM."""
    subscription_id = Variable.get("AZURE_SUBSCRIPTION_ID")

    network = NetworkManagementClient(
        DefaultAzureCredential(), subscription_id
    )

    base = (
        f"/subscriptions/{subscription_id}"
        f"/resourceGroups/{RESOURCE_GROUP}"
        f"/providers/Microsoft.Network"
    )

    # Create virtual network
    network.virtual_networks.begin_create_or_update(
        RESOURCE_GROUP,
        "pipeline-vnet",
        {
            "location": LOCATION,
            "properties": {
                "addressSpace": {
                    "addressPrefixes": ["10.0.0.0/16"]
                }
            }
        }
    ).result()

    # Create subnet
    network.subnets.begin_create_or_update(
        RESOURCE_GROUP,
        "pipeline-vnet",
        "default",
        {
            "address_prefix": "10.0.0.0/24"
        }
    ).result()

    print("VNet ready")

    # Create public IP
    network.public_ip_addresses.begin_create_or_update(
        RESOURCE_GROUP,
        "pipeline-vm-pip",
        {
            "location": LOCATION,
            "sku": {"name": "Standard"},
            "properties": {
                "publicIPAllocationMethod": "Static"
            }
        }
    ).result()

    print("Public IP ready")

    # Create network interface
    network.network_interfaces.begin_create_or_update(
        RESOURCE_GROUP,
        "pipeline-vm-nic",
        {
            "location": LOCATION,
            "properties": {
                "ipConfigurations": [
                    {
                        "name": "ipconfig1",
                        "properties": {
                            "subnet": {
                                "id": f"{base}/virtualNetworks/pipeline-vnet/subnets/default"
                            },
                            "publicIPAddress": {
                                "id": f"{base}/publicIPAddresses/pipeline-vm-pip"
                            }
                        }
                    }
                ]
            }
        }
    ).result()

    print("NIC ready")


def create_vm():
    """Create the temporary Ubuntu VM."""
    azure, subscription_id = get_azure()

    nic_id = (
        f"/subscriptions/{subscription_id}"
        f"/resourceGroups/{RESOURCE_GROUP}"
        f"/providers/Microsoft.Network"
        f"/networkInterfaces/pipeline-vm-nic"
    )

    vm = {
        "location": LOCATION,
        "properties": {
            "hardwareProfile": {
                "vmSize": VM_SIZE
            },
            "storageProfile": {
                "imageReference": {
                    "publisher": "Canonical",
                    "offer": "ubuntu-24_04-lts",
                    "sku": "server",
                    "version": "latest"
                },
                "osDisk": {
                    "createOption": "FromImage",
                    "deleteOption": "Delete"
                }
            },
            "osProfile": {
                "computerName": VM_NAME,
                "adminUsername": "azureuser",
                "adminPassword": secrets.token_urlsafe(24) + "Aa1!"
            },
            "networkProfile": {
                "networkInterfaces": [
                    {"id": nic_id}
                ]
            }
        }
    }

    print("Creating VM...")

    azure.virtual_machines.begin_create_or_update(
        RESOURCE_GROUP,
        VM_NAME,
        vm
    ).result()

    print("VM is running")


def run_pipeline():
    """Run the Python pipeline on the temporary VM."""
    azure, _ = get_azure()

    supabase_url = Variable.get("supabase_url")
    supabase_key = Variable.get("supabase_service_key")

    script = [
        "set -e",
        "export DEBIAN_FRONTEND=noninteractive",
        "apt-get update -qq",
        "apt-get install -y -qq git python3 python3-pip > /dev/null",

        f"git clone --depth 1 {shlex.quote(REPO_URL)} /tmp/repo",
        f"cd /tmp/repo/{REPO_SUBDIR}",

        "if [ -f requirements.txt ]; then pip3 install -q --break-system-packages -r requirements.txt; fi",

        f"export SUPABASE_URL={shlex.quote(supabase_url)}",
        f"export SUPABASE_SERVICE_KEY={shlex.quote(supabase_key)}",

        "python3 main.py && echo PIPELINE_OK"
    ]

    print("Running pipeline on the VM...")

    result = azure.virtual_machines.begin_run_command(
        RESOURCE_GROUP,
        VM_NAME,
        {
            "commandId": "RunShellScript",
            "script": script
        }
    ).result()

    output = "\n".join(
        item.message or "" for item in (result.value or [])
    )

    print(output)

    if "PIPELINE_OK" not in output:
        raise RuntimeError(
            "Pipeline failed on the VM. Check the output above."
        )

    print("Pipeline completed successfully")


def delete_vm():
    """Delete the temporary VM and its OS disk."""
    azure, _ = get_azure()

    try:
        azure.virtual_machines.get(
            RESOURCE_GROUP,
            VM_NAME
        )
    except ResourceNotFoundError:
        print("VM does not exist. Nothing to delete.")
        return

    print("Deleting VM and its disk...")

    azure.virtual_machines.begin_delete(
        RESOURCE_GROUP,
        VM_NAME
    ).result()

    print("VM deleted")


# Airflow DAG
with DAG(
    dag_id="ephemeral_pipeline",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["azure", "ephemeral", "pipeline"]
):

    rg = PythonOperator(
        task_id="create_resource_group",
        python_callable=create_resource_group
    )

    network = PythonOperator(
        task_id="create_network",
        python_callable=create_network
    )

    create = PythonOperator(
        task_id="create_vm",
        python_callable=create_vm
    )

    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline
    )

    delete = PythonOperator(
        task_id="delete_vm",
        python_callable=delete_vm,
        trigger_rule="all_done"
    )

    rg >> network >> create >> run >> delete
