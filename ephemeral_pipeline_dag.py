from datetime import datetime
import secrets

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource.resources import ResourceManagementClient

RESOURCE_GROUP = "rg-pipeline"
LOCATION = "northcentralus"
VM_NAME = "pipeline-vm"
VM_SIZE = "Standard_B2as_v2"
REPO_URL = "https://github.com/ParkerButterfield/ephemeral-data-pipeline.git"
REPO_SUBDIR = "."          # folder inside the repo that has main.py


def get_azure():
    """Connect to Azure using the Airflow VM's identity (no passwords)."""
    sub = Variable.get("AZURE_SUBSCRIPTION_ID")
    return ComputeManagementClient(DefaultAzureCredential(), sub), sub


def create_resource_group():
    """Create the resource group if it does not exist. Does nothing if it does."""
    sub = Variable.get("AZURE_SUBSCRIPTION_ID")
    resources = ResourceManagementClient(DefaultAzureCredential(), sub)
    if resources.resource_groups.check_existence(RESOURCE_GROUP):
        print(f"Resource group {RESOURCE_GROUP} already exists")
    else:
        resources.resource_groups.create_or_update(RESOURCE_GROUP, {"location": LOCATION})
        print(f"Created resource group {RESOURCE_GROUP} in {LOCATION}")


def create_network():
    """Same as the three az network commands. Creates on the first run, finds on later runs."""
    sub = Variable.get("AZURE_SUBSCRIPTION_ID")
    network = NetworkManagementClient(DefaultAzureCredential(), sub)
    base = f"/subscriptions/{sub}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network"

    # az network vnet create -g rg-pipeline -n pipeline-vnet --subnet-name default
    network.virtual_networks.begin_create_or_update(RESOURCE_GROUP, "pipeline-vnet", {
        "location": LOCATION,
        "properties": {
            "addressSpace": {"addressPrefixes": ["10.0.0.0/16"]},
            "subnets": [{"name": "default", "properties": {"addressPrefix": "10.0.0.0/24"}}],
        },
    }).result()
    print("VNet ready")

    # az network public-ip create -g rg-pipeline -n pipeline-vm-pip --sku Standard
    network.public_ip_addresses.begin_create_or_update(RESOURCE_GROUP, "pipeline-vm-pip", {
        "location": LOCATION,
        "sku": {"name": "Standard"},
        "properties": {"publicIPAllocationMethod": "Static"},
    }).result()
    print("Public IP ready")

    # az network nic create -g rg-pipeline -n pipeline-vm-nic --vnet-name pipeline-vnet --subnet default --public-ip-address pipeline-vm-pip
    network.network_interfaces.begin_create_or_update(RESOURCE_GROUP, "pipeline-vm-nic", {
        "location": LOCATION,
        "properties": {
            "ipConfigurations": [{
                "name": "ipconfig1",
                "properties": {
                    "subnet": {"id": f"{base}/virtualNetworks/pipeline-vnet/subnets/default"},
                    "publicIPAddress": {"id": f"{base}/publicIPAddresses/pipeline-vm-pip"},
                },
            }],
        },
    }).result()
    print("NIC ready")


def create_vm():
    azure, sub = get_azure()
    nic = f"/subscriptions/{sub}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/networkInterfaces/pipeline-vm-nic"

    vm = {
        "location": LOCATION,
        "properties": {
            "hardwareProfile": {"vmSize": VM_SIZE},
            "storageProfile": {
                "imageReference": {"publisher": "Canonical", "offer": "ubuntu-24_04-lts",
                                   "sku": "server-arm64", "version": "latest"},
                "osDisk": {"createOption": "FromImage", "deleteOption": "Delete"},
            },
            "osProfile": {"computerName": VM_NAME, "adminUsername": "azureuser",
                          "adminPassword": secrets.token_urlsafe(24) + "Aa1!"},
            "networkProfile": {"networkInterfaces": [{"id": nic}]},
        },
    }
    print("Creating VM... (1 to 3 minutes)")
    azure.virtual_machines.begin_create_or_update(RESOURCE_GROUP, VM_NAME, vm).result()
    print("VM is running")


def run_pipeline():
    azure, _ = get_azure()
    supabase_url = Variable.get("supabase_url")
    supabase_key = Variable.get("supabase_service_key")

    script = [
        "set -e",                                                   # stop on first error
        "export DEBIAN_FRONTEND=noninteractive",
        "apt-get update -qq",
        "apt-get install -y -qq git python3 python3-pip > /dev/null",
        f"git clone --depth 1 {REPO_URL} /tmp/repo",
        f"cd /tmp/repo/{REPO_SUBDIR}",
        "if [ -f requirements.txt ]; then pip3 install -q --break-system-packages -r requirements.txt; fi",
        f"export SUPABASE_URL='{supabase_url}'",
        f"export SUPABASE_SERVICE_KEY='{supabase_key}'",
        "python3 main.py && echo PIPELINE_OK",                      # marker so Airflow knows it worked
    ]

    print("Running pipeline on the VM...")
    result = azure.virtual_machines.begin_run_command(RESOURCE_GROUP, VM_NAME, {
        "commandId": "RunShellScript",
        "script": script,
    }).result()

    output = result.value[0].message
    print(output)
    if "PIPELINE_OK" not in output:
        raise RuntimeError("Pipeline failed on the VM, see output above")


def delete_vm():
    azure, _ = get_azure()
    print("Deleting VM and its disk...")
    azure.virtual_machines.begin_delete(RESOURCE_GROUP, VM_NAME).result()
    print("VM deleted")


with DAG(
    dag_id="ephemeral_pipeline",
    schedule="*/10 * * * *",       # every 10 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
):
    rg = PythonOperator(task_id="create_resource_group", python_callable=create_resource_group)
    network = PythonOperator(task_id="create_network", python_callable=create_network)
    create = PythonOperator(task_id="create_vm", python_callable=create_vm)
    run = PythonOperator(task_id="run_pipeline", python_callable=run_pipeline)
    delete = PythonOperator(task_id="delete_vm", python_callable=delete_vm,
                            trigger_rule="all_done")  # always clean up

    rg >> network >> create >> run >> delete
