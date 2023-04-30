# pip install azure-storage-blob

from azure.storage.blob import BlobServiceClient;

connection_string = 'DefaultEndpointsProtocol=https;AccountName=teststoragevipul;AccountKey=K6M8O8AbV688dr6Qzi8o7osZ07J4C73utCaFKF8NQz2aZAfUi/p8r2Jg2zXakErjhss90KmO/Zk6+ASt0Qmfaw==;EndpointSuffix=core.windows.net'
account_url = 'https://teststoragevipul.blob.core.windows.net'
account_name = 'teststoragevipul'
account_key = 'K6M8O8AbV688dr6Qzi8o7osZ07J4C73utCaFKF8NQz2aZAfUi/p8r2Jg2zXakErjhss90KmO/Zk6+ASt0Qmfaw=='
container_name = 'raw'

def connectWithConnectionString():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str = connection_string)
        # blob_service_client =  BlobServiceClient(account_url=account_url, credential={"account_name": account_name,  "account_key" : account_key})
        return blob_service_client
    except:
        print('Error')

def connectWithAccessKey():
    try:
        blob_service_client =  BlobServiceClient(account_url=account_url, credential={"account_name": account_name,  "account_key" : account_key})
        return blob_service_client
    except:
        print('Error')       
        

def listAllBlobs():
    blob_service_client = connectWithAccessKey()
    container_client  = blob_service_client.get_container_client(container=container_name)
    blobs_list = container_client.list_blobs()
    for blob in blobs_list:
        print("Blob name is :", blob.name)


def uploadToBlobStorage(file_path, file_name):
    # use either one of the below methods
    blob_service_client = connectWithAccessKey()
    # blob_service_client = connectWithConnectionString()
    blob_file_name = f"test/files/{file_name}"
    blob_client = blob_service_client.get_blob_client(container= container_name, blob=blob_file_name)

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data)
    print(f"Uploaded {blob_file_name}")


def deleteABlob(blob_file_path_with_name):
    #Method 1
    blob_service_client = connectWithAccessKey()
    blob_client = blob_service_client.get_blob_client(container= container_name, blob= blob_file_path_with_name)
    if(blob_client.exists()):
        blob_client.delete_blob()
        print(f"Deleted the blob : {blob_file_path_with_name}")
    else:
        print(f'Blob {blob_file_path_with_name} does not exist')

    #Method 2
    blob_service_client = connectWithAccessKey()
    container_client  = blob_service_client.get_container_client(container=container_name)
    container_client.delete_blob(blob_file_path_with_name)


def deleteAllBlobs():
    blob_service_client = connectWithConnectionString()
    container_client = blob_service_client.get_container_client(container=container_name)
    blob_list = container_client.list_blobs()
    container_client.delete_blobs(*blob_list)
    print(f"All blogs deleted")


    


# listAllBlobs()
# uploadToBlobStorage("/Users/vipulmalhotra/Documents/source/repo/azure/azure-storage/files/Random.txt", "RandomnessOfMyMind.txt")
# deleteABlob(f"test/files/RandomnessOfMyMind.txt")
# deleteAllBlobs()