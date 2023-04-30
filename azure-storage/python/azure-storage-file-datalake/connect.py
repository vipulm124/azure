#pip install azure-storage-file-datalake azure-identity

from azure.storage.filedatalake import DataLakeServiceClient

connection_string = 'DefaultEndpointsProtocol=https;AccountName=teststoragevipul;AccountKey=K6M8O8AbV688dr6Qzi8o7osZ07J4C73utCaFKF8NQz2aZAfUi/p8r2Jg2zXakErjhss90KmO/Zk6+ASt0Qmfaw==;EndpointSuffix=core.windows.net'

# NOTE - IN ORDER TO WORK WITH THIS LIBRARY, the url of storage account is a bit different. Please compare it with the file under azure-storage-blob
account_url = 'https://teststoragevipul.dfs.core.windows.net'
account_name = 'teststoragevipul'
account_key = 'K6M8O8AbV688dr6Qzi8o7osZ07J4C73utCaFKF8NQz2aZAfUi/p8r2Jg2zXakErjhss90KmO/Zk6+ASt0Qmfaw=='
container_name = 'raw'
directory_name = "test/file"


def initialize_storage_account():
    try:
        global blob_service_client
        blob_service_client  = DataLakeServiceClient(account_url=account_url, credential=account_key)

    except Exception as e:
        print(e)


def createAContainer():
    try:
        global container_client
        container_client = blob_service_client.create_file_system(file_system=container_name)

    except Exception as e:
        print(e)


def getAContainer():
    try:
        global container_client
        container_client = blob_service_client.get_file_system_client(file_system=container_name)
    except Exception as e:
        print(e)




def createDirectory():
    try:
        container_client.create_directory(directory_name)
    except Exception as e:
        print(e)


def getADirectory():
    try:
        directory_client = container_client.get_directory_client(directory_name)
        return directory_client
    
    except Exception as e:
        print(e)

def renameDirectory():
    try:
        directory_client = getADirectory()
        new_dir_name = "fail/file"
        directory_client.rename_directory(new_name=directory_client.file_system_name + '/' + new_dir_name)
    except Exception as e:
        print(e)

def deleteDirectory():
    try:
        directory_client = getADirectory()
        directory_client.delete_directory()
    except Exception as e:
        print(e)


def uploadFileToDirectory():
    try:
        blob_service_client  = DataLakeServiceClient(account_url=account_url, credential=account_key)
        container_client = blob_service_client.get_file_system_client(file_system=container_name)
        file_client = container_client.create_file("test/upload.txt")
        local_file = open("/Users/vipulmalhotra/Documents/source/repo/azure/azure-storage/files/Random.txt",'r')
        file_contents = local_file.read()
        file_client.append_data(data = file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
    except Exception as e:
        print(e)


def download_file_from_directory():
    try:
        blob_service_client  = DataLakeServiceClient(account_url=account_url, credential=account_key)
        container_client = blob_service_client.get_file_system_client(file_system=container_name)
        
        local_file = open("/Users/vipulmalhotra/Documents/source/repo/azure/azure-storage/files/Random2.txt",'wb')
        file_client = container_client.get_file_client("test/upload.txt")

        download = file_client.download_file()

        downloaded_bytes = download.readall()

        local_file.write(downloaded_bytes)

        local_file.close()

    except Exception as e:
     print(e)


def list_directory_contents():
    try:
        blob_service_client  = DataLakeServiceClient(account_url=account_url, credential=account_key)
        container_client = blob_service_client.get_file_system_client(file_system=container_name)

        paths = container_client.get_paths(path="test")

        for path in paths:
            print(path.name + '\n')

    except Exception as e:
     print(e)



# initialize_storage_account()
# createAContainer()
# getAContainer()
# createDirectory()
# renameDirectory()
# deleteDirectory()
# uploadFileToDirectory()
# download_file_from_directory()
list_directory_contents()