from infomaniak_auth import InfomaniakAuth
from api_client import APIClient
from safe_logger import SafeLogger
import os

logger = SafeLogger("Infomaniak client")


class Item(object):
    def __init__(self, client, drive_id, path, descriptor):
        print("ALX:creating drive_id={}, path={}, descriptor={}".format(drive_id, path, descriptor))
        self.client = client
        self.path = path
        self.descriptor = descriptor or {}
        self.drive_id = drive_id

    def get_next_child(self):
        if "data" in self.descriptor:
            for item in self.descriptor.get("data", []):
                yield {
                    "name": item.get("name"),
                    "lastModified": int(item.get("last_modified_at")) * 1000,
                    "size": self.get_size(item=item),
                    "fullPath": "/".join([self.path, item.get("name")]),
                    "directory": item.get("type")=="dir"
                }
            return 
        if self.is_file():
            raise Exception("'{}' is a file, cannot list its content".format(self.path))
        print("ALX:self.client={}".format(self.client))
        for item in self.client.get_next_folder_item(self.drive_id, self.get_file_id()):
            is_directory = item.get("type")=="dir"
            print("ALX:is_directory={}".format(is_directory))
            yield {
                "name": item.get("name"),
                "lastModified": int(item.get("last_modified_at")) * 1000,
                "size": self.get_size(item=item),
                "fullPath": "/".join([self.path, item.get("name")]),
                "directory": item.get("type")=="dir"
            }
    def get_size(self, item=None):
        if item:
            size = item.get("size")
        else:
            size = self.descriptor.get("size")
        if size is None:
            return None
        else:
            return int(size)
    def get_description(self):
        return {
            "name": self.descriptor.get("name"),
            "lastModified": int(self.descriptor.get("last_modified_at")) * 1000,
            "size": self.get_size(),
            "fullPath": "/".join([self.path, self.descriptor.get("name")]),
            "directory": self.is_folder()
        }
    def is_file(self):
        return self.descriptor.get("type") == "file"
    def is_folder(self):
        return self.descriptor.get("type") == "dir"
    def exists(self):
        print("ALX:exists = {}".format(self.descriptor))
        return not not self.descriptor
    def get_file_id(self):
        return self.descriptor.get("id")
    def delete(self):
        self.client.delete_item(self.drive_id, self.get_file_id())
    

class KdriveClient():
    def __init__(self, api_token=None):
        server_url = "https://api.infomaniak.com/2/drive"
        self.client = APIClient(
            server_url=server_url,
            auth=InfomaniakAuth(api_token=api_token),
            pagination=KdrivePagination(),
            max_number_of_retries=1
        )

    def get(self, endpoint, url=None, raw=False):
        response = self.client.get(endpoint, url=url, raw=raw)
        return response

    def post(self, endpoint, url=None, params=None, json=None, data=None, headers=None, raw=False):
        response = self.client.post(endpoint, url=url, params=params, json=json, data=data, headers=headers, raw=raw)
        return response

    def delete(self, endpoint, url=None, params=None, json=None, data=None, headers=None, raw=False):
        response = self.client.delete(endpoint, url=url, params=params, json=json, data=data, headers=headers, raw=raw)
        return response

    def next_child(self, drive_id, file_id):
        url = "https://api.infomaniak.com/3/drive/{drive_id}/files/{file_id}/files".format(
            drive_id,
            file_id
        )
        response = self.get("", url=url)

    def get_drive_list(self):
        url = "?account_id=627431"
        response = self.get(url)
        print("ALX:get_drive_list:{}".format(response))

    def get_next_folder_item(self, drive_id, file_id):
        print("ALX:get_next_folder_item:drive_id={}, file_id={}".format(drive_id, file_id))
        url = "https://api.infomaniak.com/3/drive/{}/files/{}/files".format(
            drive_id,
            file_id
        )
        for row in self.client.get_next_row("", url=url, data_path=["data"]):
            yield row

    def get_item(self, drive_id, file_id, path, relative_path):
        path_tokens = path.split('/')
        new_file_id = file_id
        print("ALX:get_item:path_tokens={}".format(path_tokens))
        for path_token in path_tokens:
            print("ALX:10:path_token={}".format(path_token))
            item = self.find_item_in_file_id(drive_id, new_file_id, path_token)
            print("ALX:11")
            if not item:
                print("ALX:12:drive_id={}, relative_path={}".format(drive_id, relative_path))
                return Item(self, drive_id, relative_path, None)
            elif not path_token:
                return Item(self, drive_id, relative_path, {"id":file_id, "type": "dir"})
            else:
                print("ALX:13")
                new_file_id = item.get("id")
        print("ALX:14:drive_id={}, relative_path={}, item={}".format(drive_id, relative_path, item))
        item = Item(self, drive_id, relative_path, item)
        print("ALX:15")
        return item

    def find_item_in_file_id(self, drive_id, file_id, item_name):
        print("ALX:find_item_in_file_id:item_name={}".format(item_name))
        for folder_item in self.get_next_folder_item(drive_id, file_id):
            print("ALX:20:folder_item={}".format(folder_item))
            if not item_name:
                print("ALX:21")
                return folder_item
            if folder_item and folder_item.get("name") == item_name:
                print("ALX:22")
                return folder_item
        print("ALX:23")
        return None
    
    def get_file_content(self, drive_id, file_id):
        url = "https://api.infomaniak.com/2/drive/{}/files/{}/download".format(drive_id, file_id)
        response = self.get("", url=url, raw=True)
        return response

    def write_file_content(self, drive_id, parent_folder_id, full_path, data):
        file_path, file_name = os.path.split(full_path)
        url = "https://api.infomaniak.com/3/drive/{}/upload".format(
            drive_id
        )
        params = {
            "total_size": len(data),
            "file_name": file_name,
            "directory_id": parent_folder_id
            # "directory_path": "Alex Bourret{}".format(file_path)
        }
        print("ALX:write_file_content:url={}".format(url))
        response = self.post("", url=url, params=params, data=data, raw=True)
        print("ALX:response code={}".format(response.status_code))
        print("ALX:response content={}".format(response.content))
        return response

    def delete_item(self, drive_id, item_id):
        url = "https://api.infomaniak.com/2/drive/{}/files/{}".format(drive_id, item_id)
        response = self.delete("", url=url)
        return response
    
    def move_item(self, drive_id, item_id, destination_directory_id):
        url = "https://api.infomaniak.com/3/drive/{}/files/{}/move/{}".format(
            drive_id,
            item_id,
            destination_directory_id
        )
        print("ALX:move:url={}".format(url))
        response = self.post("", url=url)
        print("ALX:move:response={}".format(response))
        return response

    def rename(self, drive_id, item_to_rename_id, new_name):
        url = "https://api.infomaniak.com/2/drive/{}/files/{}/rename".format(
            drive_id,
            item_to_rename_id
        )
        data = {
            "name": new_name
        }
        print("ALX:rename:url={}, data={}".format(url,data))
        response = self.post("", url=url, json=data)
        print("ALX:rename:response={}".format(response))
        return response


class KdrivePagination():
    def __init__(self):
        # No pagination, just stops after the first page
        logger.info("Single page pagination used")
        pass

    def has_next_page(self, response, items_retrieved):
        logger.info("DefaultPagination:has_next_page")
        if response is None:
            logger.info("DefaultPagination:has_next_page initialisation")
            return True
        logger.info("DefaultPagination:has_next_page Stop here")
        return False

    def get_paging_parameters(self, params):
        logger.info("DefaultPagination:get_paging_parameters")
        return {}