# This file is the actual code for the custom Python FS provider infomaniak_kdrive

from dataiku.fsprovider import FSProvider
from infomaniak_client import KdriveClient
import os, shutil
from io import BytesIO

"""
This sample provides files from inside the providerRoot passed in the config
"""


class CustomFSProvider(FSProvider):
    def __init__(self, root, config, plugin_config):
        """
        :param root: the root path for this provider
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        if len(root) > 0 and root[0] == '/':
            root = root[1:]
        self.root = root
        self.provider_root = "/"
        print("ALX:root={}".format(root))
        root_url = config.get("root_url")

        print("ALX:config={}".format(config))
        auth = config.get("api_token", {})
        api_token = auth.get("api_token")
        self.client = KdriveClient(api_token=api_token)
        print("ALX={}".format(self.client.get_drive_list()))
        # https://ksuite.infomaniak.com/kdrive/app/drive/497955/files/5
        # https://api.infomaniak.com/3/drive/{drive_id}/files/{file_id}/files
        self.drive_id, self.root_file_id = extract_id_from_url(root_url)

        #/files
        # empty dir: {'result': 'success', 'data': [], 'response_at': 1741257188, 'cursor': None, 'has_more': False}
        # file: {'result': 'error', 'error': {'code': 'destination_not_a_directory', 'description': 'Destination not a valid directory'}}

        #just the id:
        # file: {'result': 'success', 'data': {'id': 12, 'name': 'test docs.docx', 'type': 'file', 'status': None, 'visibility': 'is_in_private_space', 'drive_id': 497955, 'depth': 2, 'created_by': 1169836, 'created_at': None, 'added_at': 1651421833, 'last_modified_at': 1651941461, 'last_modified_by': 1169836, 'revised_at': 1651941461, 'updated_at': 1707225623, 'parent_id': 5, 'size': 50274, 'mime_type': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'extension_type': 'text'}}
        # root dir: {'result': 'success', 'data': {'id': 5, 'name': 'Private', 'type': 'dir', 'status': None, 'visibility': 'is_private_space', 'drive_id': 497955, 'depth': 1, 'created_by': None, 'created_at': None, 'added_at': 1651401746, 'last_modified_at': 1741257137, 'last_modified_by': None, 'revised_at': 1741257137, 'updated_at': 1741257137, 'parent_id': 1, 'color': None}}

        #self.root_file_id = 12
        self.operations = 0


    # util methods
    def get_rel_path(self, path):
        if len(path) > 0 and path[0] == '/':
            path = path[1:]
        return path

    def get_lnt_path(self, path):
        if len(path) == 0 or path == '/':
            return '/'
        elts = path.split('/')
        elts = [e for e in elts if len(e) > 0]
        return '/' + '/'.join(elts)

    def get_full_path(self, path):
        path_elts = [self.provider_root, self.get_rel_path(self.root), self.get_rel_path(path)]
        path_elts = [e for e in path_elts if len(e) > 0]
        return os.path.join(*path_elts)

    def close(self):
        """
        Perform any necessary cleanup
        """
        print('close')

    def stat(self, path):
        """
        Get the info about the object at the given path inside the provider's root, or None 
        if the object doesn't exist
        """
        full_path = self.get_full_path(path)
        print("ALX:stat:full_path={}".format(full_path)) # ALX:stat:full_path=/first/second/outside.png
        # why ALX:stat:full_path=/first/interpolated2.png/interpolated2.png ?
        self.operations += 1
        print("ALX:operation={}".format(self.operations))
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path.strip("/"), self.get_lnt_path(path).strip("/"))
        if not item.exists():
            return None
        item_description = item.get_description()
        if item.is_folder():
            return {
                "path": self.get_lnt_path(path),
                "size": 0,
                "lastModified": item_description.get("lastModified"),
                "isDirectory": True
            }
        else:
            return {
                "path": self.get_lnt_path(path),
                "size": item_description.get("size"),
                "lastModified": item_description.get("lastModified"),
                "isDirectory": False
            }
        # {
        #     "name": self.descriptor.get("name"),
        #     "lastModified": int(self.descriptor.get("last_modified_at")) * 1000,
        #     "size": self.get_size(),
        #     "fullPath": "/".join([self.path, self.descriptor.get("name")]),
        #     "directory": self.is_folder()
        # }
        # if not os.path.exists(full_path):
        #     return None
        # if os.path.isdir(full_path):
        #     return {'path': self.get_lnt_path(path), 'size':0, 'lastModified':int(os.path.getmtime(full_path)) * 1000, 'isDirectory':True}
        # else:
        #     return {'path': self.get_lnt_path(path), 'size':os.path.getsize(full_path), 'lastModified':int(os.path.getmtime(full_path)) * 1000, 'isDirectory':False}

    def set_last_modified(self, path, last_modified):
        """
        Set the modification time on the object denoted by path. Return False if not possible
        """
        full_path = self.get_full_path(path)
        os.utime(full_path, (os.path.getatime(full_path), last_modified / 1000))
        return True

    def browse(self, path):
        """
        List the file or directory at the given path, and its children (if directory)
        """
        full_path = self.get_full_path(path)
        print("ALX:browse:full_path={}, self.drive_id={}, self.root_file_id={}".format(full_path, self.drive_id, self.root_file_id))
        self.operations += 1
        print("ALX:operation={}".format(self.operations))
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path.strip("/"), self.get_lnt_path(path).strip("/"))
        print("ALX:response={}".format(item))
        if not item.exists():
            ret = {'fullPath' : None, 'exists' : False}
            print("ALX:ret1={}".format(ret))
            return ret
        elif item.is_file():
            ret = item.get_description()
            print("ALX:ret2={}".format(ret))
            return ret
        else:
            children = []
            for child in item.get_next_child():
                print("ALX:child={}".format(child))
                children.append(
                    child
                )
            ret = {
                'fullPath' : self.get_lnt_path(path),
                'exists' : True,
                'directory' : True,
                'children' : children
            }
            print("ALX:ret3={}".format(ret))
            return ret

        # if not os.path.exists(full_path):
        #     return {'fullPath' : None, 'exists' : False}
        # elif os.path.isfile(full_path):
        #     return {'fullPath' : self.get_lnt_path(path), 'exists' : True, 'directory' : False, 'size' : os.path.getsize(full_path)}
        # else:
        #     children = []
        #     for sub in os.listdir(full_path):
        #         sub_full_path = os.path.join(full_path, sub)
        #         sub_path = self.get_lnt_path(os.path.join(path, sub))
        #         if os.path.isdir(sub_full_path):
        #             children.append({'fullPath' : sub_path, 'exists' : True, 'directory' : True, 'size' : 0})
        #         else:
        #             children.append({'fullPath' : sub_path, 'exists' : True, 'directory' : False, 'size' : os.path.getsize(sub_full_path)})
        #     return {'fullPath' : self.get_lnt_path(path), 'exists' : True, 'directory' : True, 'children' : children}

    def enumerate(self, path, first_non_empty):
        """
        Enumerate files recursively from prefix. If first_non_empty, stop at the first non-empty file.

        If the prefix doesn't denote a file or folder, return None
        """
        full_path = self.get_full_path(path)
        print("ALX:enumerate:full_path={}".format(full_path))
        self.operations += 1
        print("ALX:operation={}".format(self.operations))
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path.strip("/"), self.get_lnt_path(path).strip("/"))
        if not item.exists():
            return None
        if item.is_file():
            item_description = item.get_description()
            return [
                {
                    'path': self.get_lnt_path(path),
                    'size': item_description.get("size"),
                    'lastModified': item_description.get("lastModified")
                }
            ]
        paths = []
        for child in item.get_next_child():
            # {
            #     "name": item.get("name"),
            #     "lastModified": int(item.get("last_modified_at")) * 1000,
            #     "size": self.get_size(item=item),
            #     "fullPath": "/".join([self.path, item.get("name")]),
            #     "directory": item.get("type")=="dir"
            # }
            full_sub_path = os.path.join(path, child.get("name"))
            sub_path = full_sub_path[len(os.path.join(self.provider_root, self.root)):]
            paths.append(
                {
                    'path': self.get_lnt_path(sub_path),
                    'size': child.get("size"),
                    'lastModified': child.get("lastModified")
                }
            )
        return paths
        # if not os.path.exists(full_path):
        #     return None
        # if os.path.isfile(full_path):
        #     return [{'path':self.get_lnt_path(path), 'size':os.path.getsize(full_path), 'lastModified':int(os.path.getmtime(full_path)) * 1000}]
        # paths = []
        # for root, dirs, files in os.walk(full_path):
        #     for file in files:
        #         full_sub_path = os.path.join(root, file)
        #         sub_path = full_sub_path[len(os.path.join(self.provider_root, self.root)):]
        #         paths.append({'path':self.get_lnt_path(sub_path), 'size':os.path.getsize(full_sub_path), 'lastModified':int(os.path.getmtime(full_sub_path)) * 1000})
        # return paths

    def delete_recursive(self, path):
        """
        Delete recursively from path. Return the number of deleted files (optional)
        """
        full_path = self.get_full_path(path)
        print("ALX:delete_recursive:full_path={}".format(full_path)) # ALX:delete_recursive:full_path=/first/Screenshot 2025-03-06 at 16.07.27.png
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path.strip("/"), self.get_lnt_path(path).strip("/"))
        print("ALX:item={}/{}".format(item.get_description(), item.get_file_id()))
        if not item.exists():
            return 0
        else:
            item.delete()
            return 1
        # if not os.path.exists(full_path):
        #     return 0
        # elif os.path.isfile(full_path):
        #     os.remove(full_path)
        #     return 1
        # else:
        #     shutil.rmtree(full_path)
        #     return 0

    def move(self, from_path, to_path):
        """
        Move a file or folder to a new path inside the provider's root. Return false if the moved file didn't exist
        """
        full_from_path = self.get_full_path(from_path)
        full_to_path = self.get_full_path(to_path)
        if are_files_in_same_path(full_from_path, full_to_path):
            # simple rename
            _, file_name = os.path.split(full_to_path)
            item_to_rename = self.client.get_item(self.drive_id, self.root_file_id, full_from_path.strip("/"), self.get_lnt_path(full_from_path).strip("/"))
            if not item_to_rename.exists():
                return False
            self.client.rename(self.drive_id, item_to_rename.get_file_id(), file_name)
            return True
        else:
            # move into new dir
            item_from = self.client.get_item(self.drive_id, self.root_file_id, full_from_path.strip("/"), self.get_lnt_path(full_from_path).strip("/"))
            destination_path, _ = os.path.split(full_to_path)
            item_to = self.client.get_item(self.drive_id, self.root_file_id, destination_path.strip("/"), self.get_lnt_path(destination_path).strip("/"))
            self.client.move_item(self.drive_id, item_from.get_file_id(), item_to.get_file_id())
            return True
        # print("ALX:move:full_from_path={}".format(full_from_path))
        # if os.path.exists(full_from_path):
        #     if from_path != to_path:
        #         shutil.move(full_from_path, full_to_path)
        #     return True
        # else:
        #     return False

    def read(self, path, stream, limit):
        """
        Read the object denoted by path into the stream. Limit is an optional bound on the number of bytes to send
        """
        full_path = self.get_full_path(path)
        print("ALX:read:full_path={}".format(full_path)) # ALX:read:full_path=/first/second/outside.png
        self.operations += 1
        print("ALX:operation={}".format(self.operations))
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path.strip("/"), self.get_lnt_path(path).strip("/"))
        if not item.exists():
            raise Exception('Path doesn t exist')
        response = self.client.get_file_content(self.drive_id, item.get_file_id())
        bio = BytesIO(response.content)
        shutil.copyfileobj(bio, stream)

    def write(self, path, stream):
        """
        Write the stream to the object denoted by path into the stream
        """
        full_path = self.get_full_path(path)
        print("ALX:write:full_path={}".format(full_path))
        full_path_parent = os.path.dirname(full_path)
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path_parent.strip("/"), self.get_lnt_path(full_path_parent).strip("/"))
        parent_folder_id = item.get_file_id()
        print("ALX:write:parent_folder_id={}".format(parent_folder_id))
        #if not item.exists():
        #    print("ALX:doesn't exists, creating")
        #    parent_folder_id = self.client.make_dirs(self.drive_id, self.root_file_id, full_path_parent)
        #    print("ALX:write:parent_folder_id={}".format(parent_folder_id))
        bio = BytesIO()
        shutil.copyfileobj(stream, bio)
        bio.seek(0)
        data = bio.read()
        response = self.client.write_file_content(self.drive_id, parent_folder_id, full_path, data)
        print("ALX:write:response={}".format(response))


def extract_id_from_url(url):
    #https://ksuite.infomaniak.com/kdrive/app/drive/497955/files/5
    if not url:
        return None, None
    tokens = url.strip('/').split('/')
    drive_id = file_id = None
    if tokens[-2:-1][0] == "files" and tokens[-4:-3][0]=="drive":
        drive_id = tokens[-3:-2][0]
        file_id = tokens[-1:][0]
    return drive_id, file_id

def are_files_in_same_path(path_one, path_two):
    file_path_one, _ = os.path.split(path_one)
    file_path_two, _ = os.path.split(path_two)
    return file_path_one == file_path_two
